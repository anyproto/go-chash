package chash

import (
	"errors"
	"fmt"
	"golang.org/x/exp/slices"
	"sort"
	"sync"

	"github.com/cespare/xxhash"
)

var (
	ErrMemberExists       = errors.New("member exists")
	ErrMemberNotExists    = errors.New("member not exists")
	ErrPartitionNotExists = errors.New("partition not exists")
	ErrInvalidCapacity    = errors.New("member capacity must be > 0")
)

type defaultHasher struct{}

func (h defaultHasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func New(c Config) (CHash, error) {
	if c.Hasher == nil {
		c.Hasher = defaultHasher{}
	}
	if c.ReplicationFactor == 0 {
		c.ReplicationFactor = 1
	}
	h := &cHash{config: c}
	if err := h.init(); err != nil {
		return nil, err
	}
	return h, nil
}

type CHash interface {
	// AddMembers adds one or more members to the cluster
	// May return ErrInvalidCapacity if member capacity less or equal 0
	// May return ErrMemberExists if member was added before
	AddMembers(members ...Member) error
	// RemoveMembers removes members with given ids
	RemoveMembers(memberIds ...string) error
	// Reconfigure replaces all members list
	Reconfigure(members []Member) error
	// GetMembers returns list of members for given key
	// Members count will be equal replication factor or total members count (if it is less than the replication factor)
	GetMembers(key string) []Member
	// GetPartition returns partition number for given key
	GetPartition(key string) int
	// GetPartitionMembers return members by partition number
	GetPartitionMembers(partId int) ([]Member, error)
	// Distribute members by partitions
	// Must be called if you changed members' capacity
	Distribute()
	// PartitionCount returns configured partitions count
	PartitionCount() int
}

type Member interface {
	Id() string
	Capacity() float64
}

type Hasher interface {
	Sum64([]byte) uint64
}

type Config struct {
	// Hasher implementation (optional), by default, will be used xxhash
	Hasher Hasher
	// PartitionCount - how many virtual partitions will be distributed by nodes, reasonable values from 100 to 10k
	PartitionCount uint64
	// ReplicationFactor - how many nodes expected for GetMembers
	ReplicationFactor int
}

func (c Config) Validate() (err error) {
	if c.ReplicationFactor < 1 {
		return fmt.Errorf("replcation factor must be great or equal 1")
	}
	if c.PartitionCount < 10 {
		return fmt.Errorf("patiotin count must be great ir qual 10")
	}
	return
}

const virtualMembers = 2000

type cHash struct {
	config          Config
	members         map[string]Member
	membersSet      members
	piecesPerMember map[string]int
	partitions      [][]Member
	partitionHashes []uint64
	mu              sync.RWMutex
}

func (c *cHash) init() (err error) {
	if err = c.config.Validate(); err != nil {
		return
	}
	c.members = make(map[string]Member)
	c.partitionHashes = make([]uint64, c.config.PartitionCount)
	c.partitions = make([][]Member, c.config.PartitionCount)
	for i := range c.partitionHashes {
		c.partitionHashes[i] = c.config.Hasher.Sum64([]byte(fmt.Sprint("p", i)))
	}
	return
}

func (c *cHash) AddMembers(members ...Member) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range members {
		if m.Capacity() <= 0 {
			return ErrInvalidCapacity
		}
		if _, ok := c.members[m.Id()]; ok {
			return ErrMemberExists
		}
	}
	return c.addMembers(members...)
}

func (c *cHash) addMembers(members ...Member) error {
	for _, m := range members {
		// generating enough virtual members for better hash distribution
		for i := 0; i < int(virtualMembers*m.Capacity()); i++ {
			c.membersSet = append(c.membersSet, member{
				hash:   c.config.Hasher.Sum64([]byte(fmt.Sprint(m.Id(), i))),
				Member: m,
			})
		}
		c.members[m.Id()] = m
	}
	sort.Sort(c.membersSet)
	c.distribute()
	return nil
}

func (c *cHash) RemoveMembers(memberIds ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, mId := range memberIds {
		if _, ok := c.members[mId]; !ok {
			return ErrMemberNotExists
		}
	}
	discard := func(ids ...string) members {
		idx := 0
		for _, el := range c.membersSet {
			if !slices.Contains(ids, el.Id()) {
				c.membersSet[idx] = el
				idx++
			}
		}
		return c.membersSet[:idx]
	}
	c.membersSet = discard(memberIds...)
	for _, mId := range memberIds {
		delete(c.members, mId)
	}
	c.distribute()
	return nil
}

func (c *cHash) Reconfigure(members []Member) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range members {
		if m.Capacity() <= 0 {
			return ErrInvalidCapacity
		}
	}
	c.members = make(map[string]Member)
	c.membersSet = c.membersSet[:0]
	return c.addMembers(members...)
}

func (c *cHash) GetMembers(key string) []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.partitions[c.getPartition(key)]
}

func (c *cHash) GetPartition(key string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getPartition(key)
}

func (c *cHash) PartitionCount() int {
	return int(c.config.PartitionCount)
}

func (c *cHash) getPartition(key string) int {
	h := c.config.Hasher.Sum64([]byte(key))
	return int(h % c.config.PartitionCount)
}

func (c *cHash) GetPartitionMembers(partId int) ([]Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if partId < 0 || partId >= int(c.config.PartitionCount) {
		return nil, ErrPartitionNotExists
	}
	return c.partitions[partId], nil
}

func (c *cHash) Distribute() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.distribute()
}

func (c *cHash) distribute() {
	if len(c.membersSet) == 0 {
		for i := range c.partitions {
			c.partitions[i] = nil
		}
		return
	}
	var totalCapacity float64
	rf := c.config.ReplicationFactor
	if len(c.members) < rf {
		rf = len(c.members)
	}
	for _, m := range c.members {
		totalCapacity += m.Capacity()
	}
	c.piecesPerMember = map[string]int{}
	for _, m := range c.members {
		p := int((float64(c.config.PartitionCount)*float64(rf))/(totalCapacity/m.Capacity())) + 1
		c.piecesPerMember[m.Id()] = p
	}

	var buf = make([]string, rf)
	for i, h := range c.partitionHashes {
		if len(c.partitions[i]) != rf {
			c.partitions[i] = make([]Member, rf)
		}
		c.fillClosest(c.membersSet, h, c.partitions[i], buf)
	}
}

func (c *cHash) fillClosest(m members, h uint64, ms []Member, buf []string) {
	idx := sort.Search(len(m), func(i int) bool {
		return m[i].hash >= h
	})
	var found int
	var maxOverflow int
	var foundId = buf[:0]

	var isAlreadyFound = func(id string) bool {
		return slices.Contains(foundId, id)
	}
	for found < len(ms) {
		if idx == m.Len() {
			idx = 0
		}
		if isAlreadyFound(m[idx].Id()) {
			maxOverflow++
			idx++
			continue
		}
		if c.piecesPerMember[m[idx].Id()] > -maxOverflow {
			c.piecesPerMember[m[idx].Id()]--
			ms[found] = m[idx].Member
			foundId = append(foundId, m[idx].Id())
			found++
		}
		idx++
	}
}

type member struct {
	hash uint64
	Member
}

type members []member

func (m members) Len() int {
	return len(m)
}

func (m members) Less(i, j int) bool {
	if m[i].hash == m[j].hash {
		return m[i].Id() < m[j].Id()
	} else {
		return m[i].hash < m[j].hash
	}
}

func (m members) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}
