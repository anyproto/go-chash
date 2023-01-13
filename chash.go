package chash

import (
	"errors"
	"fmt"
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
	// GetNext returns next member on members ring
	GetNext(memberId string) (Member, error)
	// GetPrev returns previous member on members ring
	GetPrev(memberId string) (Member, error)
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

type cHash struct {
	config          Config
	members         map[string]Member
	membersSet      members
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
		c.membersSet = append(c.membersSet, member{
			hash:   c.config.Hasher.Sum64([]byte(m.Id())),
			Member: m,
		})
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
	in := func(id string) bool {
		for _, mId := range memberIds {
			if id == mId {
				return true
			}
		}
		return false
	}
	filteredSet := c.membersSet[:0]
	for _, m := range c.membersSet {
		if !in(m.Id()) {
			filteredSet = append(filteredSet, m)
		}
	}
	c.membersSet = filteredSet
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

func (c *cHash) GetNext(memberId string) (Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.members[memberId]; !ok {
		if !ok {
			return nil, ErrMemberNotExists
		}
	}
	h := c.config.Hasher.Sum64([]byte(memberId))
	idx := sort.Search(len(c.membersSet), func(i int) bool {
		return c.membersSet[i].hash > h
	})
	if idx == len(c.membersSet) {
		idx = 0
	}
	return c.membersSet[idx].Member, nil
}

func (c *cHash) GetPrev(memberId string) (Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.members[memberId]; !ok {
		if !ok {
			return nil, ErrMemberNotExists
		}
	}
	h := c.config.Hasher.Sum64([]byte(memberId))
	idx := sort.Search(len(c.membersSet), func(i int) bool {
		return c.membersSet[i].hash >= h
	})
	idx--
	if idx < 0 {
		idx = len(c.membersSet) - 1
	}
	return c.membersSet[idx].Member, nil
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
	for _, m := range c.membersSet {
		totalCapacity += m.Capacity()
	}
	rf := c.config.ReplicationFactor
	if len(c.membersSet) < rf {
		rf = len(c.membersSet)
	}
	for i, m := range c.membersSet {
		p := int((float64(c.config.PartitionCount)*float64(rf))/(totalCapacity/m.Capacity())) + 1
		c.membersSet[i].pieces = p
	}

	var buf = make([]int, rf)
	for i, h := range c.partitionHashes {
		if len(c.partitions[i]) != rf {
			c.partitions[i] = make([]Member, rf)
		}
		c.membersSet.fillClosest(h, c.partitions[i], buf)
	}
}

type member struct {
	hash   uint64
	pieces int
	Member
}

type members []member

func (m members) Len() int {
	return len(m)
}

func (m members) Less(i, j int) bool {
	return m[i].hash < m[j].hash
}

func (m members) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m members) fillClosest(h uint64, ms []Member, buf []int) {
	idx := sort.Search(len(m), func(i int) bool {
		return m[i].hash >= h
	})
	var found int
	var maxOverflow int
	var foundIdx = buf[:0]
	var isAlreadyFound = func(idx int) bool {
		for _, fidx := range foundIdx {
			if idx == fidx {
				return true
			}
		}
		return false
	}
	for found < len(ms) {
		if idx == m.Len() {
			idx = 0
		}
		if isAlreadyFound(idx) {
			maxOverflow++
			idx++
			continue
		}
		if m[idx].pieces > -maxOverflow {
			m[idx].pieces--
			ms[found] = m[idx].Member
			foundIdx = append(foundIdx, idx)
			found++
		}
		idx++
	}
}
