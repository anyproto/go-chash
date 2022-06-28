package chash

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

type testMember struct {
	id    string
	cap   float64
	links []string
}

func (t testMember) Id() string {
	return t.id
}

func (t testMember) Capacity() float64 {
	return t.cap
}

func (t testMember) String() string {
	return t.id
}

func TestNew(t *testing.T) {
	t.Run("invalid part count", func(t *testing.T) {
		_, err := New(Config{PartitionCount: 0})
		assert.Error(t, err)
	})
	t.Run("invalid replication factor", func(t *testing.T) {
		_, err := New(Config{PartitionCount: 10, ReplicationFactor: -1})
		assert.Error(t, err)
	})
}

func TestCHash_AddMembers(t *testing.T) {
	t.Run("common add", func(t *testing.T) {
		pc := 100
		h, err := New(Config{
			PartitionCount:    uint64(pc),
			ReplicationFactor: 1,
		})
		require.NoError(t, err)
		assert.NoError(t, h.AddMembers(testMember{id: "1", cap: 2}, testMember{id: "2", cap: 1}, testMember{id: "3", cap: 1}))
		assert.NoError(t, h.AddMembers(testMember{id: "4", cap: 1}))
	})
	t.Run("invalid capacity", func(t *testing.T) {
		h, err := New(Config{
			PartitionCount: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, ErrInvalidCapacity, h.AddMembers(testMember{id: "1", cap: 0}))
	})
	t.Run("member exists", func(t *testing.T) {
		h, err := New(Config{
			PartitionCount: 100,
		})
		require.NoError(t, err)
		assert.NoError(t, h.AddMembers(testMember{id: "1", cap: 1}))
		assert.Equal(t, ErrMemberExists, h.AddMembers(testMember{id: "1", cap: 1}))
	})
}

func TestCHash_RemoveMembers(t *testing.T) {
	t.Run("remove", func(t *testing.T) {
		pc := 10
		h, err := New(Config{
			PartitionCount: uint64(pc),
		})
		require.NoError(t, err)
		require.NoError(t, h.AddMembers(testMember{id: "1", cap: 1}, testMember{id: "2", cap: 1}, testMember{id: "3", cap: 1}))
		assert.NoError(t, h.RemoveMembers("1", "2"))
		for i := 0; i < 0; i++ {
			m, err := h.GetPartitionMembers(i)
			require.NoError(t, err)
			assert.Equal(t, "3", m[0].Id())
		}
		assert.NoError(t, h.RemoveMembers("3"))
	})
	t.Run("err not exists", func(t *testing.T) {
		h, err := New(Config{
			PartitionCount: 10,
		})
		require.NoError(t, err)
		require.NoError(t, h.AddMembers(testMember{id: "1", cap: 1}))
		require.NoError(t, h.RemoveMembers("1"))
		assert.Equal(t, ErrMemberNotExists, h.RemoveMembers("1"))
	})
}

func TestCapacity(t *testing.T) {
	h, err := New(Config{
		PartitionCount:    1000,
		ReplicationFactor: 3,
	})
	require.NoError(t, err)
	h.AddMembers(
		&testMember{
			id:  "cap1",
			cap: 1,
		},
		&testMember{
			id:  "cap2",
			cap: 2,
		},
		&testMember{
			id:  "cap3",
			cap: 3,
		},
		&testMember{
			id:  "cap4",
			cap: 4,
		},
		&testMember{
			id:  "cap5",
			cap: 5,
		},
	)

	var ditribStat = make(map[int]int)
	var total int

	for i := 0; i < 100000; i++ {
		memb := h.GetMembers(strconv.Itoa(i))
		for _, m := range memb {
			ditribStat[int(m.Capacity())]++
			total++
		}
	}
	var prevCount int
	for i := 1; i <= 5; i++ {
		count := ditribStat[i]
		assert.Greater(t, count, prevCount)
		t.Logf("capacity: %d; objects: %d (%.2f%%)", i, count, (100/float64(total))*float64(count))
	}
}

func TestConnectionPerNode(t *testing.T) {
	h, err := New(Config{
		PartitionCount:    3000,
		ReplicationFactor: 2,
	})
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		h.AddMembers(&testMember{
			id:  fmt.Sprint("n", i),
			cap: 1,
		})
	}

	var stats = make(map[string]map[string]bool)
	for i := 0; i < 3000; i++ {
		nodes, _ := h.GetPartitionMembers(i)
		for _, n1 := range nodes {
			for _, n2 := range nodes {
				if n1.Id() != n2.Id() {
					m1 := stats[n1.Id()]
					if m1 == nil {
						m1 = make(map[string]bool)
					}
					m1[n2.Id()] = true
					stats[n1.Id()] = m1
				}
			}
		}
	}
	var totalConn int
	for _, m := range stats {
		totalConn += len(m)
	}
	t.Log("average connection per node:", totalConn/len(stats))
	/*
		j, _ := json.Marshal(stats)
		fmt.Println(string(j))
	*/
}

func TestCHash_GetNext(t *testing.T) {
	h, err := New(Config{
		PartitionCount: 10,
	})
	require.NoError(t, err)

	// empty list
	_, err = h.GetNext("a")
	assert.Error(t, err)

	// one member
	h.AddMembers(&testMember{
		id:  "a",
		cap: 1,
	})
	m, err := h.GetNext("a")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "a")

	// two members
	h.AddMembers(&testMember{
		id:  "b",
		cap: 1,
	})
	m, err = h.GetNext("a")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "b")
	m, err = h.GetNext("b")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "a")

	// three members
	h.AddMembers(&testMember{
		id:  "c",
		cap: 1,
	})
	m, err = h.GetNext("a")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "b")
	m, err = h.GetNext("b")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "c")
	m, err = h.GetNext("c")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "a")
}

func TestCHash_GetPrev(t *testing.T) {
	h, err := New(Config{
		PartitionCount: 10,
	})
	require.NoError(t, err)

	// empty list
	_, err = h.GetPrev("a")
	assert.Error(t, err)

	// one member
	h.AddMembers(&testMember{
		id:  "a",
		cap: 1,
	})
	m, err := h.GetPrev("a")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "a")

	// two members
	h.AddMembers(&testMember{
		id:  "b",
		cap: 1,
	})
	m, err = h.GetPrev("a")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "b")
	m, err = h.GetPrev("b")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "a")

	// three members
	h.AddMembers(&testMember{
		id:  "c",
		cap: 1,
	})
	m, err = h.GetPrev("a")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "c")
	m, err = h.GetPrev("b")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "a")
	m, err = h.GetPrev("c")
	require.NoError(t, err)
	assert.Equal(t, m.Id(), "b")
}

func TestCHash_Distribute(t *testing.T) {
	t.Run("uniq nodes for partition", func(t *testing.T) {
		pc := 10
		rf := 3
		h, err := New(Config{
			PartitionCount:    uint64(pc),
			ReplicationFactor: rf,
		})
		require.NoError(t, err)
		require.NoError(t, h.AddMembers(
			&testMember{
				id:  "1",
				cap: 1,
			},
			&testMember{
				id:  "3",
				cap: 0.5,
			},
			&testMember{
				id:  "4",
				cap: 1,
			}))
		for i := 0; i < pc; i++ {
			ms, e := h.GetPartitionMembers(i)
			require.NoError(t, e)
			var ids = map[string]bool{}
			for _, m := range ms {
				ids[m.Id()] = true
			}
			assert.Len(t, ids, rf, ms)
		}
	})
}

func BenchmarkCHash_GetMembers(b *testing.B) {
	h, err := New(Config{
		PartitionCount:    3000,
		ReplicationFactor: 3,
	})
	require.NoError(b, err)
	for i := 0; i < 30; i++ {
		h.AddMembers(&testMember{
			id:  fmt.Sprint("n", i),
			cap: 1,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.GetMembers(strconv.Itoa(i))
	}
}

func BenchmarkCHash_Distribute(b *testing.B) {
	h, err := New(Config{
		PartitionCount:    3000,
		ReplicationFactor: 3,
	})
	require.NoError(b, err)
	for i := 0; i < 100; i++ {
		h.AddMembers(&testMember{
			id:  fmt.Sprint("n", i),
			cap: 1,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Distribute()
	}
}
