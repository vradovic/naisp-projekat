package memtable

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/vradovic/naisp-projekat/record"
)

type SkipListNode struct {
	record  record.Record
	forward []*SkipListNode
}

type SkipList struct {
	maxHeight int
	level     int
	header    *SkipListNode
	Size      uint
}

func newSkipListNode(r record.Record, level int) *SkipListNode {
	n := SkipListNode{record: r}
	for i := 0; i <= level; i++ {
		n.forward = append(n.forward, nil)
	}
	return &n
}

func NewSkipList(maxHeight int) *SkipList {
	s := SkipList{maxHeight: maxHeight}
	s.level = 0
	r := record.Record{Key: "*"}
	s.header = newSkipListNode(r, maxHeight)
	s.Size = 0
	return &s
}

func (s *SkipList) Write(r record.Record) bool {
	update := make([]*SkipListNode, 0, s.maxHeight)
	for i := 0; i <= s.maxHeight; i++ {
		update = append(update, nil)
	}
	current := s.header
	for i := s.level; i > -1; i-- {
		for (current.forward[i] != nil) && (current.forward[i].record.Key < r.Key) {
			current = current.forward[i]
		}
		update[i] = current
	}
	current = current.forward[0]
	if current != nil && current.record.Key == r.Key {
		current.record.Value = r.Value
		current.record.Timestamp = r.Timestamp
		current.record.Tombstone = r.Tombstone

		return true
	} else if (current == nil) || (current.record.Key != r.Key) {
		rlevel := s.randomLevel()
		if rlevel > s.level {
			for i := s.level + 1; i <= rlevel; i++ {
				update[i] = s.header
			}
			s.level = rlevel
		}
		n := newSkipListNode(r, rlevel)
		for i := 0; i <= rlevel; i++ {
			n.forward[i] = update[i].forward[i]
			update[i].forward[i] = n
		}

		s.Size++
		return true
	}

	return false
}

func (s SkipList) randomLevel() int {
	level := 0
	x1 := rand.NewSource(time.Now().UnixNano())
	y1 := rand.New(x1)
	for ; y1.Int31n(2) == 1; level++ {
		// fmt.Print("broj")
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

func (s SkipList) Read(key string) (record.Record, bool) {
	current := s.header
	for i := s.level; i > -1; i-- {
		for (current.forward[i] != nil) && (current.forward[i].record.Key < key) {
			current = current.forward[i]
		}
	}
	current = current.forward[0]
	if current != nil && current.record.Key == key {
		// fmt.Print("Found key :")
		// fmt.Print(key)
		// fmt.Print("\n")
		return current.record, true
	}

	return record.Record{}, false
}

func (s *SkipList) Delete(r record.Record) bool {
	current := s.header
	for i := s.level; i > -1; i-- {
		for (current.forward[i] != nil) && (current.forward[i].record.Key < r.Key) {
			current = current.forward[i]
		}
	}
	current = current.forward[0]
	if current != nil && current.record.Key == r.Key {
		current.record.Tombstone = true
		return true
	} else {
		s.Write(r)
		return true
	}

	//return false
}

func (s *SkipList) List(prefix string) []record.Record {
	items := []record.Record{}
	items = s.GetItems()
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})
	list := []record.Record{}
	breaker := false
	for _, v := range items {
		if strings.HasPrefix(v.Key, prefix) {
			list = append(list, v)
			breaker = true
		} else if breaker {
			break
		}
	}
	return list
}

func (s *SkipList) RangeScan(start string, finish string) []record.Record {
	items := []record.Record{}
	items = s.GetItems()
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})
	list := []record.Record{}
	for _, v := range items {
		if v.Key <= finish {
			if v.Key >= start {
				list = append(list, v)
			}
		} else {
			break
		}
	}
	return list
}

func (s SkipList) DisplayList() {
	fmt.Print("\n*****Skip List******\n")
	head := s.header
	for lvl := 0; lvl <= s.level; lvl++ {
		fmt.Print("level: ")
		fmt.Println(lvl)
		node := head.forward[lvl]
		for node != nil {
			if !node.record.Tombstone {
				fmt.Print(node.record.Key)
				fmt.Print(" ")
			}
			node = node.forward[lvl]
		}
		fmt.Print("\n")
	}
}

func (s *SkipList) GetItems() []record.Record {
	head := s.header
	lvl := 0
	node := head.forward[lvl]
	records := make([]record.Record, 0)

	for node != nil {
		records = append(records, node.record)
		node = node.forward[lvl]
	}

	return records
}

func (s *SkipList) GetSize() uint {
	return s.Size
}

//func main() {
//	primes := [6]byte{100, 'B', 5, 7, 'A', 13}
//var value []byte = primes[0:3]
//var value2 []byte = primes[3:5]
//var lst = newSkipList(4)
//lst.insertElement(3, value)
//lst.insertElement(6, value)
//lst.insertElement(9, value2)
//	lst.insertElement(12, value2)
//lst.insertElement(10, value)
//	lst.displayList()
//	lst.searchElement(9)
//	lst.deleteElement(9)
//lst.displayList()
//	lst.searchElement(9)
//	lst.searchElement(10)
//	lst.searchElement(22)
//	lst.searchElement(3)
//}
