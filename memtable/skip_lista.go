package memtable

import (
	"fmt"
	"math/rand"
	"time"
)

type SkipListNode struct {
	key     int
	value   []byte
	forward []*SkipListNode
	status  int
}

type SkipList struct {
	maxHeight int
	level     int
	header    *SkipListNode
}

func newSkipListNode(key, level int, value []byte) *SkipListNode {
	n := SkipListNode{key: key, value: value}
	for i := 0; i <= level; i++ {
		n.forward = append(n.forward, nil)
	}
	n.status = 0
	return &n
}

func newSkipList(maxHeight int) *SkipList {
	s := SkipList{maxHeight: maxHeight}
	s.level = 0
	s.header = newSkipListNode(-1, maxHeight, nil)
	return &s
}

func (s *SkipList) insertElement(key int, value []byte) {
	update := make([]*SkipListNode, 0, s.maxHeight)
	for i := 0; i <= s.maxHeight; i++ {
		update = append(update, nil)
	}
	current := s.header
	for i := s.level; i > -1; i-- {
		for (current.forward[i] != nil) && (current.forward[i].key < key) {
			current = current.forward[i]
		}
		update[i] = current
	}
	current = current.forward[0]
	if (current == nil) || (current.key != key) {
		rlevel := s.randomLevel()
		if rlevel > s.level {
			for i := s.level + 1; i <= rlevel; i++ {
				update[i] = s.header
			}
			s.level = rlevel
		}
		n := newSkipListNode(key, rlevel, value)
		for i := 0; i <= rlevel; i++ {
			n.forward[i] = update[i].forward[i]
			update[i].forward[i] = n
		}
	}
}

func (s SkipList) randomLevel() int {
	level := 0
	x1 := rand.NewSource(time.Now().UnixNano())
	y1 := rand.New(x1)
	for ; y1.Int31n(2) == 1; level++ {
		fmt.Print("broj")
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}

func (s SkipList) searchElement(key int) {
	current := s.header
	for i := s.level; i > -1; i-- {
		for (current.forward[i] != nil) && (current.forward[i].key < key) {
			current = current.forward[i]
		}
	}
	current = current.forward[0]
	if current != nil && current.key == key && current.status == 0 {
		fmt.Print("Found key :")
		fmt.Print(key)
		fmt.Print("\n")
	}
}

func (s *SkipList) deleteElement(key int) {
	current := s.header
	for i := s.level; i > -1; i-- {
		for (current.forward[i] != nil) && (current.forward[i].key < key) {
			current = current.forward[i]
		}
	}
	current = current.forward[0]
	if current != nil && current.key == key {
		current.status = 1
	}
}

func (s SkipList) displayList() {
	fmt.Print("\n*****Skip List******\n")
	head := s.header
	for lvl := 0; lvl <= s.level; lvl++ {
		fmt.Print("level: ")
		fmt.Println(lvl)
		node := head.forward[lvl]
		for node != nil {
			if node.status == 0 {
				fmt.Print(node.key)
				fmt.Print(" ")
			}
			node = node.forward[lvl]
		}
		fmt.Print("\n")
	}
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
