package cache

import (
	"container/list"
)

// type Record struct {
// 	Key       string
// 	Value     []byte
// 	Timestamp []byte
// 	Tombstone bool
// }

type Cache struct {
	capacity   int
	size       int
	hashMap    map[string]*list.Element
	linkedList list.List
}

func NewCache(cap int) *Cache {
	m := make(map[string]*list.Element)
	l := list.New()
	c := Cache{cap, 0, m, *l}
	return &c
}

// adding records we know exist in database
func (c Cache) AddRecord(rec Record) {
	element, ok := c.hashMap[rec.Key]
	if ok {
		c.linkedList.Remove(element)
		c.linkedList.PushFront(rec)
		c.hashMap[rec.Key] = c.linkedList.Front()
	} else {
		if c.size == c.capacity {
			lastElement := c.linkedList.Back().Value.(Record)
			delete(c.hashMap, lastElement.Key)
			c.linkedList.Remove(c.linkedList.Back())
			c.linkedList.PushFront(rec)
			c.hashMap[rec.Key] = c.linkedList.Front()
		} else {
			c.linkedList.PushFront(rec)
			c.hashMap[rec.Key] = c.linkedList.Front()
			c.size += 1
		}
	}
}

// doesn't change cache if there isn't a record with that key because that record maybe doesn't exist in the database
// returns ture if it is found in cache
func (c Cache) LookForRecord(rec Record) bool {
	_, ok := c.hashMap[rec.Key]
	if ok {
		c.AddRecord(rec)
		return true
	} else {
		return false
	}
}

func (c Cache) DeleteRecord(rec Record) {
	element, ok := c.hashMap[rec.Key]
	if ok {
		delete(c.hashMap, rec.Key)
		c.linkedList.Remove(element)
		c.size -= 1
		return
	} else {
		return
	}
}
