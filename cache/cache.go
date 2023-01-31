package cache

import (
	"container/list"
	"fmt"

	"github.com/vradovic/naisp-projekat/record"
)

type Cache struct {
	capacity   int
	size       int
	hashMap    map[string]*list.Element
	linkedList list.List
}

// Set capacity only
func NewCache(cap int) *Cache {
	m := make(map[string]*list.Element)
	l := list.New()
	size := 0
	c := Cache{cap, size, m, *l}
	return &c
}

// adding records we know exist in database
func (c *Cache) AddRecord(rec record.Record) {
	element, ok := c.hashMap[rec.Key]
	if ok {
		c.linkedList.Remove(element)
		c.linkedList.PushFront(rec)
		c.hashMap[rec.Key] = c.linkedList.Front()
	} else {
		if c.size == c.capacity {
			lastElement := c.linkedList.Back().Value.(record.Record)
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
func (c *Cache) LookForRecord(key string) []byte {
	element, ok := c.hashMap[key]
	if ok {
		record := element.Value.(record.Record)
		c.AddRecord(record)
		return record.Value
	} else {
		return nil
	}
}

// Delete records from cache
func (c *Cache) DeleteRecord(rec record.Record) {
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

// Printing cache
func (c *Cache) Print() {
	for e := c.linkedList.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
	fmt.Println()
}
