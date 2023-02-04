package memtable

import (
	"fmt"
	"sort"
	"strings"

	"github.com/vradovic/naisp-projekat/record"
)

type BTreeNode struct {
	leaf   bool
	child  []*BTreeNode
	record []*record.Record
}

type BTree struct {
	root *BTreeNode
	t    int
	size int
}

func newBTreeNode(leaf bool) *BTreeNode {
	n := BTreeNode{leaf: leaf}
	return &n
}

func NewBTree(t int) *BTree {
	b := BTree{t: t, size: 0}
	b.root = newBTreeNode(true)
	return &b
}

func (b *BTree) Write(r record.Record) bool {
	if b.searchBTree(r.Key, nil) {
		return b.UpdateBTree(r, nil)
	} else {
		return b.InsertBTree(r)
	}
}

func (b *BTree) Delete(r record.Record) bool {
	if b.searchBTree(r.Key, nil) {
		r.Tombstone = true
		return b.UpdateBTree(r, nil)
	} else {
		r.Tombstone = true
		return b.InsertBTree(r)
	}
}

func (b *BTree) UpdateBTree(r record.Record, x *BTreeNode) bool {
	if x != nil {
		i := 0
		for i < len(x.record) && r.Key > x.record[i].Key {
			i += 1
		}
		if i < len(x.record) && r.Key == x.record[i].Key {
			x.record[i] = &r
			return true
		} else if x.leaf {
			return false
		} else {
			return b.UpdateBTree(r, x.child[i])
		}
	} else {
		return b.UpdateBTree(r, b.root)
	}
}

func (b *BTree) InsertBTree(r record.Record) bool {
	root := b.root
	if len(root.record) == ((2 * b.t) - 1) {
		temp := newBTreeNode(false)
		b.root = temp
		temp.child = insertChild(temp.child, 0, root)
		b.splitChild(temp, 0)
		b.insertNonFull(temp, r)
		return true
	} else {
		b.insertNonFull(root, r)
		return true
	}
}

func (b *BTree) insertNonFull(root *BTreeNode, r record.Record) {
	i := len(root.record) - 1
	if root.leaf {
		root.record = append(root.record, &record.Record{Key: "", Value: nil})
		for i >= 0 && r.Key < root.record[i].Key {
			root.record[i+1] = root.record[i]
			i--
		}
		root.record[i+1] = &r
		b.size++
	} else {
		for i >= 0 && r.Key < root.record[i].Key {
			i--
		}
		i++
		if len(root.child[i].record) == ((2 * b.t) - 1) {
			b.splitChild(root, i)
			if r.Key > root.record[i].Key {
				i++
			}
		}
		b.insertNonFull(root.child[i], r)
	}
}

func (b *BTree) splitChild(x *BTreeNode, i int) {
	t := b.t
	y := x.child[i]
	z := newBTreeNode(y.leaf)
	x.child = insertChild(x.child, i+1, z)
	x.record = insertRecord(x.record, i, y.record[t-1])
	z.record = y.record[t : (2*t)-1]
	y.record = y.record[0 : t-1]
	if !y.leaf {
		z.child = y.child[t : 2*t]
		y.child = y.child[0:t]
	}
}

func (b *BTree) List(prefix string) []record.Record {
	items := []record.Record{}
	items = b.GetItems()
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

func (b *BTree) RangeScan(start string, finish string) []record.Record {
	items := []record.Record{}
	items = b.GetItems()
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

func (b *BTree) printBTree(x *BTreeNode, l int) {
	fmt.Print("Level ", l, ":")
	for _, v := range x.record {
		if !v.Tombstone {
			fmt.Print(v.Key, " ")
		}
	}
	fmt.Println("")
	l++
	if len(x.child) > 0 {
		for _, v := range x.child {
			b.printBTree(v, l)
		}
	}
}

type RecordList struct {
	recordList []record.Record
}

func (b *BTree) GetItems() []record.Record {
	list := RecordList{}
	list.GetRecord(b.root)
	return list.recordList
}

func (b *RecordList) GetRecord(x *BTreeNode) {
	for _, v := range x.record {
		b.recordList = append(b.recordList, *v)
	}
	if len(x.child) > 0 {
		for _, v := range x.child {
			b.GetRecord(v)
		}
	}
}

func (b *BTree) GetSize() uint {
	return uint(b.size)
}

func (b *BTree) Read(key string) []byte {
	return b.ReadAll(key, b.root)
}

func (b *BTree) ReadAll(key string, x *BTreeNode) []byte {
	i := 0
	for i < len(x.record) && key > x.record[i].Key {
		i += 1
	}
	if i < len(x.record) && key == x.record[i].Key {
		return (x.record[i].Value)
	} else if x.leaf {
		return nil
	} else {
		return b.ReadAll(key, x.child[i])
	}
}

func (b *BTree) searchBTree(key string, x *BTreeNode) bool {
	if x != nil {
		i := 0
		for i < len(x.record) && key > x.record[i].Key {
			i += 1
		}
		if i < len(x.record) && key == x.record[i].Key {
			return true
		} else if x.leaf {
			return false
		} else {
			return b.searchBTree(key, x.child[i])
		}
	} else {
		return b.searchBTree(key, b.root)
	}
}

func insertChild(a []*BTreeNode, index int, value *BTreeNode) []*BTreeNode {
	if len(a) == index {
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...)
	a[index] = value
	return a
}

func insertRecord(a []*record.Record, index int, value *record.Record) []*record.Record {
	if len(a) == index {
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...)
	a[index] = value
	return a
}

// func main() {

// 	var tree = newBTree(3)
// 	tree.t = 3
// 	fmt.Println(tree.Write(record.Record{Key: "a", Value: []byte("")}))
// 	tree.Write(record.Record{Key: "b", Value: []byte("")})
// 	tree.Write(record.Record{Key: "c", Value: []byte("")})
// 	tree.Write(record.Record{Key: "d", Value: []byte("")})
// 	tree.Write(record.Record{Key: "e", Value: []byte("")})
// 	tree.Write(record.Record{Key: "f", Value: []byte("")})
// 	tree.Write(record.Record{Key: "g", Value: []byte("")})
// 	tree.Write(record.Record{Key: "h", Value: []byte("")})
// 	tree.Write(record.Record{Key: "i", Value: []byte("")})
// 	tree.Write(record.Record{Key: "j", Value: []byte("")})
// 	tree.Write(record.Record{Key: "k", Value: []byte("")})
// 	tree.Write(record.Record{Key: "l", Value: []byte("")})
// 	tree.Write(record.Record{Key: "m", Value: []byte("A")})
// 	tree.Write(record.Record{Key: "n", Value: []byte("")})
// 	tree.Write(record.Record{Key: "o", Value: []byte("")})
// 	tree.Write(record.Record{Key: "p", Value: []byte("")})
// 	tree.Write(record.Record{Key: "r", Value: []byte("")})
// 	tree.Write(record.Record{Key: "s", Value: []byte("")})
// 	tree.Write(record.Record{Key: "t", Value: []byte("")})
// 	tree.Write(record.Record{Key: "u", Value: []byte("")})
// 	tree.Write(record.Record{Key: "v", Value: []byte("")})
// 	tree.printBTree(tree.root, 0)
// 	fmt.Println(tree.GetSize())
// 	fmt.Println(tree.Read("m"))
// 	fmt.Println(tree.Read("asc"))
// 	fmt.Println(tree.searchBTree("asc", nil))
// 	fmt.Println(tree.searchBTree("f", nil))
// 	tree.Write(record.Record{Key: "n", Value: []byte("B")})
// 	fmt.Println(tree.Delete(record.Record{Key: "a", Value: []byte("B")}))
// 	fmt.Println(tree.GetSize())
// 	tree.Delete(record.Record{Key: "w", Value: []byte("")})
// 	tree.printBTree(tree.root, 0)
// 	fmt.Println(tree.GetSize())
// 	fmt.Println(tree.GetItems())
// 	fmt.Println("Hello world")
// }
