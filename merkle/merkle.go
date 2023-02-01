package merkle

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) []byte {
	h := sha1.Sum(data)
	return h[:]
}

func SerializeMerkleTree(root *Node, file *os.File) {
	if root == nil {
		return
	}

	fmt.Fprintln(file, root.String())
	SerializeMerkleTree(root.left, file)
	SerializeMerkleTree(root.right, file)
}

func BuildMerkleTree(data [][]byte, unixTime int64) {
	if len(data) == 0 {
		return
	}

	var nodes []*Node
	for _, d := range data {
		nodes = append(nodes, &Node{data: Hash(d)})
	}

	for len(nodes) > 1 {
		var newNodes []*Node
		for i := 0; i < len(nodes); i += 2 {
			var left, right *Node
			if i+1 < len(nodes) {
				left = nodes[i]
				right = nodes[i+1]
			} else {
				left = nodes[i]
				right = nodes[i]
			}

			data := append(left.data[:], right.data[:]...)
			newNodes = append(newNodes, &Node{data: Hash(data), left: left, right: right})
		}
		nodes = newNodes
	}

	file, _ := os.Create("resources\\MetaData_" + fmt.Sprint(unixTime) + ".txt") // dorada naziva da se poklapa sa sstable fajlom
	defer file.Close()
	root := &MerkleRoot{root: nodes[0]}
	SerializeMerkleTree(root.root, file)
	fmt.Println("Merkle tree serialized to file")

	// return &MerkleRoot{root: nodes[0]}
}

// func main() {
// 	data := [][]byte{[]byte("data1"), []byte("data2"), []byte("data3")}
// 	merkleRoot := BuildMerkleTree(data)

// 	file, _ := os.Create("merkle_tree.txt")
// 	defer file.Close()

// 	SerializeMerkleTree(merkleRoot.root, file)
// 	fmt.Println("Merkle tree serialized to file")
// }
