package cms

type Cms struct {
	M             uint
	K             uint
	Table         [][]byte
	HashFunctions []HashWithSeed
}

func NewCms(epsilon, delta float64) *Cms {
	m := CalculateM(epsilon)
	k := CalculateK(delta)

	// Matrica
	table := make([][]byte, k)
	var i uint
	for i = 0; i < k; i++ {
		table[i] = make([]byte, m)
	}

	hashFunctions := CreateHashFunctions(k)

	cms := &Cms{m, k, table, hashFunctions}
	return cms
}
