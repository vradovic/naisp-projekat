package sstable

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/vradovic/naisp-projekat/bloomfilter"
	"github.com/vradovic/naisp-projekat/merkle"
	"github.com/vradovic/naisp-projekat/record"
)

const (
	KEY_SIZE_LEN        = 8 // duzina polja za duzinu kljuca u bajtovima
	VALUE_SIZE_LEN      = 8 // duzina polja za duzinu value u bajtovima
	TOMBSTONE_LEN       = 1
	TIMESTAMP_LEN       = 8
	KEY_VALUE_START     = KEY_SIZE_LEN + VALUE_SIZE_LEN + TOMBSTONE_LEN + TIMESTAMP_LEN // mesto odakle pocinje vrendost kljuca
	HEADER_SIZE         = 32                                                            // u prva 32 bajta cuvam velicinu data segmenta, index segmenta, pocetak BloomFiltera i velicinu data zone u bloomfilteru
	M_SIZE              = 8                                                             // prvih 8 bajtova bloomfilter segmenta sadrze podatke o M
	K_SIZE              = 8
	FALSE_POSITIVE_RATE = 0.001 // velicina svakog polja u kome ce se cuvati velicinu Seed-a da bi pomocu nje ucitali tacno odredjen broj bajtova
)

type SSTable struct {
	dataSize     uint64                  // velicina data segmenta
	indexSize    uint64                  // velicina index segmenta
	summarySize  uint64                  // velicina summary segmenta
	summary      uint64                  // tacna pozicija summary segmenta za slucaj pretrage u njemu
	blockLeaders []string                // pomocna lista kljuceva
	blockIndexes []uint64                // pomocna lista indeksa
	indexLeaders []string                // pomocna litsa kljuceva
	IndexIndexes []uint64                // pomocna lista indeksa
	bF           bloomfilter.BloomFilter // Bloom Filter SSTable
	bFPosition   uint64                  // tacna pozicija BF, sluzi za zapis u header
	bFDataSize   uint64                  // velicina data zone u BF, isto zarad header-a
	merkleData   [][]byte
	path         string
}

// upis odredjene kolicine podataka u fajl
func writeBlock(recordByte *[]byte, path string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600) // unece se jos jedan parametar strukture SSTable za ime fajla
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	w.Write(*recordByte)
	w.Flush()
}

// pocetak upisa, prima sve "slogove" za upis
func writeSSTable(allRecords *[]record.Record, sstable *SSTable) {
	var block_size uint
	// fmt.Print("Unesite velicinu data bloka: ") // bira se velicina bloka kojim ce biti odvojene sektori za pretragu
	// fmt.Scan(&block_size)
	block_size = 2
	for i, record := range *allRecords {

		// upis u merkle
		in := append([]byte(record.Key), record.Value...)
		sstable.merkleData = append(sstable.merkleData, in)
		in = nil

		sstable.bF.Add([]byte(record.Key))
		if i%int(block_size) == 0 {
			sstable.blockLeaders = append(sstable.blockLeaders, record.Key)
			sstable.blockIndexes = append(sstable.blockIndexes, sstable.dataSize+HEADER_SIZE)
		}
		var thumbstoneByte byte
		if record.Tombstone {
			thumbstoneByte = 1
		} else {
			thumbstoneByte = 0
		}
		// kreiranje velicine niza bajtova koji ce se upisati
		recordByte := make([]byte, len([]byte(record.Key))+len(record.Value)+KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN+TOMBSTONE_LEN)
		sstable.dataSize += uint64(len(recordByte))

		binary.LittleEndian.PutUint64(recordByte[0:KEY_SIZE_LEN], uint64(len([]byte(record.Key))))
		binary.LittleEndian.PutUint64(recordByte[KEY_SIZE_LEN:KEY_SIZE_LEN+VALUE_SIZE_LEN], uint64(len(record.Value)))
		binary.LittleEndian.PutUint64(recordByte[KEY_SIZE_LEN+VALUE_SIZE_LEN:KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN], uint64(record.Timestamp))
		recordByte[KEY_SIZE_LEN+VALUE_SIZE_LEN+TIMESTAMP_LEN] = byte(thumbstoneByte)
		copy(recordByte[KEY_VALUE_START:KEY_VALUE_START+len([]byte(record.Key))], []byte(record.Key))
		copy(recordByte[KEY_VALUE_START+len([]byte(record.Key)):KEY_VALUE_START+len([]byte(record.Key))+len(record.Value)], record.Value)

		writeBlock(&recordByte, sstable.path)
	}
	writeIndex(sstable)       // upisi index zonu
	writeHeader(sstable)      // upisi header
	writeSummary(sstable)     // upis summary-a
	writeBloomFilter(sstable) // kreiranje BF za potrebe memorisanja a zarad kasnijeg pretrazivanja
	merkle.BuildMerkleTree(sstable.merkleData)
}

// upisuje zaglavlje fajla sa neophodnim podacima
func writeHeader(sstable *SSTable) {
	f, err := os.OpenFile(sstable.path, os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	f.Seek(0, 0)
	// upis velicine data segmenta da bi se mogli pozicionirati u index zonu
	err = binary.Write(f, binary.LittleEndian, sstable.dataSize+HEADER_SIZE)
	if err != nil {
		println(err)
		return
	}
	// upis velicine index zone da bi se mogli pozicionirati u summary zonu
	err = binary.Write(f, binary.LittleEndian, sstable.indexSize+HEADER_SIZE)
	if err != nil {
		println(err)
		return
	}
	// upis tacne pozicije BF
	err = binary.Write(f, binary.LittleEndian, sstable.bFPosition)
	if err != nil {
		println(err)
		return
	}
	// upis velicine data zone bloom filtera
	err = binary.Write(f, binary.LittleEndian, sstable.bFDataSize)
	if err != nil {
		println(err)
		return
	}

}

// upis zone indeksa
func writeIndex(sstable *SSTable) {
	var block_size uint
	// fmt.Print("Unesite velicinu index bloka: ")
	// fmt.Scan(&block_size)
	block_size = 2
	for i, key := range sstable.blockLeaders {
		if i%int(block_size) == 0 {
			sstable.indexLeaders = append(sstable.indexLeaders, key)
			sstable.IndexIndexes = append(sstable.IndexIndexes, sstable.dataSize+HEADER_SIZE+sstable.indexSize)
		}
		recordByte := make([]byte, K_SIZE+len([]byte(key))+VALUE_SIZE_LEN)
		sstable.indexSize += uint64(len(recordByte))
		binary.LittleEndian.PutUint64(recordByte[0:K_SIZE], uint64(len([]byte(key))))
		copy(recordByte[K_SIZE:K_SIZE+len([]byte(key))], []byte(key))
		binary.LittleEndian.PutUint64(recordByte[K_SIZE+len([]byte(key)):], sstable.blockIndexes[i])

		writeBlock(&recordByte, sstable.path)
	}

	sstable.summary = sstable.dataSize + sstable.indexSize + HEADER_SIZE
}

// upis summary zone
func writeSummary(sstable *SSTable) {
	for i, key := range sstable.indexLeaders {
		recordByte := make([]byte, K_SIZE+len([]byte(key))+VALUE_SIZE_LEN)
		sstable.summarySize += uint64(len(recordByte))
		binary.LittleEndian.PutUint64(recordByte[0:K_SIZE], uint64(len([]byte(key))))
		copy(recordByte[K_SIZE:K_SIZE+len([]byte(key))], []byte(key))
		binary.LittleEndian.PutUint64(recordByte[K_SIZE+len([]byte(key)):], sstable.IndexIndexes[i])

		writeBlock(&recordByte, sstable.path)
	}
	sstable.bFPosition = sstable.summary + sstable.summarySize
}

// upis BF
func writeBloomFilter(sstable *SSTable) {
	recordByte := make([]byte, M_SIZE+len(sstable.bF.Data))
	binary.LittleEndian.PutUint64(recordByte[0:M_SIZE], uint64(sstable.bF.M))
	copy(recordByte[M_SIZE:], sstable.bF.Data)
	writeBlock(&recordByte, sstable.path)
	sstable.bFDataSize = uint64(len(sstable.bF.Data))
	writeHeader(sstable)
	for _, hashFunc := range sstable.bF.HashFunctions {
		recordByte := make([]byte, K_SIZE+len(hashFunc.Seed))
		binary.LittleEndian.PutUint64(recordByte[0:K_SIZE], uint64(len(hashFunc.Seed)))
		copy(recordByte[K_SIZE:], hashFunc.Seed)
		writeBlock(&recordByte, sstable.path)
	}
}

// poziv za kreiranje SSTable-a
func NewSSTable(allRecords *[]record.Record) {
	var sstable SSTable
	sstable.path = "file.db"             // dodati random naziva na ime
	file, err := os.Create(sstable.path) // nekom metodom davati imena, npr u ms vreme ili tako nes
	if err != nil {
		panic(err)
	}
	defer file.Close()
	sstable.dataSize = 0
	sstable.indexSize = 0
	sstable.summarySize = 0
	sstable.bFDataSize = 0
	writeHeader(&sstable)

	sstable.bF = *bloomfilter.NewBloomFilter(len(*allRecords), FALSE_POSITIVE_RATE)
	writeSSTable(allRecords, &sstable)
}
