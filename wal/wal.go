package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/vradovic/naisp-projekat/globals"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)

	return &WAL{file, writer}, nil
}

func (w *WAL) Write(key, value []byte, timestamp int64, thumbstone bool) error {
	// Calculate the payload length
	payloadLength := globals.CRC_SIZE + globals.TIMESTAMP_SIZE + globals.TOMBSTONE_SIZE + globals.KEY_SIZE_SIZE + globals.VALUE_SIZE_SIZE + len(key) + len(value)

	// Allocate the payload buffer
	payload := make([]byte, payloadLength)

	// Write the timestamp to the payload
	binary.LittleEndian.PutUint64(payload[globals.TIMESTAMP_START:globals.TIMESTAMP_START+globals.TIMESTAMP_SIZE], uint64(timestamp))

	// Write the thumbstone flag to the payload
	var thumbstoneByte byte
	if thumbstone {
		thumbstoneByte = 1
	} else {
		thumbstoneByte = 0
	}
	payload[globals.TOMBSTONE_START] = thumbstoneByte

	// Write the key size to the payload
	binary.LittleEndian.PutUint64(payload[globals.KEY_SIZE_START:globals.KEY_SIZE_START+globals.KEY_SIZE_SIZE], uint64(len(key)))

	// Write the value size to the payload
	binary.LittleEndian.PutUint64(payload[globals.VALUE_SIZE_START:globals.VALUE_SIZE_START+globals.VALUE_SIZE_SIZE], uint64(len(value)))

	// Write the key and value to the payload
	copy(payload[globals.KEY_START:globals.KEY_START+len(key)], key)
	copy(payload[globals.KEY_START+len(key):], value)

	// Compute the CRC
	crc := CRC32(value)

	// Write the CRC to the payload
	binary.LittleEndian.PutUint32(payload[globals.CRC_START:globals.CRC_START+globals.CRC_SIZE], crc)

	// Write the payload to the WAL
	w.writer.Write(payload)

	// Flush to disk
	w.writer.Flush()

	return nil
}

func (w *WAL) Close() error {
	w.writer.Flush()
	return w.file.Close()

}

// func main() {
// 	wal, err := NewWAL()
// 	if err != nil {
// 		panic("greska")
// 	}
// 	defer wal.Close()

// 	key := []byte("example_key")
// 	value := []byte("example_value")

// 	err = wal.Write(key, value, time.Now().UnixNano(), false)
// 	if err != nil {
// 		panic("greska")
// 	}
// }
