package main

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
	"time"
)

const (
	WAL_FILE        = "../resources/wal.log"
	MAX_ENTRY_SIZE  = 1024
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func NewWAL() (*WAL, error) {
	file, err := os.OpenFile(WAL_FILE, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)

	return &WAL{file, writer}, nil
}

func (w *WAL) Write(key, value []byte, thumbstone bool) error {
	// Calculate the payload length
	payloadLength := TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + len(key) + len(value)

	// Allocate the payload buffer
	payload := make([]byte, payloadLength)

	// Write the timestamp to the payload
	timestamp := time.Now().UnixNano()
	binary.LittleEndian.PutUint64(payload[TIMESTAMP_START:TIMESTAMP_START+TIMESTAMP_SIZE], uint64(timestamp))

	// Write the thumbstone flag to the payload
	var thumbstoneByte byte
	if thumbstone {
		thumbstoneByte = 1
	} else {
		thumbstoneByte = 0
	}
	payload[TOMBSTONE_START] = thumbstoneByte

	// Write the key size to the payload
	binary.LittleEndian.PutUint64(payload[KEY_SIZE_START:KEY_SIZE_START+KEY_SIZE_SIZE], uint64(len(key)))

	// Write the value size to the payload
	binary.LittleEndian.PutUint64(payload[VALUE_SIZE_START:VALUE_SIZE_START+VALUE_SIZE_SIZE], uint64(len(value)))

	// Write the key and value to the payload
	copy(payload[KEY_START:KEY_START+len(key)], key)
	copy(payload[KEY_START+len(key):], value)

	// Compute the CRC
	crc := CRC32(value)

	// Write the CRC to the payload
	binary.LittleEndian.PutUint32(payload[CRC_START:CRC_START+CRC_SIZE], crc)

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
func main() {
	wal, err := NewWAL()
	if err != nil {
		// handle error
	}
	defer wal.Close()

	key := []byte("example_key")
	value := []byte("example_value")

	err = wal.Write(key, value, false)
	if err != nil {
		// handle error
	}
}
