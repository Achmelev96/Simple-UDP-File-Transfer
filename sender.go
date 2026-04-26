package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

const chunkSize = 1400

func SendFlow(path string, addr string) error {
	data, name, err := ReadFileInBytes(path)
	if err != nil {
		return err
	}

	id := GenerateID()
	md5 := CalculateMD5(data)
	chunks := BuildDataChunks(data, chunkSize)
	packets := BuildPackets(id, name, chunks, md5)

	fmt.Println("packets built:", len(packets))
	err = Send(addr, packets)
	if err != nil {
		return err
	}
	return nil
}

func ReadFileInBytes(filename string) ([]byte, string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		//fmt.Println("Read error")
		return nil, "", err
	}

	name := filepath.Base(filename)

	return data, name, nil
}

func CalculateMD5(data []byte) []byte {
	hash := md5.Sum(data)
	md5Bytes := hash[:]
	return md5Bytes
}

func GenerateID() uint16 {
	return uint16(time.Now().UnixNano())
}

func BuildDataChunks(data []byte, chunkSize int) [][]byte {
	if chunkSize < 1 {
		//fmt.Println("Chunk size must be greater than 0")
		return nil
	}

	chunks := make([][]byte, 0, (len(data)+chunkSize-1)/chunkSize)

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[i:end]
		chunks = append(chunks, chunk)
	}
	return chunks
}

func BuildPackets(id uint16, filename string, chunks [][]byte, md5 []byte) [][]byte {
	size := len(chunks) + 2
	maxSeq := uint32(len(chunks))

	finishedPacket := make([][]byte, 0, size)

	// appending fist part
	firstPiece := BuildFirstPacket(id, maxSeq, filename)
	finishedPacket = append(finishedPacket, firstPiece)

	// appending data part
	for i := 0; i < len(chunks); i++ {
		dataChunk := chunks[i]

		dataPiece := BuildDataPacket(id, uint32(i+1), dataChunk)
		finishedPacket = append(finishedPacket, dataPiece)
	}

	// appending last part
	lastPiece := BuildLastPacket(id, maxSeq+1, md5)
	finishedPacket = append(finishedPacket, lastPiece)

	return finishedPacket
}

func BuildFirstPacket(id uint16, maxSeq uint32, filename string) []byte {
	size := 10 + len(filename)

	packet := make([]byte, size)
	binary.BigEndian.PutUint16(packet[0:2], id)      // ID
	binary.BigEndian.PutUint32(packet[2:6], 0)       // number (0)
	binary.BigEndian.PutUint32(packet[6:10], maxSeq) // maximal number
	copy(packet[10:], []byte(filename))              //File name

	return packet
}

func BuildLastPacket(id uint16, seq uint32, md5 []byte) []byte {
	if len(md5) != 16 {
		panic("md5 must be 16 bytes")
	}

	size := 22

	packet := make([]byte, size)
	binary.BigEndian.PutUint16(packet[0:2], id)  // ID
	binary.BigEndian.PutUint32(packet[2:6], seq) // Number (n+1)
	copy(packet[6:], md5)                        // md5

	return packet
}

func BuildDataPacket(id uint16, seq uint32, chunk []byte) []byte {
	size := 6 + len(chunk)

	packet := make([]byte, size)
	binary.BigEndian.PutUint16(packet[0:2], id)  // ID
	binary.BigEndian.PutUint32(packet[2:6], seq) // Number (1 .. n)
	copy(packet[6:], chunk)                      // data

	return packet
}

func Send(addr string, packets [][]byte) error {
	connection, err := net.Dial("udp", addr)
	if err != nil {
		return err
	}
	defer connection.Close()

	for _, packet := range packets {
		_, err := connection.Write(packet)
		fmt.Println("sending packet, bytes:", len(packet))
		if err != nil {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}

	return nil
}
