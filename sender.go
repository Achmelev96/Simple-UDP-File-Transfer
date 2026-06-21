package main

import (
	"Simple_UDP_File_Transfer/protocol"
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
	start := time.Now()

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

	// send timer
	fmt.Println("file sent successfully:", name)
	fmt.Println("send time:", time.Since(start))

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

	if len(packets) < 2 {
		return fmt.Errorf("no packets to send")
	}

	_, err = connection.Write(packets[0])
	fmt.Println("sending packet, bytes:", len(packets[0]))
	if err != nil {
		return err
	}

	windowEnabled, err := waitForFirstACK(connection)
	if err != nil {
		return err
	}

	if windowEnabled {
		fmt.Println("control protocol enabled")
		return sendWithWindow(connection, packets)
	}

	fmt.Println("control protocol not detected, sending without window")
	return sendSimple(connection, packets)
}

func waitForFirstACK(connection net.Conn) (bool, error) {
	buffer := make([]byte, 4096)
	_ = connection.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	n, err := connection.Read(buffer)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false, nil
		}
		return false, err
	}

	control, err := protocol.ParseControlPacket(buffer[:n])
	if err != nil {
		return false, nil
	}

	if control.Type == protocol.ControlTypeACK {
		fmt.Println("ack received, base:", control.ACKBase)
		return true, nil
	}
	return false, nil
}

func sendSimple(connection net.Conn, packets [][]byte) error {
	for i := 1; i < len(packets); i++ {
		_, err := connection.Write(packets[i])
		fmt.Println("sending packet, bytes:", len(packets[i]))

		if err != nil {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
	return listenForRepair(connection, packets, 20)
}

func sendWithWindow(connection net.Conn, packets [][]byte) error {
	const windowSize = 64
	const maxIdle = 50

	maxSeq := uint32(len(packets) - 2)
	base := uint32(1)
	nextSeq := uint32(1)
	idle := 0

	for base <= maxSeq || nextSeq <= maxSeq {
		for nextSeq <= maxSeq && nextSeq < base+windowSize {
			err := sendPacketBySeq(connection, packets, nextSeq)
			if err != nil {
				return err
			}
			nextSeq++
		}

		control, err := readControl(connection, 100*time.Millisecond)
		if err != nil {
			return err
		}
		if control == nil {
			idle++
			if idle >= maxIdle {
				break
			}
			continue
		}

		idle = 0
		switch control.Type {
		case protocol.ControlTypeACK:
			fmt.Println("ack received, base:", control.ACKBase)
			if control.ACKBase > base {
				base = control.ACKBase
			}
		case protocol.ControlTypeNAK:
			err = resendMissing(connection, packets, control.Missing)
			if err != nil {
				return err
			}
		case protocol.ControlTypeComplete:
			fmt.Println("complete received")
			return nil
		}
	}

	err := sendPacketBySeq(connection, packets, maxSeq+1)
	if err != nil {
		return err
	}

	return listenForRepair(connection, packets, 50)
}

func listenForRepair(connection net.Conn, packets [][]byte, maxIdle int) error {
	for idle := 0; idle < maxIdle; idle++ {
		control, err := readControl(connection, 100*time.Millisecond)

		if err != nil {
			return err
		}
		if control == nil {
			continue
		}

		switch control.Type {
		case protocol.ControlTypeNAK:
			fmt.Println("nak received, missing:", len(control.Missing))
			err = resendMissing(connection, packets, control.Missing)
			if err != nil {
				return err
			}
			err = sendPacketBySeq(connection, packets, uint32(len(packets)-1))
			if err != nil {
				return err
			}
			idle = 0
		case protocol.ControlTypeACK:
			fmt.Println("ack received, base:", control.ACKBase)
		case protocol.ControlTypeComplete:
			fmt.Println("complete received")
			return nil
		}
	}
	return nil
}

func readControl(connection net.Conn, timeout time.Duration) (*protocol.ControlPacket, error) {
	buffer := make([]byte, 4096)
	_ = connection.SetReadDeadline(time.Now().Add(timeout))

	n, err := connection.Read(buffer)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil, nil
		}
		return nil, err
	}

	control, err := protocol.ParseControlPacket(buffer[:n])
	if err != nil {
		fmt.Println("control packet ignored:", err)
		return nil, nil
	}
	return control, nil
}

func resendMissing(connection net.Conn, packets [][]byte, missing []uint32) error {
	for _, seq := range missing {
		err := sendPacketBySeq(connection, packets, seq)

		if err != nil {
			return err
		}
	}
	return nil
}

func sendPacketBySeq(connection net.Conn, packets [][]byte, seq uint32) error {
	if int(seq) >= len(packets) {
		return fmt.Errorf("packet seq out of range: %d", seq)
	}

	_, err := connection.Write(packets[seq])
	fmt.Println("sending packet, bytes:", len(packets[seq]))
	if err != nil {
		return err
	}
	return nil
}
