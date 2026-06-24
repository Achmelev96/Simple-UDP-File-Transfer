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
	logger := NewTransferLogger()
	err = Send(addr, packets, logger)
	if err != nil {
		return err
	}

	logger.PrintSendSummary(id, time.Since(start))
	fmt.Println("file sent successfully:", name)

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

func Send(addr string, packets [][]byte, logger *TransferLogger) error {
	connection, err := net.Dial("udp", addr)
	if err != nil {
		return err
	}
	defer connection.Close()

	if len(packets) < 2 {
		return fmt.Errorf("no packets to send")
	}

	_, err = connection.Write(packets[0])
	logger.LogDataPacket(directionSent, packets[0])
	if err != nil {
		return err
	}

	windowEnabled, err := waitForFirstACK(connection, logger)
	if err != nil {
		return err
	}

	id, _ := protocol.GetTransmissionID(packets[0])
	logger.SetWindowEnabled(id, windowEnabled)

	if windowEnabled {
		return sendWithWindow(connection, packets, logger)
	}
	return sendSimple(connection, packets, logger)
}

func waitForFirstACK(connection net.Conn, logger *TransferLogger) (bool, error) {
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

	logger.LogControlPacket(directionReceived, control)
	if control.Type == protocol.ControlTypeACK {
		return true, nil
	}
	return false, nil
}

func sendSimple(connection net.Conn, packets [][]byte, logger *TransferLogger) error {
	for i := 1; i < len(packets); i++ {
		_, err := connection.Write(packets[i])
		logger.LogDataPacket(directionSent, packets[i])

		if err != nil {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
	return listenForRepair(connection, packets, 20, logger)
}

func sendWithWindow(connection net.Conn, packets [][]byte, logger *TransferLogger) error {
	const windowSize = 64
	const maxIdle = 50

	maxSeq := uint32(len(packets) - 2)
	base := uint32(1)
	nextSeq := uint32(1)
	idle := 0

	for base <= maxSeq {
		for nextSeq <= maxSeq && nextSeq < base+windowSize {
			err := sendPacketBySeq(connection, packets, nextSeq, logger)
			if err != nil {
				return err
			}
			nextSeq++
		}

		control, err := readControl(connection, 100*time.Millisecond, logger)
		if err != nil {
			return err
		}
		if control == nil {
			idle++
			if idle >= maxIdle {
				if err := resendWindow(connection, packets, base, nextSeq, logger); err != nil {
					return err
				}
				idle = 0

			}
			continue
		}

		idle = 0
		switch control.Type {
		case protocol.ControlTypeACK:
			if control.ACKBase > base {
				base = control.ACKBase
			}
		case protocol.ControlTypeNAK:
			err = resendMissing(connection, packets, control.Missing, logger)
			if err != nil {
				return err
			}
		case protocol.ControlTypeComplete:
			return nil
		}
	}

	err := sendPacketBySeq(connection, packets, maxSeq+1, logger)
	if err != nil {
		return err
	}

	return listenForRepair(connection, packets, 50, logger)
}

// The window from which packets were sent but not received
// Packets outside this window cannot be requested
func resendWindow(connection net.Conn, packets [][]byte, base uint32, nextSeq uint32, logger *TransferLogger) error {
	for seq := base; seq < nextSeq; seq++ {
		if err := sendPacketBySeq(connection, packets, seq, logger); err != nil {
			return err
		}
	}
	return nil
}

func listenForRepair(connection net.Conn, packets [][]byte, maxIdle int, logger *TransferLogger) error {

	for idle := 0; idle < maxIdle; idle++ {
		control, err := readControl(connection, 100*time.Millisecond, logger)

		if err != nil {
			return err
		}
		if control == nil {
			continue
		}

		switch control.Type {
		case protocol.ControlTypeNAK:
			err = resendMissing(connection, packets, control.Missing, logger)

			if err != nil {
				return err
			}
			err = sendPacketBySeq(connection, packets, uint32(len(packets)-1), logger)
			if err != nil {
				return err
			}
			idle = 0

		case protocol.ControlTypeComplete:
			return nil
		}
	}
	return nil
}

func readControl(connection net.Conn, timeout time.Duration, logger *TransferLogger) (*protocol.ControlPacket, error) {
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
	logger.LogControlPacket(directionReceived, control)
	return control, nil
}

func resendMissing(connection net.Conn, packets [][]byte, missing []uint32, logger *TransferLogger) error {
	for _, seq := range missing {
		err := sendPacketBySeq(connection, packets, seq, logger)

		if err != nil {
			return err
		}
	}
	return nil
}

func sendPacketBySeq(connection net.Conn, packets [][]byte, seq uint32, logger *TransferLogger) error {
	if int(seq) >= len(packets) {
		return fmt.Errorf("packet seq out of range: %d", seq)
	}

	_, err := connection.Write(packets[seq])
	logger.LogDataPacket(directionSent, packets[seq])
	if err != nil {
		return err
	}
	return nil
}
