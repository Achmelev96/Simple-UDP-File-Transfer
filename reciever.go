package main

import (
	"Simple_UDP_File_Transfer/protocol"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

func ReceiveFlow(addr string) error {
	connection, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer connection.Close()

	buffer := make([]byte, 2048)
	sessions := make(map[uint16]*Session)

	for {
		n, _, err := connection.ReadFrom(buffer)
		if err != nil {
			return err
		}

		packet := make([]byte, n)
		copy(packet, buffer[:n])

		id, err := protocol.GetTransmissionID(packet)
		if err != nil {
			return err
		}

		session, ok := sessions[id]
		if !ok {
			session = NewSession(id)
			sessions[id] = session
		}

		err = session.AddPacket(packet)
		if err != nil {
			return err
		}

		if session.IsComplete() {
			fileData := session.BuildFile()

			err = session.ValidateMD5(fileData)
			if err != nil {
				return err
			}

			err = SaveReceivedFile(session.FileName, fileData)
			if err != nil {
				return err
			}

			fmt.Println("file received successfully:", session.FileName)

			delete(sessions, id)
		}
	}
}

func SaveReceivedFile(fileName string, data []byte) error {
	baseName := filepath.Base(fileName)
	outputName := filepath.Join("testdata", "received_"+baseName)
	return os.WriteFile(outputName, data, 0644)
}
