package main

import (
	"Simple_UDP_File_Transfer/protocol"
	"fmt"
	"net"
	"time"
)

func ReceiveFlow(addr string) error {
	connection, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}
	defer connection.Close()

	buffer := make([]byte, 4096)
	sessions := make(map[uint16]*Session)
	statuses := make(map[uint16]string)

	doneChan := make(chan AssembleResult, 16) // buffered so it doesn't freeze

	for {
		connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		n, _, err := connection.ReadFrom(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// No packet arrived. Check completed goroutines.
				select {
				case result := <-doneChan:
					if result.OK {
						fmt.Println("file received successfully, id:", result.ID)
						fmt.Println("receive time:", result.Duration)
					} else {
						fmt.Println("file assembly failed, id:", result.ID, "error:", result.Err)
						fmt.Println("receive time before failure:", result.Duration)
					}

					delete(statuses, result.ID)

				default:
				}

				continue
			}
			return err
		}

		fmt.Println("received packet, bytes:", n)
		packet := make([]byte, n)
		copy(packet, buffer[:n])

		// Receive the packet and read its ID
		// If there is no session with this ID, create a new session for it
		id, err := protocol.GetTransmissionID(packet)
		if err != nil {
			return err
		}

		if statuses[id] == "assembling" {
			fmt.Println("packet ignored, session is assembling:", id)
			continue
		}

		session, ok := sessions[id]
		if !ok {
			session = NewSession(id)
			sessions[id] = session
			statuses[id] = "receiving"
		}

		err = session.AddPacket(packet)
		fmt.Println("packet added to session:", id)
		if err != nil {
			return err
		}

		// As soon as one of the files has received all its packages,
		// its verification and assembly is initialized in a separate thread
		if session.IsComplete() {
			fmt.Println("session complete:", session.FileName)

			statuses[id] = "assembling"

			delete(sessions, id)

			go AssembleAndSave(session, doneChan)
		}

		// Cleaning up incomplete transfer sessions and late duplicates
		cleanupOldSessions(sessions, statuses, 10*time.Second)

		select {
		case result := <-doneChan:
			if result.OK {
				fmt.Println("file received successfully, id:", result.ID)
			} else {
				fmt.Println("file assembly failed, id:", result.ID, "error:", result.Err)
			}

			delete(statuses, result.ID)

		default:
		}
	}
}

func cleanupOldSessions(sessions map[uint16]*Session, statuses map[uint16]string, timeout time.Duration) {
	now := time.Now()

	for id, session := range sessions {
		if statuses[id] == "assembling" {
			continue
		}

		if now.Sub(session.LastSeen) > timeout {
			//fmt.Println("session dropped by timeout, id:", id)
			delete(sessions, id)
			delete(statuses, id)
		}
	}
}
