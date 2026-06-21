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

	buffer := make([]byte, 32384)
	sessions := make(map[uint16]*Session)
	statuses := make(map[uint16]string)

	doneChan := make(chan AssembleResult, 16) // buffered so it doesn't freeze

	for {
		connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

		n, remoteAddr, err := connection.ReadFrom(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// No packet arrived. Check completed goroutines.
				select {
				case result := <-doneChan:
					handleAssembleResult(connection, statuses, result)

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

		seq, err := protocol.GetSequenceNumber(packet)
		if err != nil {
			return err
		}

		err = session.AddPacket(packet)
		fmt.Println("packet added to session:", id)
		if err != nil {
			return err
		}

		if session.FirstReceived && remoteAddr != nil {
			if seq == session.MaxSeq+1 && !session.IsComplete() {
				missing := session.MissingSequences(350)
				_, err = connection.WriteTo(protocol.BuildNAKPacket(id, missing), remoteAddr)
				if err != nil {
					return err
				}
				fmt.Println("nak sent, missing:", len(missing))
			} else {
				_, err = connection.WriteTo(protocol.BuildACKPacket(id, session.ACKBase()), remoteAddr)
				if err != nil {
					return err
				}
				fmt.Println("ack sent, base:", session.ACKBase())
			}
		}

		// As soon as one of the files has received all its packages,
		// its verification and assembly is initialized in a separate thread
		if session.IsComplete() {
			fmt.Println("session complete:", session.FileName)

			statuses[id] = "assembling"

			delete(sessions, id)

			go AssembleAndSave(session, remoteAddr, doneChan)
		}

		// Cleaning up incomplete transfer sessions and late duplicates
		cleanupOldSessions(sessions, statuses, 10*time.Second)

		select {
		case result := <-doneChan:
			handleAssembleResult(connection, statuses, result)

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

func handleAssembleResult(connection net.PacketConn, statuses map[uint16]string, result AssembleResult) {
	if result.OK {
		fmt.Println("file received successfully, id:", result.ID)
		fmt.Println("receive time:", result.Duration)
		if result.Addr != nil {
			_, err := connection.WriteTo(protocol.BuildCompletePacket(result.ID), result.Addr)
			if err != nil {
				fmt.Println("complete send failed, id:", result.ID, "error:", err)
			} else {
				fmt.Println("complete sent, id:", result.ID)
			}
		}
	} else {
		fmt.Println("file assembly failed, id:", result.ID, "error:", result.Err)
		fmt.Println("receive time before failure:", result.Duration)
	}

	delete(statuses, result.ID)
}
