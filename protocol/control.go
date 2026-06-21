package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	ControlTypeNAK      byte = 0
	ControlTypeComplete byte = 1
	ControlTypeACK      byte = 2

	ControlHeaderSize = 5
	ControlACKSize    = ControlHeaderSize + 4
)

type ControlPacket struct {
	ID      uint16
	Type    byte
	Count   uint16
	Missing []uint32
	ACKBase uint32
}

func BuildACKPacket(id uint16, ackBase uint32) []byte {
	packet := make([]byte, ControlACKSize)
	binary.BigEndian.PutUint16(packet[0:2], id)
	packet[2] = ControlTypeACK
	binary.BigEndian.PutUint16(packet[3:5], 1)
	binary.BigEndian.PutUint32(packet[5:9], ackBase)
	return packet
}

func BuildNAKPacket(id uint16, missing []uint32) []byte {
	packet := make([]byte, ControlHeaderSize+len(missing)*SeqSize)
	binary.BigEndian.PutUint16(packet[0:2], id)
	packet[2] = ControlTypeNAK
	binary.BigEndian.PutUint16(packet[3:5], uint16(len(missing)))
	offset := ControlHeaderSize

	for _, seq := range missing {
		binary.BigEndian.PutUint32(packet[offset:offset+SeqSize], seq)
		offset += SeqSize
	}
	return packet
}

func BuildCompletePacket(id uint16) []byte {
	packet := make([]byte, ControlHeaderSize)
	binary.BigEndian.PutUint16(packet[0:2], id)
	packet[2] = ControlTypeComplete
	binary.BigEndian.PutUint16(packet[3:5], 0)
	return packet
}

func ParseControlPacket(packet []byte) (*ControlPacket, error) {
	if len(packet) < ControlHeaderSize {
		return nil, fmt.Errorf("control packet too short")
	}

	control := &ControlPacket{
		ID:    binary.BigEndian.Uint16(packet[0:2]),
		Type:  packet[2],
		Count: binary.BigEndian.Uint16(packet[3:5]),
	}

	switch control.Type {
	case ControlTypeNAK:
		want := ControlHeaderSize + int(control.Count)*SeqSize
		if len(packet) != want {
			return nil, fmt.Errorf("wrong nak size: got %d, want %d", len(packet), want)
		}
		control.Missing = make([]uint32, 0, control.Count)
		for offset := ControlHeaderSize; offset < len(packet); offset += SeqSize {
			control.Missing = append(control.Missing, binary.BigEndian.Uint32(packet[offset:offset+SeqSize]))
		}
	case ControlTypeComplete:
		if control.Count != 0 || len(packet) != ControlHeaderSize {
			return nil, fmt.Errorf("wrong complete packet")
		}
	case ControlTypeACK:
		if len(packet) != ControlACKSize {
			return nil, fmt.Errorf("wrong ack size: got %d, want %d", len(packet), ControlACKSize)
		}
		control.ACKBase = binary.BigEndian.Uint32(packet[5:9])
	default:
		return nil, fmt.Errorf("unknown control type: %d", control.Type)
	}
	return control, nil
}
