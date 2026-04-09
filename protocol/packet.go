package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	IDSize     = 2
	SeqSize    = 4
	HeaderSize = IDSize + SeqSize

	MaxSeqSize = 4 // MaxSeq
	MD5Size    = 16

	FirstMinSize = HeaderSize + MaxSeqSize // 10
	LastSize     = HeaderSize + MD5Size    // 22
)

func GetTransmissionID(packet []byte) (uint16, error) {
	if len(packet) < IDSize {
		return 0, fmt.Errorf("packet is too short(id)")
	}
	return binary.BigEndian.Uint16(packet[0:2]), nil
}

func GetSequenceNumber(packet []byte) (uint32, error) {
	if len(packet) < HeaderSize {
		return 0, fmt.Errorf("packet is too short(seq)")
	}
	return binary.BigEndian.Uint32(packet[2:6]), nil
}

func ParseFirstPacket(packet []byte) (uint16, uint32, string, error) {
	if len(packet) < FirstMinSize {
		return 0, 0, "", fmt.Errorf("too short for first packet")
	}

	id := binary.BigEndian.Uint16(packet[0:2])
	seq := binary.BigEndian.Uint32(packet[2:6])
	if seq != 0 {
		return 0, 0, "", fmt.Errorf("seq is %d hmmm", seq)
	}

	maxSeq := binary.BigEndian.Uint32(packet[6:10])
	fileName := string(packet[10:])

	return id, maxSeq, fileName, nil
}

func ParseDataPacket(packet []byte) (uint16, uint32, []byte, error) {
	if len(packet) < HeaderSize {
		return 0, 0, nil, fmt.Errorf("data packet too short")
	}

	id := binary.BigEndian.Uint16(packet[0:2])
	seq := binary.BigEndian.Uint32(packet[2:6])

	data := make([]byte, len(packet[6:]))
	copy(data, packet[6:])

	return id, seq, data, nil
}

func ParseLastPacket(packet []byte) (uint16, uint32, []byte, error) {
	if len(packet) != LastSize {
		return 0, 0, nil, fmt.Errorf("wrong size: got %d, want %d", len(packet), LastSize)
	}

	id := binary.BigEndian.Uint16(packet[0:2])
	seq := binary.BigEndian.Uint32(packet[2:6])

	md5Bytes := make([]byte, MD5Size)
	copy(md5Bytes, packet[6:22])

	return id, seq, md5Bytes, nil
}
