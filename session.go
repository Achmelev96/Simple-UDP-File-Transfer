package main

import (
	"Simple_UDP_File_Transfer/protocol"
	"bytes"
	"crypto/md5"
	"fmt"
	"net"
	"time"
)

type Session struct {
	ID             uint16
	FileName       string
	MaxSeq         uint32
	HighestDataSeq uint32

	Chunks   map[uint32][]byte
	Received map[uint32]bool

	MD5    []byte
	HasEnd bool

	FirstReceived bool

	Pending    map[uint32][]byte // before fist packet
	LastSeen   time.Time         // timer for cleanup
	StartTime  time.Time         // timer for receiving
	RemoteAddr net.Addr          // address for ACK/NAK
	LastRepair time.Time         // timer for NAK
}

func NewSession(id uint16) *Session {
	return &Session{
		ID:        id,
		Chunks:    make(map[uint32][]byte),
		Received:  make(map[uint32]bool),
		Pending:   make(map[uint32][]byte),
		LastSeen:  time.Now(),
		StartTime: time.Now(),
	}
}

func (s *Session) AddPacket(packet []byte) error {
	id, err := protocol.GetTransmissionID(packet)
	if err != nil {
		return err
	}

	if s.ID != 0 && s.ID != id {
		return fmt.Errorf("id problem: got %d, expected %d", id, s.ID)
	}

	if s.ID == 0 {
		s.ID = id
	}

	seq, err := protocol.GetSequenceNumber(packet)
	if err != nil {
		return err
	}

	s.LastSeen = time.Now() // update timer

	// First
	if seq == 0 {
		_, maxSeq, fileName, err := protocol.ParseFirstPacket(packet)
		if err != nil {
			return err
		}

		s.MaxSeq = maxSeq
		s.FileName = fileName
		s.FirstReceived = true

		return s.processPending()
	}

	// If the first packet hasn't arrived yet, put everything in the "pending"
	if !s.FirstReceived {
		raw := make([]byte, len(packet))
		copy(raw, packet)
		s.Pending[seq] = raw
		return nil
	}

	// last
	if seq == s.MaxSeq+1 {
		_, _, md5Bytes, err := protocol.ParseLastPacket(packet)
		if err != nil {
			return err
		}
		s.MD5 = md5Bytes
		s.HasEnd = true
		return nil
	}

	// Data
	_, dataSeq, data, err := protocol.ParseDataPacket(packet)
	if err != nil {
		return err
	}

	s.Chunks[dataSeq] = data
	s.Received[dataSeq] = true
	if dataSeq > s.HighestDataSeq {
		s.HighestDataSeq = dataSeq
	}
	return nil
}

func (s *Session) processPending() error {
	for seq, raw := range s.Pending {
		if seq == s.MaxSeq+1 {
			_, _, md5Bytes, err := protocol.ParseLastPacket(raw)
			if err != nil {
				return err
			}
			s.MD5 = md5Bytes
			s.HasEnd = true
			delete(s.Pending, seq)
			continue
		}

		_, dataSeq, data, err := protocol.ParseDataPacket(raw)
		if err != nil {
			return err
		}

		s.Chunks[dataSeq] = data
		s.Received[dataSeq] = true
		if dataSeq > s.HighestDataSeq {
			s.HighestDataSeq = dataSeq
		}
		delete(s.Pending, seq)
	}

	return nil
}

func (s *Session) ACKBase() uint32 {
	if !s.FirstReceived {
		return 0
	}

	ackBase := uint32(1)
	for ackBase <= s.MaxSeq {
		if !s.Received[ackBase] {
			break
		}
		ackBase++
	}
	return ackBase
}

func (s *Session) MissingSequences(limit int) []uint32 {
	return s.MissingSequencesUpTo(s.repairUpperBound(), limit)
}

func (s *Session) repairUpperBound() uint32 {
	if s.HasEnd {
		return s.MaxSeq
	}
	return s.HighestDataSeq
}

func (s *Session) MissingSequencesUpTo(maxSeen uint32, limit int) []uint32 {
	missing := make([]uint32, 0)
	if !s.FirstReceived {
		return missing
	}

	if maxSeen > s.MaxSeq {
		maxSeen = s.MaxSeq
	}

	for seq := uint32(1); seq <= maxSeen; seq++ {
		if !s.Received[seq] {
			missing = append(missing, seq)
			if limit > 0 && len(missing) >= limit {
				break
			}
		}
	}
	return missing
}

func (s *Session) IsComplete() bool {
	if !s.FirstReceived || !s.HasEnd {
		return false
	}

	for i := uint32(1); i <= s.MaxSeq; i++ {
		if _, ok := s.Chunks[i]; !ok {
			return false
		}
	}
	return true
}

func (s *Session) BuildFile() []byte {
	var result []byte

	for i := uint32(1); i <= s.MaxSeq; i++ {
		result = append(result, s.Chunks[i]...)
	}

	return result
}

func (s *Session) ValidateMD5(fileData []byte) error {
	if len(s.MD5) != 16 {
		return fmt.Errorf("session md5 is invalid")
	}

	sum := md5.Sum(fileData)
	if !bytes.Equal(sum[:], s.MD5) {
		return fmt.Errorf("md5 mismatch")
	}

	return nil
}
