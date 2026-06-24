package main

import (
	"Simple_UDP_File_Transfer/protocol"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type transferDirection string

const (
	directionSent     transferDirection = "sent"
	directionReceived transferDirection = "received"
)

type packetStats struct {
	FirstPackets int
	LastPackets  int
	DataPackets  int
	TotalPackets int
	TotalBytes   int
	DataSizes    map[int]int
}

type controlStats struct {
	ACK      int
	NAK      int
	Complete int
	Other    int
}

type sessionLog struct {
	ID            uint16
	FileName      string
	MaxSeq        uint32
	Start         time.Time
	End           time.Time
	WindowEnabled *bool
	Sent          packetStats
	Received      packetStats
	ControlSent   controlStats
	ControlRecv   controlStats
}

type TransferLogger struct {
	mu       sync.Mutex
	sessions map[uint16]*sessionLog
}

func NewTransferLogger() *TransferLogger {
	return &TransferLogger{sessions: make(map[uint16]*sessionLog)}
}

func (l *TransferLogger) LogDataPacket(direction transferDirection, packet []byte) {
	id, err := protocol.GetTransmissionID(packet)
	if err != nil {
		return
	}
	seq, err := protocol.GetSequenceNumber(packet)
	if err != nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	s := l.session(id)
	stats := &s.Sent
	if direction == directionReceived {
		stats = &s.Received
	}
	ensureDataSizes(stats)

	stats.TotalPackets++
	stats.TotalBytes += len(packet)

	switch {
	case seq == 0:
		stats.FirstPackets++
		_, maxSeq, fileName, err := protocol.ParseFirstPacket(packet)
		if err == nil {
			s.MaxSeq = maxSeq
			s.FileName = fileName
		}
	case s.MaxSeq > 0 && seq == s.MaxSeq+1:
		stats.LastPackets++
	default:
		stats.DataPackets++
		stats.DataSizes[len(packet)]++
	}
}

func (l *TransferLogger) LogControlPacket(direction transferDirection, control *protocol.ControlPacket) {
	if control == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	s := l.session(control.ID)
	stats := &s.ControlSent
	if direction == directionReceived {
		stats = &s.ControlRecv
	}

	switch control.Type {
	case protocol.ControlTypeACK:
		stats.ACK++
	case protocol.ControlTypeNAK:
		stats.NAK++
	case protocol.ControlTypeComplete:
		stats.Complete++
	default:
		stats.Other++
	}
}

func (l *TransferLogger) LogControlBytes(direction transferDirection, packet []byte) {
	control, err := protocol.ParseControlPacket(packet)
	if err != nil {
		return
	}
	l.LogControlPacket(direction, control)
}

func (l *TransferLogger) SetWindowEnabled(id uint16, enabled bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	s := l.session(id)
	s.WindowEnabled = &enabled
}

func (l *TransferLogger) PrintSendSummary(id uint16, duration time.Duration) {
	l.printSession(id, duration, func(s sessionLog, duration time.Duration) {
		fmt.Println()
		printSessionHeader(s)
		if s.WindowEnabled != nil {
			fmt.Println("sliding window:", yesNo(*s.WindowEnabled))
		}
		fmt.Println("data packets sent:", formatPacketStats(s.Sent))
		fmt.Println("control received:", formatControlStats(s.ControlRecv))
		printDuration(duration)
		fmt.Println()
	})
}

func (l *TransferLogger) PrintReceiveSummary(id uint16, duration time.Duration) {
	l.printSession(id, duration, func(s sessionLog, duration time.Duration) {
		fmt.Println()
		printSessionHeader(s)
		fmt.Println("data packets received:", formatPacketStats(s.Received))
		fmt.Println("control sent:", formatControlStats(s.ControlSent))
		printDuration(duration)
		fmt.Println()
	})
}

func (l *TransferLogger) printSession(id uint16, duration time.Duration, printer func(sessionLog, time.Duration)) {
	l.mu.Lock()
	s, ok := l.sessions[id]
	if !ok {
		l.mu.Unlock()
		return
	}
	if duration <= 0 {
		duration = time.Since(s.Start)
	}
	if !s.End.IsZero() {
		duration = s.End.Sub(s.Start)
	}
	snapshot := *s
	l.mu.Unlock()

	printer(snapshot, duration)
}

func (l *TransferLogger) session(id uint16) *sessionLog {
	s, ok := l.sessions[id]
	if !ok {
		s = &sessionLog{ID: id, Start: time.Now()}
		ensureDataSizes(&s.Sent)
		ensureDataSizes(&s.Received)
		l.sessions[id] = s
	}
	return s
}

func ensureDataSizes(stats *packetStats) {
	if stats.DataSizes == nil {
		stats.DataSizes = make(map[int]int)
	}
}

func printSessionHeader(s sessionLog) {
	fmt.Println("session id:", s.ID)
	if s.FileName != "" {
		fmt.Println("file:", s.FileName)
	}
}

func printDuration(duration time.Duration) {
	if duration > 0 {
		fmt.Println("duration:", duration)
	}
}

func formatPacketStats(stats packetStats) string {
	parts := []string{
		fmt.Sprintf("total=%d", stats.TotalPackets),
		fmt.Sprintf("bytes=%d", stats.TotalBytes),
		fmt.Sprintf("first=%d", stats.FirstPackets),
		fmt.Sprintf("data=%d", stats.DataPackets),
		fmt.Sprintf("last=%d", stats.LastPackets),
	}
	if len(stats.DataSizes) > 0 {
		parts = append(parts, "data packet sizes="+formatSizeCounts(stats.DataSizes))
	}
	return strings.Join(parts, ", ")
}

func formatSizeCounts(counts map[int]int) string {
	sizes := make([]int, 0, len(counts))
	for size := range counts {
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)

	parts := make([]string, 0, len(sizes))
	for _, size := range sizes {
		parts = append(parts, fmt.Sprintf("%dB x%d", size, counts[size]))
	}
	return strings.Join(parts, "; ")
}

func formatControlStats(stats controlStats) string {
	return fmt.Sprintf("ack=%d, nak=%d, complete=%d, other=%d", stats.ACK, stats.NAK, stats.Complete, stats.Other)
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
