package main

import (
	"net"
	"os"
	"path/filepath"
	"time"
)

type AssembleResult struct {
	ID       uint16
	OK       bool
	Duration time.Duration
	Err      error
	Addr     net.Addr
}

// AssembleAndSave Validates the file, compiles it, and saves it
// Once everything is completed successfully, it notifies the channel about it
func AssembleAndSave(session *Session, addr net.Addr, done chan<- AssembleResult) {
	duration := time.Since(session.StartTime)
	fileData := session.BuildFile()

	err := session.ValidateMD5(fileData)
	if err != nil {
		done <- AssembleResult{
			ID:   session.ID,
			OK:   false,
			Err:  err,
			Addr: addr,
		}
		return
	}

	err = SaveReceivedFile(session.FileName, fileData)
	if err != nil {
		done <- AssembleResult{
			ID:   session.ID,
			OK:   false,
			Err:  err,
			Addr: addr,
		}
		return
	}

	done <- AssembleResult{
		ID:       session.ID,
		OK:       true,
		Duration: duration,
		Err:      err,
		Addr:     addr,
	}
}

func SaveReceivedFile(fileName string, data []byte) error {
	baseName := filepath.Base(fileName)
	outputName := filepath.Join("testdata", "rec_"+baseName)
	return os.WriteFile(outputName, data, 0644)
}
