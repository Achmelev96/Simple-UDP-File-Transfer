package main

import (
	"os"
	"path/filepath"
	"time"
)

type AssembleResult struct {
	ID       uint16
	OK       bool
	Duration time.Duration
	Err      error
}

// AssembleAndSave Validates the file, compiles it, and saves it
// Once everything is completed successfully, it notifies the channel about it
func AssembleAndSave(session *Session, done chan<- AssembleResult) {
	duration := time.Since(session.StartTime)
	fileData := session.BuildFile()

	err := session.ValidateMD5(fileData)
	if err != nil {
		done <- AssembleResult{
			ID:  session.ID,
			OK:  false,
			Err: err,
		}
		return
	}

	err = SaveReceivedFile(session.FileName, fileData)
	if err != nil {
		done <- AssembleResult{
			ID:  session.ID,
			OK:  false,
			Err: err,
		}
		return
	}

	done <- AssembleResult{
		ID:       session.ID,
		OK:       true,
		Duration: duration,
		Err:      err,
	}
}

func SaveReceivedFile(fileName string, data []byte) error {
	baseName := filepath.Base(fileName)
	outputName := filepath.Join("testdata", "rec_"+baseName)
	return os.WriteFile(outputName, data, 0644)
}
