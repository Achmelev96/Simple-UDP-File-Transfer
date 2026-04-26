package main

import (
	"os"
	"path/filepath"
)

type AssembleResult struct {
	ID  uint16
	OK  bool
	Err error
}

// Validates the file, compiles it, and saves it
// Once everything is completed successfully, it notifies the channel about it
func AssembleAndSave(session *Session, done chan<- AssembleResult) {
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
		ID:  session.ID,
		OK:  true,
		Err: err,
	}
}

func SaveReceivedFile(fileName string, data []byte) error {
	baseName := filepath.Base(fileName)
	outputName := filepath.Join("testdata", "received_"+baseName)
	return os.WriteFile(outputName, data, 0644)
}
