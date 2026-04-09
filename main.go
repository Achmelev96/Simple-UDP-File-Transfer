package main

import (
	"fmt"
	"os"
)

/*
package structure:

	First {
		2 bytes  - TransmissionID (uint16)
		4 bytes  - SequenceNumber = 0
		4 bytes  - MaxSequenceNumber (uint32)
		N bytes  - FileName
	}

	Data {
		2 bytes  - TransmissionID
		4 bytes  - SequenceNumber = 1..n
		N bytes  - Data
	}

	Last {
		2 bytes  - TransmissionID
		4 bytes  - SequenceNumber = n+1
		16 bytes - MD5
	}
*/

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage:")
		fmt.Println("  go run . receiver")
		fmt.Println("  go run . sender <file-path>")
		return
	}

	mode := os.Args[1]

	switch mode {
	case "receiver":
		err := ReceiveFlow(":9000")
		if err != nil {
			fmt.Println("receiver error:", err)
			return
		}

	case "sender":
		if len(os.Args) < 3 {
			fmt.Println("sender mode requires file path")
			return
		}

		filePath := os.Args[2]
		err := SendFlow(filePath, "10.0.0.103:9000")
		if err != nil {
			fmt.Println("sender error:", err)
			return
		}

	default:
		fmt.Println("unknown mode:", mode)
		fmt.Println("use 'receiver' or 'sender'")
	}
}
