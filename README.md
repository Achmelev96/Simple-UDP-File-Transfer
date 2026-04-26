## Start the Receiver

"go run . receiver"

- Starts a UDP server listening on port 9000
- Receives incoming file packets
- Reassembles the file and saves it locally

The receiver expects a folder named testdata in the project root.
All received files will be saved there with the prefix:

received_<original_filename>

## Receiver Flow

The receiver processes incoming UDP packets and distributes them into sessions based on their transmission ID.  
Each session collects packets until the file is complete.  

Once a session is complete, it is passed to a separate worker (goroutine),  
which rebuilds the file, validates its integrity (MD5), and saves it to disk.  

Meanwhile, the receiver continues processing incoming packets without blocking.

![Receiver Flow](docs/receiver-flow.png)


Send a File
"go run . sender file-path"
