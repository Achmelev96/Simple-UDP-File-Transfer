Start the Receiver

"go run . receiver"

- Starts a UDP server listening on port 9000
- Receives incoming file packets
- Reassembles the file and saves it locally

The receiver expects a folder named testdata in the project root.
All received files will be saved there with the prefix:

received_<original_filename>

## Receiver Flow

![Receiver Flow](docs/receiver-flow.png)


Send a File
"go run . sender file-path"
