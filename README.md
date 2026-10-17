# UDP File Receiver

This project implements a UDP file receiver server in Java.  
The server listens for incoming UDP packets from a client and reconstructs the transmitted file on the receiver side.

## Files

- `src/receiver.java` – The UDP server that receives packets and writes the file.
- `src/sender.java` – A simple client used to manually test sending a file.
- `data/testfile.txt` – Sample file to transfer.
- `test/tsender.java` – A testing client that sends multiple connections quickly to test performance and safety.

## How It Works

1. The receiver server starts and waits for UDP packets.
2. A sender client transmits a file in packets.
3. The receiver collects the packets and rebuilds the file.

## How to Run

Compile the programs:

```bash
javac -d out src/receiver.java src/sender.java test/tsender.java
```

Or run directly from source (no compile step):

```bash
java src/receiver.java [-v] <port>
java src/sender.java [-v] <host> <port> data/testfile.txt <loss%> <reorder%> <dup%>
java test/tsender.java <host> <port> <loss%> <reorder%> <dup%> auto
```
