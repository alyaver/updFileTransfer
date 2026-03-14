# UDP File Receiver

This project implements a UDP file receiver server in Java.  
The server listens for incoming UDP packets from a client and reconstructs the transmitted file on the receiver side.

## Files

- `receiver.java` – The UDP server that receives packets and writes the file.
- `sender.java` – A simple client used to manually test sending a file.
- `tsender.java` – A testing client that sends multiple connections quickly to test performance and safety.

## How It Works

1. The receiver server starts and waits for UDP packets.
2. A sender client transmits a file in packets.
3. The receiver collects the packets and rebuilds the file.

## How to Run

Compile the programs:

```bash
javac receiver.java
javac sender.java
