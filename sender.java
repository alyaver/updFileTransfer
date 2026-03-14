import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/**
 * UDP file sender with configurable loss and reorder simulation.
 *
 * Usage:
 *   java sender.java [-v] <host> <port> <file> <loss%> <reorder%>
 *
 * The sender splits the file into 1000-byte chunks, base64-encodes each,
 * and sends them as UDP datagrams using a stop-and-wait protocol.
 *
 * Loss simulation: each outgoing DATA packet has a <loss%> chance of being
 * silently dropped (not sent).
 *
 * Reorder simulation: each outgoing DATA packet has a <reorder%> chance of
 * being delayed and sent after the next packet instead.
 */
public class sender {

    private static final int CHUNK_SIZE = 1000;
    private static final int ACK_TIMEOUT_MS = 2000;
    private static final int MAX_RETRIES = 20;

    private final DatagramSocket socket;
    private final InetAddress destAddr;
    private final int destPort;
    private final boolean verbose;
    private final Random rng = new Random();

    private final String transferId;
    private final String filename;
    private final String checksum;
    private final List<byte[]> chunks;

    private final int lossPct;
    private final int reorderPct;

    public sender(String host, int port, String filePath,
                  int lossPct, int reorderPct, boolean verbose) throws Exception {
        this.destAddr = InetAddress.getByName(host);
        this.destPort = port;
        this.lossPct = lossPct;
        this.reorderPct = reorderPct;
        this.verbose = verbose;

        this.socket = new DatagramSocket();
        this.socket.setSoTimeout(ACK_TIMEOUT_MS);

        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        this.filename = Paths.get(filePath).getFileName().toString();
        this.checksum = md5Hex(fileBytes);
        this.transferId = "xfer-" + UUID.randomUUID().toString().substring(0, 8);

        this.chunks = new ArrayList<>();
        for (int off = 0; off < fileBytes.length; off += CHUNK_SIZE) {
            int end = Math.min(off + CHUNK_SIZE, fileBytes.length);
            this.chunks.add(Arrays.copyOfRange(fileBytes, off, end));
        }
    }

    public void run() throws Exception {
        log("Transfer %s: %s (%d chunks, md5=%s)",
            transferId, filename, chunks.size(), checksum);
        log("Loss=%d%%, Reorder=%d%%", lossPct, reorderPct);

        sendBegin();
        sendAllChunks();
        sendEnd();

        log("Transfer %s complete.", transferId);
        socket.close();
    }

    // --- Protocol phases ---

    private void sendBegin() throws Exception {
        String msg = String.format("BEGIN %s %s %d %s",
            transferId, filename, chunks.size(), checksum);
        sendAndWaitAck(msg, "BEGIN");
    }

    private void sendAllChunks() throws Exception {
        String delayedMsg = null;
        int delayedSeq = -1;

        for (int seq = 0; seq < chunks.size(); seq++) {
            String b64 = Base64.getEncoder().encodeToString(chunks.get(seq));
            String msg = String.format("DATA %s %d %s", transferId, seq, b64);

            // Flush any previously delayed packet first
            if (delayedMsg != null) {
                vlog("  [reorder] sending delayed seq=%d", delayedSeq);
                sendAndWaitAck(delayedMsg, String.valueOf(delayedSeq));
                delayedMsg = null;
                delayedSeq = -1;
            }

            // Reorder simulation: hold this packet, send it next iteration
            if (rng.nextInt(100) < reorderPct && seq < chunks.size() - 1) {
                vlog("  [reorder] delaying seq=%d", seq);
                delayedMsg = msg;
                delayedSeq = seq;
                continue;
            }

            // Loss simulation: skip sending (receiver won't ACK, so we retry)
            if (rng.nextInt(100) < lossPct) {
                vlog("  [loss] dropping seq=%d", seq);
            }

            sendAndWaitAck(msg, String.valueOf(seq));
        }

        // Flush any remaining delayed packet
        if (delayedMsg != null) {
            sendAndWaitAck(delayedMsg, String.valueOf(delayedSeq));
        }
    }

    private void sendEnd() throws Exception {
        sendAndWaitAck(String.format("END %s", transferId), "END");
    }

    // --- Stop-and-wait with retransmission ---

    private void sendAndWaitAck(String message, String expectedAckTag)
            throws Exception {
        byte[] msgBytes = message.getBytes();
        DatagramPacket outPkt = new DatagramPacket(
            msgBytes, msgBytes.length, destAddr, destPort);

        String expectedReply = String.format("ACK %s %s", transferId, expectedAckTag);
        byte[] buf = new byte[1024];

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            socket.send(outPkt);
            vlog("  sent %s (attempt %d)", expectedAckTag, attempt);

            try {
                DatagramPacket inPkt = new DatagramPacket(buf, buf.length);
                socket.receive(inPkt);
                String reply = new String(
                    inPkt.getData(), 0, inPkt.getLength()).trim();

                if (reply.equals(expectedReply)) {
                    vlog("  got %s", reply);
                    return;
                }

                vlog("  unexpected reply: %s (wanted: %s)", reply, expectedReply);
            } catch (SocketTimeoutException e) {
                vlog("  timeout waiting for %s", expectedReply);
            }
        }

        System.err.printf("ERROR: gave up after %d retries for %s %s%n",
            MAX_RETRIES, transferId, expectedAckTag);
    }

    // --- Helpers ---

    private static String md5Hex(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private void log(String fmt, Object... args) {
        System.err.printf("[sender] " + fmt + "%n", args);
    }

    private void vlog(String fmt, Object... args) {
        if (verbose) System.err.printf("[sender] " + fmt + "%n", args);
    }

    // --- Entry point ---

    public static void main(String[] args) throws Exception {
        int ai = 0;
        boolean verbose = false;
        if (args.length > 0 && args[0].equals("-v")) {
            verbose = true;
            ai = 1;
        }

        if (args.length - ai != 5) {
            System.err.println(
                "Usage: java sender.java [-v] <host> <port> <file> <loss%> <reorder%>");
            System.exit(1);
        }

        String host = args[ai];
        int port = Integer.parseInt(args[ai + 1]);
        String filePath = args[ai + 2];
        int lossPct = Integer.parseInt(args[ai + 3]);
        int reorderPct = Integer.parseInt(args[ai + 4]);

        sender s = new sender(host, port, filePath, lossPct, reorderPct, verbose);
        s.run();
    }
}
