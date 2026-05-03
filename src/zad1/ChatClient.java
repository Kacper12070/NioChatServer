/**
 *
 *  @author Żuchowski Kacper s33521
 *
 */

package zad1;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class ChatClient {

    private final String host;
    private final int port;
    private final String id;

    private SocketChannel channel;

    private final StringBuilder chatView = new StringBuilder();
    private final ByteArrayOutputStream input = new ByteArrayOutputStream();

    public ChatClient(String host, int port, String id) {
        this.host = host;
        this.port = port;
        this.id = id;
    }

    public void login() {
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(host, port));

            while (!channel.finishConnect()) {
                sleepQuietly(1);
            }

            writeLine("LOGIN\t" + id);
            readAvailable(50);

        } catch (Exception e) {
            addException(e);
        }
    }

    public void logout() {
        try {
            if (channel == null || !channel.isOpen()) {
                return;
            }

            writeLine("LOGOUT");
            readUntilClosed(2000);

        } catch (Exception e) {
            addException(e);
        } finally {
            closeQuietly();
        }
    }

    public void send(String req) {
        try {
            if (channel == null || !channel.isOpen()) {
                return;
            }

            writeLine("MSG\t" + req);
            readAvailable(30);

        } catch (Exception e) {
            addException(e);
        }
    }

    public String getChatView() {
        readAvailable(10);

        return "=== " + id + " chat view\n" + chatView;
    }

    private void writeLine(String line) throws IOException {
        ByteBuffer buffer = StandardCharsets.UTF_8.encode(line + "\n");

        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);

            if (written == 0) {
                sleepQuietly(1);
            }
        }
    }

    private void readAvailable(long quietTimeMillis) {
        if (channel == null || !channel.isOpen()) {
            return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long end = System.currentTimeMillis() + quietTimeMillis;

        try {
            while (System.currentTimeMillis() < end) {
                int read = channel.read(buffer);

                if (read > 0) {
                    buffer.flip();
                    processBuffer(buffer);
                    buffer.clear();

                    end = System.currentTimeMillis() + quietTimeMillis;
                } else if (read == 0) {
                    sleepQuietly(1);
                } else {
                    closeQuietly();
                    break;
                }
            }
        } catch (IOException e) {
            addException(e);
            closeQuietly();
        }
    }

    private void readUntilClosed(long maxTimeMillis) {
        if (channel == null || !channel.isOpen()) {
            return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long end = System.currentTimeMillis() + maxTimeMillis;

        try {
            while (System.currentTimeMillis() < end) {
                int read = channel.read(buffer);

                if (read > 0) {
                    buffer.flip();
                    processBuffer(buffer);
                    buffer.clear();

                    end = System.currentTimeMillis() + 100;
                } else if (read == 0) {
                    sleepQuietly(1);
                } else {
                    closeQuietly();
                    break;
                }
            }
        } catch (IOException e) {
            addException(e);
            closeQuietly();
        }
    }

    private void processBuffer(ByteBuffer buffer) {
        while (buffer.hasRemaining()) {
            byte b = buffer.get();

            if (b == '\n') {
                String line = new String(input.toByteArray(), StandardCharsets.UTF_8);
                input.reset();
                chatView.append(line).append("\n");
            } else if (b != '\r') {
                input.write(b);
            }
        }
    }

    private void addException(Exception e) {
        chatView.append("*** ").append(e.toString()).append("\n");
    }

    private void closeQuietly() {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ignored) {
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
