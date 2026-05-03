/**
 *
 * @author Żuchowski Kacper s33521
 *
 */

package zad1;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class ChatServer {

    private final String host;
    private final int port;

    private volatile boolean running;
    private Thread serverThread;
    private Selector selector;
    private ServerSocketChannel serverChannel;

    private final StringBuilder serverLog = new StringBuilder();

    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public ChatServer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void startServer() {
        try {
            selector = Selector.open();

            serverChannel = ServerSocketChannel.open();
            serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverChannel.configureBlocking(false);
            serverChannel.bind(new InetSocketAddress(host, port));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            running = true;

            serverThread = new Thread(this::serverLoop, "ChatServerThread");
            serverThread.setDaemon(false);
            serverThread.start();

            System.out.println("Server started");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stopServer() {
        running = false;

        if (selector != null) {
            selector.wakeup();
        }

        try {
            if (serverThread != null) {
                serverThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("Server stopped");
    }

    private void serverLoop() {
        try {
            while (running) {
                selector.select(500);

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    try {
                        if (key.isAcceptable()) {
                            acceptClient();
                        } else if (key.isReadable()) {
                            readFromClient(key);
                        } else if (key.isWritable()) {
                            writeToClient(key);
                        }
                    } catch (IOException e) {
                        disconnectClient(key, (SocketChannel) key.channel());
                    }
                }
            }
        } catch (IOException e) {
            if (running) {
                e.printStackTrace();
            }
        } finally {
            closeAll();
        }
    }

    private void acceptClient() throws IOException {
        SocketChannel clientChannel;

        while ((clientChannel = serverChannel.accept()) != null) {
            clientChannel.configureBlocking(false);

            SelectionKey key = clientChannel.register(selector, SelectionKey.OP_READ);
            key.attach(new ClientState(key));
        }
    }

    private void readFromClient(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientState state = (ClientState) key.attachment();

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        int read;

        while ((read = channel.read(buffer)) > 0) {
            buffer.flip();

            while (buffer.hasRemaining()) {
                byte b = buffer.get();

                if (b == '\n') {
                    String request = state.takeLine();
                    processRequest(key, channel, state, request);
                } else if (b != '\r') {
                    state.lineBuffer.write(b);
                }
            }

            buffer.clear();
        }

        if (read == -1) {
            disconnectClient(key, channel);
        }
    }

    private void writeToClient(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientState state = (ClientState) key.attachment();

        while (!state.outgoing.isEmpty()) {
            ByteBuffer buffer = state.outgoing.peek();
            channel.write(buffer);

            if (buffer.hasRemaining()) {
                break;
            }

            state.outgoing.poll();
        }

        if (state.outgoing.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);

            if (state.closeAfterWrite) {
                disconnectClient(key, channel);
            }
        }
    }

    private void processRequest(SelectionKey key, SocketChannel sender,
                                ClientState state, String request) {
        if (request == null || request.isEmpty()) {
            return;
        }

        String[] parts = request.split("\t", 2);
        String command = parts[0];

        switch (command) {
            case "LOGIN" -> {
                if (parts.length < 2) {
                    return;
                }

                state.id = parts[1];
                state.loggedIn = true;

                String message = state.id + " logged in";
                addToServerLog(message);
                broadcast(message);
            }

            case "MSG" -> {
                if (!state.loggedIn || parts.length < 2) {
                    return;
                }

                String message = state.id + ": " + parts[1];
                addToServerLog(message);
                broadcast(message);
            }

            case "LOGOUT" -> {
                if (!state.loggedIn) {
                    return;
                }

                String message = state.id + " logged out";
                addToServerLog(message);
                broadcast(message);

                state.loggedIn = false;
                state.closeAfterWrite = true;

                enableWrite(key);
            }
        }
    }

    private void broadcast(String message) {
        byte[] data = (message + "\n").getBytes(StandardCharsets.UTF_8);

        for (SelectionKey key : selector.keys()) {
            if (!key.isValid()) {
                continue;
            }

            Object attachment = key.attachment();

            if (!(attachment instanceof ClientState state)) {
                continue;
            }

            if (!state.loggedIn && !state.closeAfterWrite) {
                continue;
            }

            state.outgoing.add(ByteBuffer.wrap(data));
            enableWrite(key);
        }
    }

    private void enableWrite(SelectionKey key) {
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        }
    }

    private void disconnectClient(SelectionKey key, SocketChannel channel) {
        ClientState state = (ClientState) key.attachment();

        if (state != null && state.loggedIn) {
            state.loggedIn = false;

            String message = state.id + " logged out";
            addToServerLog(message);
            broadcast(message);
        }

        key.cancel();

        try {
            channel.close();
        } catch (IOException ignored) {
        }
    }

    private void addToServerLog(String message) {
        String time = LocalTime.now().format(TIME_FORMAT);
        serverLog.append(time).append(" ").append(message).append("\n");
    }

    public String getServerLog() {
        String log = serverLog.toString();

        if (log.endsWith("\n")) {
            log = log.substring(0, log.length() - 1);
        }

        return log;
    }

    private void closeAll() {
        if (selector != null) {
            for (SelectionKey key : new ArrayList<>(selector.keys())) {
                try {
                    key.channel().close();
                } catch (IOException ignored) {
                }
            }

            try {
                selector.close();
            } catch (IOException ignored) {
            }
        }

        if (serverChannel != null) {
            try {
                serverChannel.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static class ClientState {
        final SelectionKey key;

        String id;
        boolean loggedIn = false;
        boolean closeAfterWrite = false;

        final Queue<ByteBuffer> outgoing = new ArrayDeque<>();
        final ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();

        ClientState(SelectionKey key) {
            this.key = key;
        }

        String takeLine() {
            String line = lineBuffer.toString(StandardCharsets.UTF_8);
            lineBuffer.reset();
            return line;
        }
    }
}