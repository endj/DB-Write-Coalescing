package org.example.strats;

import org.example.Message;
import org.example.SendMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.example.Main.DB;
import static org.example.Main.exit;

public class WaitNotifyWriteCoalescer implements SendMessage {
    private static final String SQL = "INSERT INTO messages (user_name, thread_id, message, created_at) " +
                                      "VALUES (?, ?, ?, ?)";

    private final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8128);

    public WaitNotifyWriteCoalescer() {
        var executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {

            while (!Thread.currentThread().isInterrupted()) {
                long start = System.nanoTime();
                int msg = processQueueAndNotifyWriters();
                long stop = System.nanoTime();
                long duration = stop - start;
                long durationMs = TimeUnit.NANOSECONDS.toMillis(duration);
                long timeLeft = 300 - durationMs;

                if (timeLeft > 0) {
                    try {
                        Thread.sleep(timeLeft);
                    } catch (InterruptedException e) {
                        throw exit(e);
                    }
                }

                if (msg > 0) {
                    System.out.println("Writer: " + durationMs + "ms " + msg);
                }
            }

        });
    }

    public Message sendMessage(Message msg) {
        if (!queue.offer(msg)) {
            return null;
        }
        synchronized (msg) {
            try {
                msg.wait();
            } catch (InterruptedException e) {
                exit(e);
            }
        }
        return msg;
    }

    public int processQueueAndNotifyWriters() {
        List<Message> batch = new ArrayList<>();
        queue.drainTo(batch);
        if (batch.isEmpty())
            return 0;
        batchWrite(batch);
        for (Message message : batch) {
            synchronized (message) {
                message.notify();
            }
        }
        return batch.size();
    }

    private void batchWrite(List<Message> messages) {

        try (var connection = DB.getConnection();
             var stmt = connection.prepareStatement(SQL)) {

            connection.setAutoCommit(false);

            for (Message msg : messages) {
                stmt.setString(1, msg.userName());
                stmt.setInt(2, msg.threadId());
                stmt.setString(3, msg.message());
                stmt.setLong(4, msg.createdAt());
                stmt.addBatch();
            }

            stmt.executeBatch();
            connection.commit();
        } catch (Exception e) {
            throw exit(e);
        }
    }
}
