package org.example.strats;

import org.example.Message;
import org.example.SendMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.example.Main.DB;
import static org.example.Main.exit;

public class CompletableFutureWriteCoalescer implements SendMessage {
    private static final String SQL = "INSERT INTO messages (user_name, thread_id, message, created_at) " +
                                      "VALUES (?, ?, ?, ?)";

    record MsgFuture(Message msg, CompletableFuture<Message> future) {
    }

    Queue<MsgFuture> msgQueue = new ConcurrentLinkedQueue<>();

    public CompletableFutureWriteCoalescer() {
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

    public int processQueueAndNotifyWriters() {
        int max = 4000;
        int taken = 0;
        List<MsgFuture> batch = new ArrayList<>();

        MsgFuture msg;
        while ((msg = msgQueue.poll()) != null && taken++ < max) {
            batch.add(msg);
        }
        if (batch.isEmpty())
            return 0;
        batchWrite(batch);
        for (MsgFuture message : batch) {
            message.future.complete(message.msg);
        }
        return batch.size();
    }

    private void batchWrite(List<MsgFuture> messages) {

        try (var connection = DB.getConnection();
             var stmt = connection.prepareStatement(SQL)) {

            connection.setAutoCommit(false);

            for (MsgFuture msgf : messages) {
                Message msg = msgf.msg;
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

    @Override
    public Message sendMessage(Message msg) {

        var msgResponse = new CompletableFuture<Message>();
        msgQueue.add(new MsgFuture(msg, msgResponse));

        try {
            return msgResponse.get();
        } catch (Exception e) {
            throw exit(e);
        }
    }
}
