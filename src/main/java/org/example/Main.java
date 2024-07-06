package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.strats.CompletableFutureWriteCoalescer;
import org.example.strats.SingleWrite;
import org.example.strats.Strat;
import org.example.strats.WaitNotifyWriteCoalescer;

import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Main {
    public static final HikariDataSource DB;

    static {
        HikariConfig hikariConfig = new HikariConfig("src/main/resources/db.properties");
        DB = new HikariDataSource(hikariConfig);
    }

    private static final Strat STRAT = Strat.COMPLETABLE_FUTURE;

    public static void main(String[] args) {
        createTable();
        ExecutorService executorService = Executors.newFixedThreadPool(2048);
        SendMessage messageSender = switch (STRAT) {
            case WAIT_NOTIFY -> new WaitNotifyWriteCoalescer();
            case SINGLE_WRITE -> new SingleWrite();
            case COMPLETABLE_FUTURE -> new CompletableFutureWriteCoalescer();
        };

        while (!Thread.currentThread().isInterrupted()) {
            Scanner scanner = new Scanner(System.in);
            int numberToSend = scanner.nextInt();
            System.out.println("msg count: " + numberToSend);

            List<Callable<Long>> list = IntStream.range(0, numberToSend)
                    .mapToObj(i -> (Callable<Long>) () -> {
                        long start = System.nanoTime();
                        messageSender.sendMessage(new Message(
                                "name",
                                1,
                                "msg",
                                1L
                        ));
                        long stop = System.nanoTime();
                        return TimeUnit.NANOSECONDS.toMillis(stop - start);
                    }).toList();

            try {
                List<Future<Long>> futures = executorService.invokeAll(list);
                long totalDuration = 0;

                for (Future<Long> future : futures) {
                    try {
                        totalDuration += future.get();
                    } catch (ExecutionException e) {
                        exit(e);
                    }
                }
                var avgMs = totalDuration / numberToSend;


                System.out.println(numberToSend + " requests, avg: " + avgMs + " ms");
            } catch (InterruptedException e) {
                exit(e);
            }
        }
    }

    private static void createTable() {
        try (var con = DB.getConnection();
             var stmt = con.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS messages");
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_name VARCHAR(128) NOT NULL,
                        thread_id INT NOT NULL,
                        message VARCHAR(128) NOT NULL,
                        created_at BIGINT NOT NULL,
                        INDEX idx_thread_id (thread_id)
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static RuntimeException exit(Exception e) {
        System.out.println(e.getMessage());
        System.exit(1);
        return new RuntimeException(e);
    }
}