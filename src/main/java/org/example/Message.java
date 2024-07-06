package org.example;

public record Message(
        String userName,
        int threadId,
        String message,
        long createdAt
) {
}
