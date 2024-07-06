package org.example.strats;

import org.example.Message;
import org.example.SendMessage;

import static org.example.Main.DB;
import static org.example.Main.exit;

public class SingleWrite implements SendMessage {
    private static final String sql = "INSERT INTO messages (user_name, thread_id, message, created_at) " +
                                      "VALUES (?, ?, ?, ?)";

    @Override
    public Message sendMessage(Message msg) {

        try (var connection = DB.getConnection();
             var stmt = connection.prepareStatement(sql)) {

            stmt.setString(1, msg.userName());
            stmt.setInt(2, msg.threadId());
            stmt.setString(3, msg.message());
            stmt.setLong(4, msg.createdAt());

            int rowsInserted = stmt.executeUpdate();
        } catch (Exception e) {
            throw exit(e);
        }
        return msg;
    }
}
