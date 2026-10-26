import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Local connection adapter for the original professor's Blocking.java. */
public final class ConnectionFactory {
    public static final int POSTGRESQL = 1;
    private ConnectionFactory() {}
    public static Connection getDefaultParameterConnection(int database) {
        if (database != POSTGRESQL) throw new IllegalArgumentException("Only PostgreSQL supported");
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/adb_hw2");
        String username = System.getenv().getOrDefault("DB_USER", "postgres");
        String password = System.getenv("DB_PASSWORD");
        if (password == null) throw new IllegalStateException("Set DB_PASSWORD in IntelliJ environment variables");
        try { return DriverManager.getConnection(url, username, password); }
        catch (SQLException e) { throw new IllegalStateException("Could not connect to PostgreSQL", e); }
    }
}
