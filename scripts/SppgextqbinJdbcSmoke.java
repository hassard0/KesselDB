// SP-PG-EXTQ-BIN T3 — JDBC smoke for binary-format params.
// Compile: javac -cp /tmp/postgresql-jdbc.jar SppgextqbinJdbcSmoke.java
// Run:     java -cp /tmp/postgresql-jdbc.jar:. SppgextqbinJdbcSmoke

import java.sql.*;

public class SppgextqbinJdbcSmoke {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://127.0.0.1:5532/kesseldb";
        java.util.Properties p = new java.util.Properties();
        p.setProperty("user", "test");
        p.setProperty("password", "admin");
        // Default extended mode — sends binary params.
        try (Connection conn = DriverManager.getConnection(url, p)) {
            System.out.println("JDBC " + conn.getMetaData().getDriverVersion());
            System.out.println("  connect: OK");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE jdbc_bin_smoke (id BIGINT, name CHAR(32))");
                System.out.println("  CREATE TABLE: OK");
            }

            // Literal INSERT (seed).
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO jdbc_bin_smoke (id, name) VALUES (50, 'jdbc')");
                System.out.println("  INSERT (literal): OK");
            }

            // Parameterized INSERT — JDBC sends binary INT8 by default.
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO jdbc_bin_smoke (id, name) VALUES (?, ?)"
            )) {
                ps.setLong(1, 51);
                ps.setString(2, "param");
                ps.executeUpdate();
                System.out.println("  INSERT PreparedStatement (binary INT8 param): OK");
            }

            // Parameterized SELECT — same.
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM jdbc_bin_smoke WHERE id = ?"
            )) {
                ps.setLong(1, 50);
                try (ResultSet rs = ps.executeQuery()) {
                    int rows = 0;
                    while (rs.next()) {
                        long id = rs.getLong(1);
                        System.out.println("  SELECT row: id=" + id);
                        rows++;
                    }
                    System.out.println("  SELECT PreparedStatement (binary INT8 param): OK — " + rows + " rows");
                }
            }
        }
        System.out.println("  === JDBC PASS ===");
    }
}
