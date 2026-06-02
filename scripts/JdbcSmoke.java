// SP-PG-JDBC-SMOKE T1 — real pgJDBC end-to-end smoke against KesselDB.
//
// Compile (vulcan):
//   ~/jdbc-smoke/jdk-21.0.2/bin/javac -cp ~/jdbc-smoke/postgresql.jar JdbcSmoke.java
// Run (simple-mode):
//   ~/jdbc-smoke/jdk-21.0.2/bin/java -cp .:~/jdbc-smoke/postgresql.jar JdbcSmoke simple
// Run (extended-mode):
//   ~/jdbc-smoke/jdk-21.0.2/bin/java -cp .:~/jdbc-smoke/postgresql.jar JdbcSmoke extended
//
// Modes:
//   simple   -> ?preferQueryMode=simple ; exercises the SP-PG-EXTQ-CAST
//               cast-stripper path (PreparedStatement substitutes literals
//               client-side AND tags them with `::int8` / `::text` which
//               must be stripped before the kessel-sql lexer sees them).
//   extended -> default JDBC mode ; exercises the Parse/Bind/Describe/
//               Execute/Sync flow with binary INT8 params (SP-PG-EXTQ-BIN)
//               and binary INT8 result columns (SP-PG-EXTQ-BIN-RESULTS).
//
// All assertions are inline; the program prints "ALL TESTS PASS" on success
// and throws on any unexpected row count or value.

import java.sql.*;

public class JdbcSmoke {
    public static void main(String[] args) throws Exception {
        String mode = (args.length > 0) ? args[0] : "simple";
        String url = "jdbc:postgresql://127.0.0.1:5532/kesseldb";
        if ("simple".equals(mode)) {
            url += "?preferQueryMode=simple";
        }
        String user = "test";
        String password = "admin";

        System.out.println("JDBC smoke — mode=" + mode + " url=" + url);

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected. driver=" + conn.getMetaData().getDriverVersion());

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE jdbc_smoke (id BIGINT, name CHAR(32))");
                System.out.println("CREATE TABLE: OK");
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO jdbc_smoke (id, name) VALUES (?, ?)")) {
                ps.setLong(1, 42);
                ps.setString(2, "hello-jdbc");
                int rows = ps.executeUpdate();
                System.out.println("INSERT: " + rows + " row(s)");
                if (rows != 1) {
                    throw new AssertionError("expected INSERT to affect 1 row, got " + rows);
                }
            }

            int seenAll = 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM jdbc_smoke")) {
                while (rs.next()) {
                    long id = rs.getLong(1);
                    String name = rs.getString(2);
                    System.out.println("Row: id=" + id + ", name=" + name);
                    seenAll++;
                }
            }
            if (seenAll < 1) {
                throw new AssertionError("expected SELECT * to return at least 1 row, got " + seenAll);
            }

            int seenParam = 0;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT * FROM jdbc_smoke WHERE id = ?")) {
                ps.setLong(1, 42);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong(1);
                        String name = rs.getString(2);
                        System.out.println("Param SELECT: id=" + id + ", name=" + name);
                        if (id != 42) {
                            throw new AssertionError("expected id=42, got " + id);
                        }
                        seenParam++;
                    }
                }
            }
            if (seenParam != 1) {
                throw new AssertionError("expected param SELECT to return 1 row, got " + seenParam);
            }

            // version() probe — canonical fingerprint every libpq client emits.
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT version()")) {
                if (rs.next()) {
                    System.out.println("Server version: " + rs.getString(1));
                } else {
                    throw new AssertionError("SELECT version() returned 0 rows");
                }
            }

            System.out.println("ALL TESTS PASS");
        }
    }
}
