# PostgreSQL wire

KesselDB speaks PostgreSQL Frontend/Backend Protocol v3.0 (Simple
Query path + SCRAM-SHA-256). Built behind `--features pg-gateway`.

The operator's Bearer token IS the SCRAM password — one credential
surface; rotating the token rotates HTTP + WS + PG-SCRAM atomically.

Supported clients (each verified via synthetic-peer KATs that replay
the tool's verbatim connect / introspection SQL):

- CLI — `psql`, `pgcli`
- Drivers — `org.postgresql:postgresql` JDBC, psycopg2/psycopg3, `pgx`,
  `tokio-postgres`, `sqlx-pg`
- GUI / BI — pgAdmin 4, DBeaver, DataGrip / IntelliJ, Metabase, Tableau,
  Looker, Hex, Superset
- ORM — Drizzle, Prisma, GORM, Diesel (simple-query mode; Extended
  Query / prepared statements is V2 SP-PG-EXTQ)

Real session capture, the supported `pg_catalog` / `information_schema`
surface (SP-PG-CAT), V1 limitations, and troubleshooting:
[Usage guide (full) §9](full-usage.md#9-postgresql-clients-psql-pgcli-jdbc-psycopg-pgx-).
