"""Generate a sample DuckDB database with representative schemas and data types.

Used for:
  - pyfabric integration testing (data module, SQL queries, schema ops)
  - User reference for trying pyfabric QA/testing features locally

Run standalone:
    python tests/fixtures/create_sample_db.py

Or use via pytest fixture (session-scoped, see conftest.py).

Schemas mirror Fabric lakehouse layout (dbo schema, Delta-style tables).
"""

from pathlib import Path

import duckdb

SAMPLE_DB_PATH = Path(__file__).parent / "sample.duckdb"


def create_sample_db(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Create a sample DuckDB database with representative tables.

    Returns the open connection (caller should close when done).
    """
    db_path = db_path or SAMPLE_DB_PATH
    db_path.unlink(missing_ok=True)

    conn = duckdb.connect(str(db_path))

    # Create dbo schema (mirrors Fabric lakehouse default)
    conn.execute("CREATE SCHEMA IF NOT EXISTS dbo")

    # ── Products table: strings, integers, decimals, booleans ────────────
    conn.execute("""
        CREATE TABLE dbo.products (
            product_id    INTEGER PRIMARY KEY,
            product_name  VARCHAR NOT NULL,
            category      VARCHAR,
            price         DECIMAL(10, 2),
            is_active     BOOLEAN DEFAULT true,
            created_at    TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        INSERT INTO dbo.products VALUES
            (1, 'Widget A',      'Hardware',    19.99,  true,  '2025-01-15 08:30:00'),
            (2, 'Widget B',      'Hardware',    29.99,  true,  '2025-02-20 14:15:00'),
            (3, 'Service Plan',  'Services',   149.00,  true,  '2025-03-01 09:00:00'),
            (4, 'Legacy Item',   'Deprecated',   0.01,  false, '2020-06-15 00:00:00'),
            (5, 'Unicode Test™', 'Special',     99.99,  true,  '2025-04-01 12:00:00')
    """)

    # ── Customers table: strings, NULLs, edge cases ──────────────────────
    conn.execute("""
        CREATE TABLE dbo.customers (
            customer_id   INTEGER PRIMARY KEY,
            full_name     VARCHAR NOT NULL,
            email         VARCHAR,
            phone         VARCHAR,
            region        VARCHAR,
            signup_date   DATE
        )
    """)
    conn.execute("""
        INSERT INTO dbo.customers VALUES
            (100, 'Alice Johnson',  'alice@example.com',  '555-0101', 'West',    '2024-01-10'),
            (101, 'Bob Smith',      'bob@example.com',    NULL,       'East',    '2024-03-22'),
            (102, 'Carol Williams', NULL,                 '555-0103', 'Central', '2024-06-15'),
            (103, 'David Lee',      'david@example.com',  '555-0104', NULL,      '2024-09-01'),
            (104, '',               'empty@example.com',  '',         'West',    '2025-01-01')
    """)

    # ── Orders table: foreign keys, timestamps, large integers ───────────
    conn.execute("""
        CREATE TABLE dbo.orders (
            order_id      BIGINT PRIMARY KEY,
            customer_id   INTEGER,
            product_id    INTEGER,
            quantity      INTEGER NOT NULL,
            total_amount  DECIMAL(12, 2),
            order_date    TIMESTAMP NOT NULL,
            status        VARCHAR DEFAULT 'pending'
        )
    """)
    conn.execute("""
        INSERT INTO dbo.orders VALUES
            (1000000001, 100, 1, 5,   99.95,  '2025-03-15 10:30:00', 'completed'),
            (1000000002, 100, 3, 1,  149.00,  '2025-03-16 11:00:00', 'completed'),
            (1000000003, 101, 2, 10, 299.90,  '2025-03-17 09:45:00', 'shipped'),
            (1000000004, 102, 1, 1,   19.99,  '2025-04-01 14:20:00', 'pending'),
            (1000000005, 103, 5, 2,  199.98,  '2025-04-10 16:00:00', 'cancelled'),
            (1000000006, NULL, 1, 1,   19.99,  '2025-04-11 08:00:00', 'pending')
    """)

    # ── Time series table: timestamps at various granularities ───────────
    conn.execute("""
        CREATE TABLE dbo.sensor_readings (
            sensor_id     VARCHAR NOT NULL,
            reading_time  TIMESTAMP NOT NULL,
            temperature   DOUBLE,
            humidity      DOUBLE,
            is_anomaly    BOOLEAN DEFAULT false
        )
    """)
    conn.execute("""
        INSERT INTO dbo.sensor_readings VALUES
            ('SENS-001', '2025-04-01 00:00:00', 22.5,  45.0,  false),
            ('SENS-001', '2025-04-01 01:00:00', 22.3,  46.2,  false),
            ('SENS-001', '2025-04-01 02:00:00', 98.7,  99.9,  true),
            ('SENS-001', '2025-04-01 03:00:00', 22.1,  44.8,  false),
            ('SENS-002', '2025-04-01 00:00:00', 18.0,  55.0,  false),
            ('SENS-002', '2025-04-01 01:00:00', NULL,   NULL,  false),
            ('SENS-002', '2025-04-01 02:00:00', 17.5,  56.1,  false)
    """)

    # ── Empty table: tests handling of zero-row results ──────────────────
    conn.execute("""
        CREATE TABLE dbo.audit_log (
            log_id        BIGINT,
            event_type    VARCHAR,
            event_data    VARCHAR,
            logged_at     TIMESTAMP
        )
    """)

    # ── Wide table: many columns, various types ──────────────────────────
    conn.execute("""
        CREATE TABLE dbo.wide_record (
            id            INTEGER PRIMARY KEY,
            col_varchar   VARCHAR,
            col_int       INTEGER,
            col_bigint    BIGINT,
            col_double    DOUBLE,
            col_decimal   DECIMAL(18, 6),
            col_bool      BOOLEAN,
            col_date      DATE,
            col_timestamp TIMESTAMP,
            col_blob      BLOB
        )
    """)
    conn.execute("""
        INSERT INTO dbo.wide_record VALUES
            (1, 'text', 42, 9999999999, 3.14159, 123456.789012, true,
             '2025-04-01', '2025-04-01 12:00:00', '\x48454C4C4F'::BLOB),
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """)

    return conn


if __name__ == "__main__":
    conn = create_sample_db()
    # Print summary
    tables = conn.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema = 'dbo' ORDER BY table_name"
    ).fetchall()
    print(f"Created {SAMPLE_DB_PATH} with {len(tables)} tables:")
    for schema, name in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{name}").fetchone()[0]
        print(f"  {schema}.{name}: {count} rows")
    conn.close()
