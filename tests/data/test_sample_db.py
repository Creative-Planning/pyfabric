"""Integration tests using the sample DuckDB database.

Validates that the sample database has the expected schema, data types,
and edge cases that pyfabric's QA features need to handle.
"""


class TestSampleDbSchema:
    def test_has_dbo_schema(self, sample_db):
        schemas = sample_db.execute(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()
        assert ("dbo",) in schemas

    def test_has_all_tables(self, sample_db):
        tables = sample_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'dbo' ORDER BY table_name"
        ).fetchall()
        names = [t[0] for t in tables]
        assert "products" in names
        assert "customers" in names
        assert "orders" in names
        assert "sensor_readings" in names
        assert "audit_log" in names
        assert "wide_record" in names


class TestProductsTable:
    def test_row_count(self, sample_db):
        count = sample_db.execute("SELECT COUNT(*) FROM dbo.products").fetchone()[0]
        assert count == 5

    def test_data_types(self, sample_db):
        row = sample_db.execute(
            "SELECT product_id, product_name, price, is_active, created_at "
            "FROM dbo.products WHERE product_id = 1"
        ).fetchone()
        assert isinstance(row[0], int)  # product_id
        assert isinstance(row[1], str)  # product_name
        assert row[2] is not None  # price (Decimal)
        assert isinstance(row[3], bool)  # is_active

    def test_boolean_values(self, sample_db):
        active = sample_db.execute(
            "SELECT COUNT(*) FROM dbo.products WHERE is_active = true"
        ).fetchone()[0]
        inactive = sample_db.execute(
            "SELECT COUNT(*) FROM dbo.products WHERE is_active = false"
        ).fetchone()[0]
        assert active == 4
        assert inactive == 1

    def test_unicode_data(self, sample_db):
        name = sample_db.execute(
            "SELECT product_name FROM dbo.products WHERE product_id = 5"
        ).fetchone()[0]
        assert "™" in name


class TestCustomersTable:
    def test_null_handling(self, sample_db):
        """Verify NULLs in different columns are preserved."""
        nulls = sample_db.execute(
            "SELECT customer_id, email, phone, region FROM dbo.customers "
            "WHERE email IS NULL OR phone IS NULL OR region IS NULL"
        ).fetchall()
        assert len(nulls) >= 2  # Bob (no phone), Carol (no email), David (no region)

    def test_empty_string_vs_null(self, sample_db):
        """Empty string and NULL are different — QA should detect both."""
        row = sample_db.execute(
            "SELECT full_name, phone FROM dbo.customers WHERE customer_id = 104"
        ).fetchone()
        assert row[0] == ""  # empty string, not NULL
        assert row[1] == ""  # empty string, not NULL


class TestOrdersTable:
    def test_large_ids(self, sample_db):
        """BIGINT order IDs are preserved."""
        max_id = sample_db.execute("SELECT MAX(order_id) FROM dbo.orders").fetchone()[0]
        assert max_id > 1_000_000_000

    def test_null_foreign_key(self, sample_db):
        """Orders with NULL customer_id (orphaned records)."""
        orphans = sample_db.execute(
            "SELECT COUNT(*) FROM dbo.orders WHERE customer_id IS NULL"
        ).fetchone()[0]
        assert orphans == 1

    def test_join_with_customers(self, sample_db):
        """JOIN produces expected results."""
        rows = sample_db.execute(
            "SELECT c.full_name, o.total_amount "
            "FROM dbo.orders o JOIN dbo.customers c ON o.customer_id = c.customer_id "
            "WHERE o.status = 'completed'"
        ).fetchall()
        assert len(rows) == 2


class TestSensorReadings:
    def test_time_series_data(self, sample_db):
        count = sample_db.execute(
            "SELECT COUNT(*) FROM dbo.sensor_readings"
        ).fetchone()[0]
        assert count == 7

    def test_anomaly_detection(self, sample_db):
        """Anomalous readings are flagged."""
        anomalies = sample_db.execute(
            "SELECT temperature FROM dbo.sensor_readings WHERE is_anomaly = true"
        ).fetchall()
        assert len(anomalies) == 1
        assert anomalies[0][0] > 90  # spike

    def test_null_readings(self, sample_db):
        """NULL sensor values (missing data)."""
        nulls = sample_db.execute(
            "SELECT COUNT(*) FROM dbo.sensor_readings WHERE temperature IS NULL"
        ).fetchone()[0]
        assert nulls == 1


class TestEdgeCases:
    def test_empty_table(self, sample_db):
        """audit_log has zero rows — queries should handle gracefully."""
        count = sample_db.execute("SELECT COUNT(*) FROM dbo.audit_log").fetchone()[0]
        assert count == 0

    def test_wide_table_all_types(self, sample_db):
        """Wide record covers every common data type."""
        row = sample_db.execute("SELECT * FROM dbo.wide_record WHERE id = 1").fetchone()
        assert row is not None
        assert len(row) == 10  # 10 columns

    def test_wide_table_all_nulls(self, sample_db):
        """Row with all nullable columns NULL."""
        row = sample_db.execute("SELECT * FROM dbo.wide_record WHERE id = 2").fetchone()
        # All columns except id should be None
        assert row[0] == 2
        assert all(v is None for v in row[1:])

    def test_aggregate_queries(self, sample_db):
        """Standard aggregation patterns work."""
        result = sample_db.execute(
            "SELECT category, COUNT(*) as cnt, AVG(price) as avg_price "
            "FROM dbo.products GROUP BY category ORDER BY cnt DESC"
        ).fetchall()
        assert len(result) >= 3
