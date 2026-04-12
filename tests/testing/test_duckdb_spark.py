"""Tests for DuckDBSparkSession — the DuckDB-backed Spark mock."""

import pytest

from pyfabric.testing.duckdb_spark import (
    DuckDBSparkSession,
    Row,
    TableInfo,
)


class TestRow:
    def test_index_access(self):
        r = Row((1, "alice", 3.14), ["id", "name", "value"])
        assert r[0] == 1
        assert r[1] == "alice"
        assert r[2] == 3.14

    def test_column_access(self):
        r = Row((42, "test"), ["id", "name"])
        assert r["id"] == 42
        assert r["name"] == "test"

    def test_column_access_without_names_raises(self):
        r = Row((1, 2), None)
        with pytest.raises(KeyError):
            r["id"]

    def test_as_dict(self):
        r = Row((1, "alice"), ["id", "name"])
        assert r.asDict() == {"id": 1, "name": "alice"}

    def test_repr_with_columns(self):
        r = Row((1,), ["id"])
        assert "id=1" in repr(r)


class TestTableInfo:
    def test_attributes(self):
        t = TableInfo(database="lh_test", name="products")
        assert t.database == "lh_test"
        assert t.name == "products"
        assert t.isTemporary is False

    def test_index_access(self):
        t = TableInfo(database="db", name="tbl")
        assert t[0] == "db"
        assert t[1] == "tbl"
        assert t[2] is False


class TestDuckDBSparkSession:
    def test_basic_sql(self):
        spark = DuckDBSparkSession()
        df = spark.sql("SELECT 1 AS value, 'hello' AS msg")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0]["msg"] == "hello"
        spark.stop()

    def test_count(self):
        spark = DuckDBSparkSession()
        df = spark.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(x)")
        assert df.count() == 3
        spark.stop()

    def test_show_does_not_raise(self, capsys):
        spark = DuckDBSparkSession()
        df = spark.sql("SELECT 1 AS id, 'test' AS name")
        df.show()  # should print without error
        captured = capsys.readouterr()
        assert "id" in captured.out
        assert "test" in captured.out
        spark.stop()

    def test_show_empty(self, capsys):
        spark = DuckDBSparkSession()
        df = spark.sql("SELECT 1 AS x WHERE 1=0")
        df.show()
        captured = capsys.readouterr()
        assert "Empty" in captured.out
        spark.stop()

    def test_iteration(self):
        spark = DuckDBSparkSession()
        df = spark.sql("SELECT * FROM (VALUES (1), (2)) AS t(x)")
        values = [r[0] for r in df]
        assert values == [1, 2]
        spark.stop()

    def test_to_pandas(self):
        pytest.importorskip("pandas")
        spark = DuckDBSparkSession()
        df = spark.sql("SELECT 1 AS id, 'a' AS name")
        pdf = df.toPandas()
        assert len(pdf) == 1
        assert "id" in pdf.columns
        spark.stop()


class TestDeltaTableDiscovery:
    """Test Delta table rewriting with real Delta files."""

    @pytest.fixture
    def lakehouse_with_delta(self, tmp_path):
        """Create a lakehouse dir with a real Delta table."""
        try:
            import pyarrow as pa
            from deltalake import write_deltalake
        except ImportError:
            pytest.skip("deltalake + pyarrow required")

        table_dir = tmp_path / "lh_test" / "Tables" / "products"
        table_dir.mkdir(parents=True)
        table = pa.table(
            {
                "id": pa.array([1, 2, 3]),
                "name": pa.array(["Widget A", "Widget B", "Widget C"]),
                "price": pa.array([9.99, 19.99, 29.99]),
            }
        )
        write_deltalake(str(table_dir), table)
        return tmp_path

    def test_sql_rewrites_table_reference(self, lakehouse_with_delta):
        spark = DuckDBSparkSession(lakehouse_root=lakehouse_with_delta)
        df = spark.sql("SELECT * FROM lh_test.products")
        assert df.count() == 3
        spark.stop()

    def test_show_tables(self, lakehouse_with_delta):
        spark = DuckDBSparkSession(lakehouse_root=lakehouse_with_delta)
        df = spark.sql("SHOW TABLES IN lh_test")
        rows = df.collect()
        names = [r[1] for r in rows]
        assert "products" in names
        spark.stop()

    def test_show_tables_empty_lakehouse(self, tmp_path):
        spark = DuckDBSparkSession(lakehouse_root=tmp_path)
        df = spark.sql("SHOW TABLES IN nonexistent")
        assert df.count() == 0
        spark.stop()

    def test_catalog_list_tables(self, lakehouse_with_delta):
        spark = DuckDBSparkSession(lakehouse_root=lakehouse_with_delta)
        tables = spark.catalog.listTables("lh_test")
        assert len(tables) >= 1
        assert any(t.name == "products" for t in tables)
        spark.stop()

    def test_catalog_table_exists(self, lakehouse_with_delta):
        spark = DuckDBSparkSession(lakehouse_root=lakehouse_with_delta)
        assert spark.catalog.tableExists("products")
        assert not spark.catalog.tableExists("nonexistent")
        spark.stop()
