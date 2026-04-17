"""Schema-as-code for lakehouse tables.

Define column and table schemas once; generate Spark DDL, DuckDB DDL, and
PyArrow schemas from the same definitions. Used by consumers that need to
mirror a Fabric lakehouse locally in DuckDB, validate rows before insert,
and push Delta tables back to OneLake.

Example:

    from pyfabric.data.schema import Col, TableDef

    PRODUCTS = TableDef(
        name="products",
        description="Product catalog",
        columns=(
            Col("product_id", "int", nullable=False, pk=True),
            Col("name", "string", nullable=False),
            Col("price", "double"),
            Col("is_active", "boolean"),
        ),
    )

    spark_ddl = PRODUCTS.to_spark_ddl(schema="dbo")
    duck_ddl = PRODUCTS.to_duckdb_ddl(schema="ddb")
    arrow_schema = PRODUCTS.to_arrow_schema()
"""

import datetime as _dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa


# Internal type keys are the single source of truth. Maps below translate
# them to the vocabulary of each backend.

SPARK_TYPES: dict[str, str] = {
    "string": "STRING",
    "int": "INT",
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}

DUCKDB_TYPES: dict[str, str] = {
    "string": "VARCHAR",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}

# Accepted Python types per type_key for row validation. `bool` is excluded
# from numeric types even though `isinstance(True, int)` is True — passing
# a bool for an int column is almost always a mistake.
PYTHON_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "int": (int,),
    "bigint": (int,),
    "double": (int, float),
    "boolean": (bool,),
    "date": (_dt.date,),
    "timestamp": (_dt.datetime,),
}


@dataclass(frozen=True)
class Col:
    """A column definition.

    Attributes:
        name:      Column name.
        type_key:  Internal type key — one of the keys in SPARK_TYPES etc.
        nullable:  Whether the column allows NULL.
        pk:        Primary key flag (informational — not enforced in Delta).
    """

    name: str
    type_key: str
    nullable: bool = True
    pk: bool = False

    def __post_init__(self) -> None:
        if self.type_key not in SPARK_TYPES:
            raise ValueError(
                f"Unknown type_key {self.type_key!r} for column {self.name!r}. "
                f"Valid keys: {sorted(SPARK_TYPES)}"
            )


@dataclass(frozen=True)
class TableDef:
    """A table definition."""

    name: str
    columns: tuple[Col, ...]
    description: str = ""

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def pk_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.pk]

    def column(self, name: str) -> Col:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"Column {name!r} not found in table {self.name!r}")

    def to_spark_ddl(self, schema: str = "dbo") -> str:
        """CREATE TABLE statement using Spark/Delta types."""
        lines = []
        for c in self.columns:
            spark_type = SPARK_TYPES[c.type_key]
            null = "" if c.nullable else " NOT NULL"
            lines.append(f"  `{c.name}` {spark_type}{null}")
        cols = ",\n".join(lines)
        return (
            f"CREATE TABLE IF NOT EXISTS {schema}.{self.name} (\n{cols}\n) USING DELTA"
        )

    def to_duckdb_ddl(self, schema: str | None = None) -> str:
        """CREATE TABLE statement using DuckDB types."""
        lines = []
        for c in self.columns:
            duck_type = DUCKDB_TYPES[c.type_key]
            null = "" if c.nullable else " NOT NULL"
            lines.append(f"  {c.name} {duck_type}{null}")
        cols = ",\n".join(lines)
        qualified = f"{schema}.{self.name}" if schema else self.name
        return f"CREATE TABLE IF NOT EXISTS {qualified} (\n{cols}\n)"

    def to_arrow_schema(self) -> "pa.Schema":
        """PyArrow Schema suitable for constructing a RecordBatch/Table."""
        import pyarrow as pa

        arrow_types = {
            "string": pa.string(),
            "int": pa.int32(),
            "bigint": pa.int64(),
            "double": pa.float64(),
            "boolean": pa.bool_(),
            "date": pa.date32(),
            "timestamp": pa.timestamp("us"),
        }
        fields = [
            pa.field(c.name, arrow_types[c.type_key], nullable=c.nullable)
            for c in self.columns
        ]
        return pa.schema(fields)

    def validate_row(self, row: dict[str, Any]) -> list[str]:
        """Return a list of validation error messages, empty if row is valid.

        Enforces:
          - Non-nullable columns present and not None.
          - Python type matches the column's type_key (see PYTHON_TYPES).
          - Empty strings are NOT accepted as a substitute for None on
            non-string columns — a common source of silent data corruption.
          - For `boolean` columns, reject values that happen to be ``int``
            (e.g. ``0``/``1``) to match the strict semantics elsewhere.
        """
        errors: list[str] = []
        for c in self.columns:
            if c.name not in row:
                if not c.nullable:
                    errors.append(f"missing required column {c.name!r}")
                continue

            val = row[c.name]

            if val is None:
                if not c.nullable:
                    errors.append(f"column {c.name!r} is NOT NULL but got None")
                continue

            if c.type_key != "string" and isinstance(val, str) and val == "":
                errors.append(
                    f"column {c.name!r} ({c.type_key}) got empty string; "
                    "use None for missing values on non-string columns"
                )
                continue

            allowed = PYTHON_TYPES[c.type_key]
            if c.type_key in ("int", "bigint", "double") and isinstance(val, bool):
                errors.append(
                    f"column {c.name!r} ({c.type_key}) got bool; expected "
                    f"{[t.__name__ for t in allowed]}"
                )
                continue

            if not isinstance(val, allowed):
                errors.append(
                    f"column {c.name!r} ({c.type_key}) got "
                    f"{type(val).__name__}; expected "
                    f"{[t.__name__ for t in allowed]}"
                )
        return errors


def all_spark_ddl(tables: tuple[TableDef, ...], schema: str = "dbo") -> list[str]:
    """CREATE TABLE statements for a tuple of tables (Spark/Delta)."""
    return [t.to_spark_ddl(schema) for t in tables]


def all_duckdb_ddl(
    tables: tuple[TableDef, ...], schema: str | None = None
) -> list[str]:
    """CREATE TABLE statements for a tuple of tables (DuckDB)."""
    return [t.to_duckdb_ddl(schema) for t in tables]


# ── Schema drift detection ──────────────────────────────────────────────────
#
# Each backend reports column types in its own vocabulary. We normalize them
# to the internal type_key so drift checks are uniform. Maps live here rather
# than on TableDef because they're reverse lookups.

_DUCKDB_TO_TYPE_KEY: dict[str, str] = {
    "VARCHAR": "string",
    "INTEGER": "int",
    "BIGINT": "bigint",
    "DOUBLE": "double",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
}


def validate_duckdb_schema(
    table_def: "TableDef",
    conn: Any,
    *,
    schema: str | None = None,
) -> list[str]:
    """Compare ``table_def`` against the live DuckDB table. Returns discrepancies.

    Checks for missing columns, extra columns, and type mismatches. Returns
    an empty list when the actual table matches the definition.

    Args:
        conn:    DuckDB connection.
        schema:  DuckDB schema name. ``None`` means the default schema.
    """
    if schema:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table_def.name],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table_def.name],
        ).fetchall()
    actual = {name: (data_type or "").upper() for name, data_type in rows}
    return _diff(table_def, actual, _DUCKDB_TO_TYPE_KEY)


_ARROW_TO_TYPE_KEY: dict[str, str] = {
    "string": "string",
    "large_string": "string",
    "int32": "int",
    "int64": "bigint",
    "double": "double",
    "float": "double",
    "bool": "boolean",
    "date32[day]": "date",
    "timestamp": "timestamp",
}


def validate_arrow_schema(table_def: "TableDef", arrow_schema: Any) -> list[str]:
    """Compare ``table_def`` against a PyArrow Schema. Returns discrepancies."""
    actual: dict[str, str] = {}
    for field in arrow_schema:
        t = str(field.type)
        # Timestamps carry unit/tz suffixes; normalize to "timestamp".
        if t.startswith("timestamp"):
            t = "timestamp"
        actual[field.name] = t
    return _diff(table_def, actual, _ARROW_TO_TYPE_KEY)


def _diff(
    table_def: "TableDef",
    actual: dict[str, str],
    mapping: dict[str, str],
) -> list[str]:
    errors: list[str] = []
    if not actual:
        return [f"table {table_def.name!r} not found"]

    expected_names = {c.name for c in table_def.columns}
    for c in table_def.columns:
        if c.name not in actual:
            errors.append(f"missing column {c.name!r} ({c.type_key})")
            continue
        actual_key = mapping.get(actual[c.name])
        if actual_key is None:
            errors.append(
                f"column {c.name!r} has unrecognized backend type {actual[c.name]!r}"
            )
            continue
        if actual_key != c.type_key:
            errors.append(
                f"column {c.name!r} type mismatch: expected {c.type_key}, "
                f"got {actual_key} ({actual[c.name]})"
            )

    for extra in sorted(set(actual) - expected_names):
        errors.append(f"unexpected column {extra!r} ({actual[extra]})")
    return errors
