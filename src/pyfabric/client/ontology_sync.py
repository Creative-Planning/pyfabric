"""
Sync ontology entity types to Lakehouse tables and data bindings.

Workflow:
  1. Define/update entity properties in the ontology definition
  2. This module creates or alters the Lakehouse table(s) to match
  3. This module creates or updates the data binding(s) to connect them

Supports single-entity sync and batch sync of all entities in one API round trip.

Ontology valueType -> Spark SQL type mapping:
  String   -> STRING
  DateTime -> TIMESTAMP
  BigInt   -> BIGINT
  Double   -> DOUBLE
  Boolean  -> BOOLEAN
  Object   -> STRING

Usage:
    from pyfabric.client.http import FabricClient
    from pyfabric.client.livy import LivyClient
    from pyfabric.client.ontology_sync import sync_all_entities

    client = FabricClient()
    with LivyClient(cred, ws_id, lh_id) as livy:
        results = sync_all_entities(client, ws_id, ontology_id, livy, lh_id)
"""

import structlog

from .http import FabricClient
from .livy import LivyClient
from .ontology import (
    add_data_binding_to_parts,
    decode_definition,
    encode_definition,
    entity_name_to_table,
    get_entity_type_from_parts,
    get_ontology_definition,
    list_data_bindings_from_parts,
    list_entity_types_from_parts,
    make_lakehouse_binding,
    make_property_binding,
    update_data_binding_in_parts,
    update_ontology_definition,
)

log = structlog.get_logger()

ONTOLOGY_TYPE_TO_SPARK = {
    "String": "STRING",
    "DateTime": "TIMESTAMP",
    "BigInt": "BIGINT",
    "Double": "DOUBLE",
    "Boolean": "BOOLEAN",
    "Object": "STRING",
}


def _spark_type(ontology_type: str) -> str:
    return ONTOLOGY_TYPE_TO_SPARK.get(ontology_type, "STRING")


def _table_exists(livy: LivyClient, table_name: str) -> bool:
    result = livy.execute(
        f'print(spark.catalog.tableExists("{table_name}"))',
        kind="pyspark",
    )
    return result is not None and "True" in result


def _get_table_columns(livy: LivyClient, table_name: str) -> set[str]:
    """Get existing column names from a Lakehouse table."""
    result = livy.execute(
        f'print([f.name for f in spark.table("{table_name}").schema.fields])',
        kind="pyspark",
    )
    if not result:
        return set()
    import ast

    try:
        return set(ast.literal_eval(result.strip()))
    except (ValueError, SyntaxError):
        return set()


def _sync_table(livy: LivyClient, entity: dict, table_name: str) -> None:
    """Create or alter a Lakehouse table to match an entity's properties."""
    all_props = entity.get("properties", []) + entity.get("timeseriesProperties", [])
    if not all_props:
        log.info("SKIP %s - no properties", entity["name"])
        return

    if _table_exists(livy, table_name):
        log.info("Table '%s' exists - checking for new columns...", table_name)
        existing_cols = _get_table_columns(livy, table_name)
        new_props = [p for p in all_props if p["name"] not in existing_cols]
        if new_props:
            for prop in new_props:
                spark_type = _spark_type(prop["valueType"])
                log.info("  ALTER TABLE: adding %s (%s)", prop["name"], spark_type)
                livy.sql(
                    f"ALTER TABLE {table_name} ADD COLUMN {prop['name']} {spark_type}"
                )
            log.info("  Added %d column(s)", len(new_props))
        else:
            log.info("  Already in sync")
    else:
        cols = ", ".join(
            f"{p['name']} {_spark_type(p['valueType'])}" for p in all_props
        )
        log.info("Creating table '%s'...", table_name)
        livy.sql(f"CREATE TABLE {table_name} ({cols}) USING DELTA")
        log.info("  Created with %d columns", len(all_props))


def _build_binding(
    parts: list[dict],
    entity: dict,
    entity_type_id: str,
    workspace_id: str,
    lakehouse_id: str,
    table_name: str,
) -> tuple[list[dict], str]:
    """Build or update a data binding for an entity. Returns (updated_parts, binding_id)."""
    all_props = entity.get("properties", []) + entity.get("timeseriesProperties", [])

    prop_bindings = [make_property_binding(p["name"], p["id"]) for p in all_props]

    # Find existing binding for this table
    existing_bindings = list_data_bindings_from_parts(parts, entity_type_id)
    existing_binding_id = None
    for b in existing_bindings:
        src = (
            b["content"]
            .get("dataBindingConfiguration", {})
            .get("sourceTableProperties", {})
        )
        if src.get("sourceTableName") == table_name:
            existing_binding_id = b["content"]["id"]
            break

    has_timeseries = len(entity.get("timeseriesProperties", [])) > 0
    binding_type = "TimeSeries" if has_timeseries else "NonTimeSeries"
    timestamp_col = None
    if has_timeseries:
        ts_props = entity["timeseriesProperties"]
        ts_datetime = next((p for p in ts_props if p["valueType"] == "DateTime"), None)
        timestamp_col = ts_datetime["name"] if ts_datetime else ts_props[0]["name"]

    bid, binding_def = make_lakehouse_binding(
        entity_type_id,
        prop_bindings,
        workspace_id,
        lakehouse_id,
        table_name,
        binding_type=binding_type,
        timestamp_column=timestamp_col,
        binding_id=existing_binding_id,
    )

    if existing_binding_id:
        log.info("  Updating binding (%s)", bid)
        parts = update_data_binding_in_parts(parts, entity_type_id, bid, binding_def)
    else:
        log.info("  Creating binding (%s)", bid)
        parts = add_data_binding_to_parts(parts, entity_type_id, bid, binding_def)

    return parts, bid


def sync_entity_to_lakehouse(
    client: FabricClient,
    ws_id: str,
    ontology_id: str,
    entity_type_id: str,
    livy: LivyClient,
    lakehouse_id: str,
    table_name: str,
) -> str:
    """
    Sync a single ontology entity type to a Lakehouse table and data binding.

    Returns the binding ID.
    """
    log.info("Fetching ontology definition...")
    raw = get_ontology_definition(client, ws_id, ontology_id)
    parts = decode_definition(raw)

    entity = get_entity_type_from_parts(parts, entity_type_id)
    if not entity:
        raise ValueError(f"Entity type {entity_type_id} not found in ontology")

    all_props = entity.get("properties", []) + entity.get("timeseriesProperties", [])
    if not all_props:
        raise ValueError(f"Entity type {entity['name']} has no properties")

    log.info("Entity: %s (%d properties)", entity["name"], len(all_props))

    _sync_table(livy, entity, table_name)
    parts, bid = _build_binding(
        parts, entity, entity_type_id, ws_id, lakehouse_id, table_name
    )

    log.info("Pushing ontology definition...")
    encoded = encode_definition(parts)
    update_ontology_definition(client, ws_id, ontology_id, encoded["parts"])

    log.info("Done - entity '%s' synced to table '%s'", entity["name"], table_name)
    return bid


def sync_all_entities(
    client: FabricClient,
    ws_id: str,
    ontology_id: str,
    livy: LivyClient,
    lakehouse_id: str,
    *,
    entity_ids: list[str] | None = None,
    table_map: dict[str, str] | None = None,
) -> dict:
    """
    Sync all (or specified) ontology entities to Lakehouse tables and bindings.

    Reads the ontology definition once, creates/alters all tables, builds all
    bindings in memory, then pushes a single updated definition.

    Returns:
        dict mapping entity_type_id -> {"table": table_name, "binding_id": bid}
    """
    table_map = table_map or {}

    log.info("Fetching ontology definition...")
    raw = get_ontology_definition(client, ws_id, ontology_id)
    parts = decode_definition(raw)

    all_entities = list_entity_types_from_parts(parts)
    if entity_ids:
        entities = [e for e in all_entities if str(e["id"]) in entity_ids]
        if len(entities) != len(entity_ids):
            found = {str(e["id"]) for e in entities}
            missing = set(entity_ids) - found
            raise ValueError(f"Entity type(s) not found: {missing}")
    else:
        entities = all_entities

    if not entities:
        log.info("No entities to sync.")
        return {}

    log.info("Syncing %d entity type(s) to Lakehouse...", len(entities))

    results = {}
    for entity in entities:
        et_id = str(entity["id"])
        table_name = table_map.get(et_id, entity_name_to_table(entity["name"]))
        all_props = entity.get("properties", []) + entity.get(
            "timeseriesProperties", []
        )

        log.info(
            "[%s] (%d properties -> %s)", entity["name"], len(all_props), table_name
        )

        if not all_props:
            log.info("  SKIP - no properties")
            continue

        _sync_table(livy, entity, table_name)
        results[et_id] = {"table": table_name}

    log.info("Building data bindings...")
    for et_id, info in results.items():
        entity = get_entity_type_from_parts(parts, et_id)
        log.info("  [%s]", entity["name"])
        parts, bid = _build_binding(
            parts, entity, et_id, ws_id, lakehouse_id, info["table"]
        )
        info["binding_id"] = bid

    log.info("Pushing ontology definition (single update)...")
    encoded = encode_definition(parts)
    update_ontology_definition(client, ws_id, ontology_id, encoded["parts"])

    log.info("Sync complete:")
    for et_id, info in results.items():
        entity = get_entity_type_from_parts(parts, et_id)
        log.info(
            "  %s -> %s (binding: %s...)",
            entity["name"],
            info["table"],
            info["binding_id"][:8],
        )

    return results


def create_tables_from_config(
    livy: LivyClient,
    entities_config: list,
    entity_map: dict,
) -> None:
    """Create Lakehouse tables from a config entity list and entity_map."""
    for entity_cfg in entities_config:
        name = entity_cfg["name"]
        table = entity_map[name]["table"]
        cols = ", ".join(
            f"{p['name']} {_spark_type(p['valueType'])}"
            for p in entity_cfg["properties"]
        )
        log.info("Creating table %s...", table)
        livy.sql(f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING DELTA")


def load_csv_data(
    livy: LivyClient,
    csv_dir,
    entities_config: list,
    entity_map: dict,
) -> None:
    """Load seed data from CSV files into Lakehouse tables.

    CSV column headers must match property names exactly. Files are named
    after the entity (e.g. Course.csv for entity "Course").
    """
    import csv as csv_mod
    from pathlib import Path

    csv_dir = Path(csv_dir)

    for entity_cfg in entities_config:
        name = entity_cfg["name"]
        table = entity_map[name]["table"]
        csv_path = csv_dir / f"{name}.csv"

        if not csv_path.exists():
            log.info("SKIP %s - no seed data file", name)
            continue

        type_map = {p["name"]: p["valueType"] for p in entity_cfg["properties"]}

        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))

        if not rows:
            log.info("SKIP %s - empty CSV", name)
            continue

        columns = list(rows[0].keys())
        col_list = ", ".join(columns)

        value_rows = []
        for row in rows:
            values = []
            for col in columns:
                val = row[col].strip() if row[col] else ""
                vtype = type_map.get(col, "String")
                if val == "":
                    values.append("NULL")
                elif vtype == "String":
                    values.append(f"'{val.replace(chr(39), chr(39) * 2)}'")
                elif vtype in ("BigInt", "Double"):
                    values.append(val)
                elif vtype == "Boolean":
                    values.append(val.lower())
                elif vtype == "DateTime":
                    values.append(f"TIMESTAMP '{val}'")
                else:
                    values.append(f"'{val.replace(chr(39), chr(39) * 2)}'")
            value_rows.append(f"({', '.join(values)})")

        sql = f"INSERT INTO {table} ({col_list}) VALUES {', '.join(value_rows)}"
        log.info("Loading %d rows into %s...", len(rows), table)
        livy.sql(sql)
