"""ID generation utilities for ontology definitions."""

import random
import uuid


def _generate_bigint_id() -> str:
    """Generate a positive 64-bit integer ID as a string."""
    return str(random.randint(10**12, 10**16 - 1))


def generate_id() -> str:
    """Generate a random positive 64-bit integer ID as a string."""
    return str(random.randint(10**15, 10**18))


def generate_guid() -> str:
    """Generate a UUID for data binding IDs."""
    return str(uuid.uuid4())
