import json
from pathlib import Path
import pytest

try:
    import fastjsonschema  # type: ignore
except ImportError:  # pragma: no cover
    fastjsonschema = None  # Will be installed during implementation phase

SCHEMA_PATH = Path('specs/001-i-am-creatina/contracts/archive_metadata.schema.json')

@pytest.mark.skipif(fastjsonschema is None, reason="fastjsonschema not installed yet")
def test_archive_metadata_schema_loads():
    schema = json.loads(SCHEMA_PATH.read_text(encoding='utf-8'))
    assert schema['title'] == 'Archive File Metadata'
    assert 'properties' in schema

# Placeholder: future tests will validate a generated sidecar example against the schema.
