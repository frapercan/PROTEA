#!/usr/bin/env python3
"""Build module-to-property-test map for mutation.yml."""
import json
import re
import sys
from pathlib import Path

test_dir = Path("tests/property")
test_module_map = {}
for test_file in sorted(test_dir.glob("test_*_property.py")):
    match = re.match(r"test_(.+)_property\.py$", test_file.name)
    if match:
        module_name = match.group(1)
        test_module_map[module_name] = module_name
json.dump(test_module_map, sys.stdout)
