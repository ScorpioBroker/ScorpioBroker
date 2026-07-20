#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if command -v python3 >/dev/null 2>&1; then
  exec python3 "${SCRIPT_DIR}/export-schema.py" "$@"
elif command -v py >/dev/null 2>&1; then
  exec py -3 "${SCRIPT_DIR}/export-schema.py" "$@"
else
  exec python "${SCRIPT_DIR}/export-schema.py" "$@"
fi
