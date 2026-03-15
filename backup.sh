#!/bin/bash
set -e

BASE_DIR=~/docker/queuedeck-dev
BACKUP_DIR=~/docker/backups/queuedeck
STAMP=$(date +"%Y-%m-%d_%H-%M-%S")
NEW_BACKUP="$BACKUP_DIR/queuedeck-dev-$STAMP.tar.gz"

mkdir -p "$BACKUP_DIR"

echo "Creating backup..."
tar -czf "$NEW_BACKUP" -C "$BASE_DIR" .

echo "Backup created:"
echo "$NEW_BACKUP"

echo
echo "Pruning old timestamped backups..."

python3 - <<'PY'
from pathlib import Path
from collections import defaultdict
import os

backup_dir = Path(os.path.expanduser("~/docker/backups/queuedeck"))
files = sorted(backup_dir.glob("queuedeck-dev-20*.tar.gz"))

by_day = defaultdict(list)
for f in files:
    day = f.name[len("queuedeck-dev-"):len("queuedeck-dev-")+10]
    by_day[day].append(f)

keep = set()

# Keep latest 10 overall
for f in files[-10:]:
    keep.add(f)

# Keep newest backup from each day
for day_files in by_day.values():
    keep.add(day_files[-1])

delete = [f for f in files if f not in keep]

for f in delete:
    f.unlink()
    print("Deleted", f.name)

print(f"Kept {len(keep)} timestamped backups.")
PY
