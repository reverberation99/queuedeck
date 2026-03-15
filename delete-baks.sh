#!/bin/bash

echo
echo "QueueDeck backup cleanup"
echo "This will delete ONLY files matching *.bak.*"
echo

files=$(find . -type f -name "*.bak.*")

if [ -z "$files" ]; then
    echo "No backup files found."
    exit 0
fi

echo "The following files will be removed:"
echo
echo "$files"
echo

read -p "Continue deleting these files? (y/N): " confirm

if [[ "$confirm" =~ ^[Yy]$ ]]; then
    find . -type f -name "*.bak.*" -delete
    echo
    echo "Backup files deleted."
else
    echo
    echo "Operation cancelled."
fi
