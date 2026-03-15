#!/bin/bash

echo "Searching for QueueDeck backup files (.bak.*)..."
echo

find . -type f -name "*.bak.*" | sort

echo
echo "Done."
