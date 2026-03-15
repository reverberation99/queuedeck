#!/bin/bash

set -e

PROJECT_DIR="$HOME/docker/queuedeck-dev"

cd "$PROJECT_DIR"

echo
echo "QueueDeck Git Push Helper"
echo "========================="
echo

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: $PROJECT_DIR is not a git repository."
  exit 1
fi

echo "Current branch:"
git branch --show-current
echo

echo "Changed files:"
git status --short
echo

if [ -z "$(git status --porcelain)" ]; then
  echo "No changes to commit."
  exit 0
fi

read -rp "Commit message: " COMMIT_MSG

if [ -z "$COMMIT_MSG" ]; then
  echo "Commit message cannot be empty."
  exit 1
fi

echo
echo "Adding files..."
git add .

echo "Committing..."
git commit -m "$COMMIT_MSG"

echo "Pushing to GitHub..."
git push

echo
echo "Done."
echo
git status -sb
