#!/bin/bash

set -e

echo "🔄 Adding changes..."
git add .

echo "📝 Enter commit message:"
read msg

git commit -m "$msg"

echo "🚀 Pushing to GitHub..."
git push

echo "✅ Done."
