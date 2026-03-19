#!/usr/bin/env bash

set -e

IMAGE="reverberation99/queuedeck:latest"
DATE_TAG=$(date +%Y%m%d-%H%M)

echo "🚧 Building Docker image..."
sudo docker build -t $IMAGE .

echo "🏷️ Tagging with date version..."
sudo docker tag $IMAGE reverberation99/queuedeck:$DATE_TAG

echo "📤 Pushing latest..."
sudo docker push $IMAGE

echo "📤 Pushing version tag: $DATE_TAG..."
sudo docker push reverberation99/queuedeck:$DATE_TAG

echo "✅ Done!"
echo "👉 Latest: $IMAGE"
echo "👉 Version: reverberation99/queuedeck:$DATE_TAG"
