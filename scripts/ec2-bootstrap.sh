#!/usr/bin/env bash
# Ubuntu 22.04/24.04 LTS on EC2: install Docker Engine + Compose plugin.
# Run once after SSH: bash scripts/ec2-bootstrap.sh
# Then log out and back in (or `newgrp docker`) so your user can run `docker`.

set -euo pipefail

if [[ "$(id -u)" -eq 0 ]]; then
  echo "Run as the default ubuntu user (script will use sudo)."
  exit 1
fi

sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg

curl -fsSL https://get.docker.com | sudo sh

sudo usermod -aG docker "$USER"

echo ""
echo "Docker installed. Log out and SSH back in, then:"
echo "  cd document-intelligence-system/docker"
echo "  docker compose -f docker-compose.ec2.yml up -d --build"
echo ""
echo "Optional: open http://<this-host>:8000/docs only after locking down the security group."
