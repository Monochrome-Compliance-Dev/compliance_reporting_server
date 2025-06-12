#!/bin/bash
# Install PostgreSQL client
echo "📦 Installing PostgreSQL client..."

# Use Amazon Linux 2023 package manager
if command -v dnf &> /dev/null; then
  sudo dnf install -y postgresql
else
  echo "❌ dnf not found – are we on the wrong platform?"
  exit 1
fi