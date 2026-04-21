#!/bin/bash
# Publish cdpm-recovery-sim to MU App Store
# Usage: ./publish.sh [version]
# Example: ./publish.sh 1.1.0

VERSION=${1:-1.0.2}

echo "Publishing cdpm-recovery-sim v${VERSION} to MU App Store..."

# Update version in pyproject.toml
sed -i "s/^version = .*/version = \"${VERSION}\"/" pyproject.toml

# Also update the __init__.py in the package directory
cp cdpm_recovery_sim.py cdpm_recovery_sim/__init__.py

SSL_CERT_FILE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem \
REQUESTS_CA_BUNDLE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem \
UV_NATIVE_TLS=1 \
UV_PYTHON=/home/asegaran/MODULE_YIELD_DASHBOARD/.venv/bin/python \
UV_PYTHON_PREFERENCE=only-system \
PATH="/home/asegaran/MODULE_YIELD_DASHBOARD/.venv/bin:$PATH" \
~/.local/bin/appstore publish . \
  --name cdpm-recovery-sim \
  --version "${VERSION}" \
  --entry-point "cdpm-recovery-sim=cdpm_recovery_sim:main" \
  --skip-test \
  --skip-confirm

echo "Done!"
echo ""
echo "To install/upgrade: appstore install cdpm-recovery-sim --upgrade"
echo "(Note: Users may need UV_NATIVE_TLS=1 environment variable)"
