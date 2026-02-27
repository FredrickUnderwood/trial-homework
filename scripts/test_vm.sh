#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: ./test_vm.sh http://<VM_IP>:8080"
  exit 1
fi

BASE_URL=$1

# We can reuse the same test script logic by delegating to test_local.sh
# just passing the base URL
./scripts/test_local.sh "$BASE_URL"
