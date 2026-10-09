#!/bin/bash
cd "$(dirname "$0")"
if ! command -v node >/dev/null 2>&1; then echo "Node.js is not installed. Get the LTS version from https://nodejs.org then run this again."; open https://nodejs.org 2>/dev/null; read -p "Press Enter..."; exit 1; fi
AAC_OPEN=1 node server.js
