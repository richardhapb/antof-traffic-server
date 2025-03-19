#!/bin/bash
set -e

# Load test environment variables
set -a
source .env.test
set +a

# Run migrations
sqlx database create
sqlx migrate run
