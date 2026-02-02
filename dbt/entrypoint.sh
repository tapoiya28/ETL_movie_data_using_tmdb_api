#!/bin/bash
set -e

PROJECT_DIR="/usr/app/my_project"
PROFILES_FILE="/root/.dbt/profiles.yml"

# Check if project exists
if [ ! -d "$PROJECT_DIR" ]; then
    echo "Project not found. Initializing dbt project..."
    dbt init my_project --skip-profile-setup
    echo "dbt project initialized successfully!"
fi

# Create profiles.yml if it doesn't exist (outside project check!)
if [ ! -f "$PROFILES_FILE" ]; then
    echo "Creating default profiles.yml..."
    mkdir -p /root/.dbt
    cat > "$PROFILES_FILE" <<EOF
my_project:
  outputs:
    dev:
      type: postgres
      host: pgdatabase
      user: root
      password: root
      port: 5432
      dbname: movie_pipeline
      schema: dev
      threads: 4
  target: dev
EOF
    echo "profiles.yml created!"
fi

# Execute the command passed to the container
exec dbt "$@"