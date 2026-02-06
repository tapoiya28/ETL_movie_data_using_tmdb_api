#!/bin/bash
set -e

PROJECT_NAME="my_project"
PROJECT_DIR="/usr/app/$PROJECT_NAME"
PROFILES_FILE="/root/.dbt/profiles.yml"

# Check if project exists
if [ ! -d "$PROJECT_DIR" ]; then
    echo "Project not found. Initializing dbt project..."
    dbt init $PROJECT_NAME --skip-profile-setup
    echo "dbt project initialized successfully!"
fi

# Create profiles.yml if it doesn't exist (outside project check!)
if [ ! -f "$PROFILES_FILE" ]; then
  echo "Creating default profiles.yml..."
  mkdir -p /root/.dbt
  cat > "$PROFILES_FILE" <<EOF
$PROJECT_NAME:
  outputs:
    dev:
      type: postgres
      host: pgdatabase
      user: root
      password: root
      port: 5432
      dbname: movie_pipeline
      schema: raw
      threads: 4
  target: dev
EOF
    echo "profiles.yml created!"
fi
# Execute the command passed to the container with project directory
exec dbt "$@" --project-dir $PROJECT_DIR