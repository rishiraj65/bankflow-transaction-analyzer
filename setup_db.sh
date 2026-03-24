#!/bin/sh
# This script will check if PostgreSQL is running and create the database if missing.
# (Requires psql to be installed)

DB_NAME="retail_banking"

# Check if DB exists
if psql -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
    echo "Database '$DB_NAME' already exists."
else
    echo "Creating database '$DB_NAME'..."
    createdb $DB_NAME
fi

# Apply schema
echo "Applying schema from sql/schema.sql..."
psql -d $DB_NAME -f sql/schema.sql
