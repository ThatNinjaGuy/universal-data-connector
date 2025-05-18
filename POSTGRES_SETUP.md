# PostgreSQL Setup Guide for macOS

This guide will help you set up PostgreSQL on your Mac for use with the Universal Data Connector.

## Installation

### Using Homebrew (Recommended)

1. Install Homebrew if you haven't already:

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

2. Install PostgreSQL:

```bash
brew install postgresql@14
```

3. Start PostgreSQL service:

```bash
brew services start postgresql@14
```

4. Verify installation:

```bash
psql --version
```

## Database Setup

1. Create a new database:

```bash
createdb udc
```

2. Connect to the database:

```bash
psql udc
```

3. Create the employee table:

```sql
CREATE TABLE employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2)
);
```

4. Insert sample data:

```sql
INSERT INTO employee (name, email, department, salary) VALUES
    ('John Doe', 'john.doe@example.com', 'IT', 75000.00),
    ('Jane Smith', 'jane.smith@example.com', 'HR', 65000.00),
    ('Bob Wilson', 'bob.wilson@example.com', 'Sales', 80000.00),
    ('Alice Johnson', 'alice.johnson@example.com', 'Marketing', 90000.00),
    ('Michael Brown', 'michael.brown@example.com', 'Engineering', 100000.00);
```

5. Verify the data:

```sql
SELECT * FROM employee;
```

## Configuration

1. Update the pipeline configuration in `src/main/resources/pipeline-config.yaml`:

```yaml
- name: "postgres-to-parquet"
  source:
    type: jdbc
    properties:
      jdbcUrl: "jdbc:postgresql://localhost:5432/udc"
      user: "your_mac_username"  # Replace with your Mac username
      password: ""  # Leave empty if no password set
      table: "employee"
      query: "SELECT id, name, email, department, salary FROM employee ORDER BY id"
      batchSize: 1000
```

## Common Commands

### Start/Stop PostgreSQL

```bash
# Start PostgreSQL
brew services start postgresql@14

# Stop PostgreSQL
brew services stop postgresql@14

# Restart PostgreSQL
brew services restart postgresql@14
```

### Database Management

```bash
# List all databases
psql -l

# Connect to a specific database
psql database_name

# Drop a database
dropdb database_name

# Create a new database
createdb database_name
```

### User Management

```bash
# Create a new user
createuser username

# Set password for a user
psql -d postgres -c "ALTER USER username WITH PASSWORD 'password';"
```

## Troubleshooting

### Connection Issues

1. Verify PostgreSQL is running:

```bash
brew services list
```

2. Check PostgreSQL logs:

```bash
tail -f /usr/local/var/log/postgresql@14.log
```

3. Verify port is listening:

```bash
lsof -i :5432
```

### Permission Issues

1. If you get permission errors, ensure your user has the right privileges:

```sql
ALTER USER your_mac_username WITH SUPERUSER;
```

2. If you can't connect to the database, check pg_hba.conf:

```bash
# Location: /usr/local/var/postgresql@14/pg_hba.conf
# Add this line to allow local connections:
local   all             all                                     trust
```

### Data Issues

1. Reset the employee table:

```sql
DROP TABLE IF EXISTS employee;
CREATE TABLE employee (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2)
);
```

2. Clear all data:

```sql
TRUNCATE TABLE employee;
```

## Uninstallation

If you need to remove PostgreSQL:

1. Stop the service:

```bash
brew services stop postgresql@14
```

2. Uninstall PostgreSQL:

```bash
brew uninstall postgresql@14
```

3. Remove data directory (optional):

```bash
rm -rf /usr/local/var/postgresql@14
```

## Additional Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Homebrew PostgreSQL](https://formulae.brew.sh/formula/postgresql@14)
- [PostgreSQL Mac App](https://postgresapp.com/) (Alternative installation method)
