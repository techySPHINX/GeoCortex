import duckdb
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths relative to the script directory
db_path = os.path.join(script_dir, 'toll_analytics.db')
schema_path = os.path.join(script_dir, 'schema.sql')

def setup_database():
    """
    Sets up the DuckDB database by connecting to it and executing the 
    SQL schema to create the necessary tables.
    """
    # Connect to the DuckDB database. It will be created if it doesn't exist.
    con = duckdb.connect(database=db_path, read_only=False)

    # Read the SQL schema file
    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    # Execute the SQL script to create tables
    con.execute(schema_sql)

    # Close the connection
    con.close()

    print(f"Database '{db_path}' created and schema applied successfully.")

if __name__ == "__main__":
    setup_database()
