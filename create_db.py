import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv()

def create_database():
    # Connect to the default 'postgres' database to create the new one
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "retail_banking_scratch_v1")
    
    try:
        conn = psycopg2.connect(
            dbname='postgres', 
            user=db_user, 
            password=db_password, 
            host=db_host,
            port=db_port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'retail_banking_scratch_v1'")
        exists = cur.fetchone()
        
        if not exists:
            print("Creating database 'retail_banking_scratch_v1'...")
            cur.execute('CREATE DATABASE retail_banking_scratch_v1')
            print("Database 'retail_banking_scratch_v1' created successfully.")
        else:
            print("Database 'retail_banking_scratch_v1' already exists.")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        print("Please check your PostgreSQL credentials in create_db.py if they differ from 'postgres/postgres'.")

if __name__ == "__main__":
    create_database()
