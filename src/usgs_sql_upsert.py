import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_connection_string():
    # Create SQL Server connection string from env variables
    server = os.getenv('SQL_SERVER', 'localhost\\SQLEXPRESS')
    database = os.getenv('SQL_DATABASE')
    username = os.getenv('SQL_USERNAME')
    password = os.getenv('SQL_PASSWORD')
    
    if not database:
        raise ValueError("SQL_DATABASE must be set in .env file")
    
    # Build connection URL using SQLAlchemy's URL object
    connection_params = {
        "drivername": "mssql+pyodbc",
        "host": server,
        "database": database,
        "username": username,
        "password": password,
        "query": {
            "driver": "ODBC Driver 17 for SQL Server",
            "TrustServerCertificate": "yes",
            "Encrypt": "no"
        }
    }
    
    return URL.create(**connection_params)

# Get column names from existing database table
def get_table_columns(engine, table_name='usgs_earthquake_data'):
    
    query = f"""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        columns = [row[0] for row in result]
    
    if not columns:
        raise ValueError(f"Table '{table_name}' does not exist")
    
    return columns

# Upsert data to SQL Server table using MERGE statement
def upsert_data(df, engine, table_name='usgs_earthquake_data', batch_size=1000):
    
    # Get actual table columns from database
    db_columns = get_table_columns(engine, table_name)

    # Filter to matching columns only
    available_cols = [col for col in db_columns if col in df.columns]
    df_filtered = df[available_cols].where(pd.notnull(df[available_cols]), None)
    
    # Process in batches
    total_batches = (len(df_filtered) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(df_filtered))
        batch_df = df_filtered.iloc[start_idx:end_idx]
        
        temp_table = f"#temp_{table_name}"
        
        with engine.connect() as conn:
            # Drop temp table if it exists
            conn.execute(text(f"IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL DROP TABLE {temp_table}"))
            
            # Insert batch into temp table
            batch_df.to_sql(temp_table, conn, if_exists='append', index=False)
            
            update_cols = [col for col in available_cols if col != 'id']
            update_set = ', '.join([f"target.[{col}] = source.[{col}]" for col in update_cols])
            insert_cols = ', '.join([f"[{col}]" for col in available_cols])
            insert_values = ', '.join([f"source.[{col}]" for col in available_cols])
            
            merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING {temp_table} AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values});
            """
            
            conn.execute(text(merge_sql))
            conn.commit()
        
        print(f"Batch {batch_num + 1}/{total_batches} complete ({end_idx}/{len(df_filtered)} records)")
    
    print("Upsert complete!")

def main():
    csv_path = './data/processed/usgs_earthquake_processed.csv'
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    print(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} records")
    
    print("Connecting to database...")
    engine = create_engine(create_connection_string(), echo=False)
    
    upsert_data(df, engine)
    print("Complete")

if __name__ == "__main__":
    main()