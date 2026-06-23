import os
import sys
import time
import json
import csv
import pyodbc  # Swapped sqlite3 for pyodbc

WATCH_DIR = "./watched_folder"

# Your exact SQL Server connection string (Added the required Driver parameter)
# Fixed: A single-line raw string 'r""' using native ODBC parameters (No Encrypt/MARS noise needed for LocalDB)
CONN_STR = r"Driver={ODBC Driver 17 for SQL Server};Server=(localdb)\MSSQLLocalDB;Trusted_Connection=yes;"

def setup_database():
    """Creates the inventory table in SQL Server if it doesn't exist."""
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # SQL Server syntax check for table existence
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'inventory')
        BEGIN
            CREATE TABLE inventory (
                id NVARCHAR(50) PRIMARY KEY,
                item_name NVARCHAR(255),
                quantity INT,
                last_updated DATETIME
            )
        END
    ''')
    conn.commit()
    conn.close()

def upsert_to_sql_server(item_id, item_name, quantity):
    """Executes a high-performance MERGE (Upsert) statement in SQL Server."""
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # SQL Server doesn't have "INSERT OR REPLACE". We use "MERGE" instead.
    merge_query = '''
        MERGE inventory AS target
        USING (SELECT ? AS id, ? AS item_name, ? AS quantity) AS source
        ON (target.id = source.id)
        WHEN MATCHED THEN
            UPDATE SET target.item_name = source.item_name,
                       target.quantity = source.quantity,
                       target.last_updated = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (id, item_name, quantity, last_updated)
            VALUES (source.id, source.item_name, source.quantity, GETDATE());
    '''
    cursor.execute(merge_query, (item_id, item_name, int(quantity)))
    conn.commit()
    conn.close()

def process_json(file_path):
    """Parses JSON data for SQL Server."""
    with open(file_path, 'r') as f:
        data = json.load(f)
        upsert_to_sql_server(data["id"], data["item_name"], data["quantity"])
    print(f"✅ SQL Server updated via JSON: {data['id']}")

def process_csv(file_path):
    """Parses CSV data rows for SQL Server."""
    with open(file_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            upsert_to_sql_server(row["id"], row["item_name"], row["quantity"])
    print(f"✅ SQL Server updated via CSV: {file_path}")

def process_live_sql_query(file_path):
    """Reads a modified SQL script file and runs it live against SQL Server."""
    print(f"⚡ Detected modification in SQL Query file: {file_path}")
    try:
        with open(file_path, 'r') as f:
            sql_script = f.read()
        
        if not sql_script.strip():
            return # Skip if file is accidentally cleared/empty
            
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.execute(sql_script)
        conn.commit()
        conn.close()
        print("🚀 Custom SQL script executed flawlessly on Live Database!\n")
    except Exception as e:
        print(f"❌ Failed to execute modified SQL query: {e}\n")

def start_folder_watcher():
    """Monitors folder for Data updates (JSON/CSV) AND Query modifications (SQL)."""
    print(f"👀 Watching '{WATCH_DIR}' for Data & SQL Query modifications... (Ctrl+C to stop)")
    file_registry = {} 

    while True:
        try:
            if not os.path.exists(WATCH_DIR):
                os.makedirs(WATCH_DIR)

            current_files = os.listdir(WATCH_DIR)
            
            for file_name in current_files:
                file_path = os.path.join(WATCH_DIR, file_name)
                
                # Accept JSON, CSV, and now SQL query files
                if file_name.endswith(('.json', '.csv', '.sql')):
                    mod_time = os.path.getmtime(file_path)
                    
                    # Stream changes if the file is brand new or modified
                    if file_name not in file_registry or mod_time > file_registry[file_name]:
                        file_registry[file_name] = mod_time
                        
                        # Direct to the proper engine
                        if file_name.endswith('.json'):
                            process_json(file_path)
                        elif file_name.endswith('.csv'):
                            process_csv(file_path)
                        elif file_name.endswith('.sql'):
                            process_live_sql_query(file_path)
                            
            time.sleep(1)
            
        except KeyboardInterrupt:
            print("\nStopping pipeline.")
            sys.exit()
        except Exception as e:
            print(f"❌ Core Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    setup_database()
    start_folder_watcher()