import pyodbc

try:
    conn = pyodbc.connect(
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=.\SQLEXPRESS;"
        r"Trusted_Connection=yes;"
    )

    print("✅ Connected Successfully")

    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")

    print(cursor.fetchone()[0])

    conn.close()

except Exception as e:
    print("❌ Connection Failed")
    print(e)