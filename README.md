# Real-Time Inventory Processing System

## Features
- Event-driven watchdog monitoring
- CSV validation
- Parameterized SQL queries
- SQL Server MERGE upsert
- Self-bootstrapping table creation
- last_updated audit column
- Graceful shutdown
- Logging

## SQL Server
Update the connection string to:
SERVER=localhost\SQLEXPRESS

## Run
pip install -r requirements.txt
python inventory_watchdog.py
