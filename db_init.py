import sqlite3

# Creating database connection
conn = sqlite3.connect("telemetry.db")
cursor = conn.cursor()

# Creating a generic table for telemetry data
table_name = "telemetry_data"
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_name TEXT,
        timestamp TIMESTAMP,
        co2 FLOAT,
        temperatura FLOAT,
        humedad FLOAT,
        dispositivos INT
    )
    """.format(
        table_name=table_name
    )
)

conn.commit()
conn.close()
