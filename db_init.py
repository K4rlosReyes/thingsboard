import sqlite3

conn = sqlite3.connect("telemetry.db")
cursor = conn.cursor()

# Crear tablas para cada parámetro de cada dispositivo
for device in filtered_devices:
    device_id = device.id.id
    device_details = rest_client.get_device_by_id(device_id)
    device_keys = device_details.additional_info.get("keys", [])

    for key in device_keys:
        table_name = f"telemetry_{device.name}_{key}"
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                value TEXT
            )
        """
        )

conn.commit()
conn.close()
