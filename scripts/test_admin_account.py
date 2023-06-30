import json
import random
import time

import requests

url = "https://thingsboard.cloud/api/v1/r9BSSkbzWtOc6NhQtGCl/telemetry"

while True:
    temperature = random.randint(10, 20)
    speed = random.randint(80, 100)

    data = {"temperature": temperature, "speed": speed}

    headers = {"Content-Type": "application/json"}

    response = requests.post(url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        print(
            "Datos aleatorios enviados correctamente: Temperatura =",
            temperature,
            "Velocidad =",
            speed,
        )
    else:
        print("Error al enviar los datos aleatorios:", response.status_code)

    time.sleep(3)
