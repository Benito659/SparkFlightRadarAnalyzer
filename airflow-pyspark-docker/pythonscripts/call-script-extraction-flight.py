import requests

host = 'host.docker.internal'
port = 5000 
url = f'http://{host}:{port}/launchscriptextractionflight'
try:
    response = requests.get(url, timeout=3600)  
    if response.status_code == 200:
        print(f"Success! Response: {response.text}")
    else:
        raise ConnectionRefusedError
except requests.Timeout:
    print("Request timed out. Handle this accordingly.")
except requests.RequestException as e:
    print(f"An error occurred: {e}")



