import json
import http.client

def seed():
    with open("mock_transactions.json", "r") as f:
        data = f.read()
    
    conn = http.client.HTTPConnection("127.0.0.1", 8000)
    headers = {'Content-type': 'application/json'}
    conn.request("POST", "/upload", data, headers)
    response = conn.getresponse()
    print(f"Status: {response.status}, Reason: {response.reason}")
    print(response.read().decode())
    conn.close()

if __name__ == "__main__":
    seed()
