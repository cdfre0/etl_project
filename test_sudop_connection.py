import requests

def test_sudop_connection():
    """Tests the connection to the SUDOP API."""
    test_url = "https://api-sudop.uokik.gov.pl/sudop-api/slownik/forma-pomocy"
    print(f"Attempting to connect to: {test_url}")
    
    try:
        response = requests.get(test_url, timeout=10)
        response.raise_for_status()
        
        print(f"Successfully connected! Status Code: {response.status_code}")
        print("Response headers:")
        for key, value in response.headers.items():
            print(f"  {key}: {value}")
            
    except requests.exceptions.RequestException as e:
        print(f"Connection test failed: {e}")

if __name__ == "__main__":
    test_sudop_connection()
