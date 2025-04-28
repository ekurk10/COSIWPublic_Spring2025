"""
WattTime.py

This file defines functions useful for interacting with the WattTime API for collecting
or requesting carbon emission data.
"""
import requests
import os
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

BASE_ENDPOINT = 'https://api.watttime.org/'
ENV_FILE = 'credentials/WattTime.env'

def register(env_file):
    """
    Register a new WattTime user for the first time.

    Parameters:
    env_file (string): The path of the env file with WattTime registration details.

    Returns:
    Response: Response of the registration.
    """
    load_dotenv(env_file)

    register_url = BASE_ENDPOINT + 'register'
    params = {'username': os.getenv('USERNAME'),
            'password': os.getenv('PASSWORD'),
            'email': os.getenv('EMAIL'),
            'org': os.getenv('ORG')}
    
    response = requests.post(register_url, json=params)
    return response

def generate_token(env_file):
    """
    Generate a new WattTime API token.

    Parameters:
    env_file (string): The path of the env file with WattTime registration details.

    Returns:
    String: The new API token.

    Raises:
    HTTPError if the request failed
    """
    load_dotenv(env_file)

    login_url = BASE_ENDPOINT + 'login'
    response = requests.get(login_url, auth=HTTPBasicAuth(os.getenv('USERNAME'), os.getenv('PASSWORD')))
    response.raise_for_status()
    TOKEN = response.json()['token']

    return TOKEN

def determine_region(token, latitude, longitude, signal_type='co2_moer'):
    """
    Determine the WattTime region given coordinates.

    Parameters:
    token (string): WattTime API token.
    latitude (string): The latitude of the location in decimal form
    longitude (string): The longitude of the location in decimal form
    signal_type (string): The signal region. Won't need to change

    Returns:
    Response: The API response.

    Raises:
    HTTPError if the request failed
    """

    url = BASE_ENDPOINT + 'v3/region-from-loc'

    headers = {"Authorization": f"Bearer {token}"}
    params = {"latitude": latitude, "longitude": longitude, "signal_type": signal_type}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    return response

def get_forecast(token, region="CAISO_NORTH", signal_type="co2_moer", horizon_hours=24):
    """
    Return a WattTime forecast for the given region.

    Parameters:
    token (string): WattTime API token.
    region (string): The signal region
    signal_type (string): The signal type. Won't need to change
    horizon_hours (integer): The length of the requested forecast

    Returns:
    Response: The API response.

    Raises:
    HTTPError if the request failed
    """

    url = BASE_ENDPOINT + 'v3/forecast'

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "region": region,
        "signal_type": signal_type,
        "horizon_hours": horizon_hours
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response

def get_historical(token, start, end, region="CAISO_NORTH", signal_type="co2_moer"):
    """
    Return WattTime historical data for the given region.

    Parameters:
    token (string): WattTime API token.
    start (string): ISO8601-compliant timezone
    end (string): ISO8601-compliant timezone
    region (string): The signal region
    signal_type (string): The signal region. Won't need to change

    Returns:
    Response: The API response.

    Raises:
    HTTPError if the request failed
    """

    url = BASE_ENDPOINT + 'v3/historical'

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "region": region,
        "start": start,
        "end": end,
        "signal_type": signal_type,
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    return response

def get_current(token, region, signal_type="co2_moer"):
    """
    Return WattTime current co2 index for the given region.

    Parameters:
    token (string): WattTime API token.
    region (string): The signal region
    signal_type (string): The signal region. Won't need to change

    Returns:
    Response: The API response.

    Raises:
    HTTPError if the request failed
    """

    url = BASE_ENDPOINT + 'v3/signal-index'

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "region": region,
        "signal_type": signal_type,
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    return response

if __name__ == "__main__":
    # Uncomment if registering
    # register()

    # Make some API Calls
    token = generate_token(ENV_FILE)
    response = determine_region(token, "37.3719", "-79.8164")
    print(response.json()["region"])

    #response = get_forecast(token)
    #print(response.json())

    #response = get_historical(token, "2025-04-15T00:00Z", "2025-04-15T00:05Z")
    #print(response.json())

    #response = get_current(token, "FR")
    #print(response.json())
