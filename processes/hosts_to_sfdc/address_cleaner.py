# -*- coding: utf-8 -*-
"""This is a very minimal wrapper for the Google Geocode API."""
import os
from requests import get

API_KEY = os.environ.get('GMAPS_API_KEY')


def address_resolver(json):
    """Return a denormailzed utf-8 encoded address."""
    final = {}
    if json['results']:
        data = json['results'][0]
        for item in data['address_components']:
            for category in item['types']:
                data[category] = {}
                data[category] = item['long_name']
        final['street'] = data.get("route", None)
        final['state'] = data.get("administrative_area_level_1", None)
        final['city'] = data.get("locality", None)
        final['county'] = data.get("administrative_area_level_2", None)
        final['country'] = data.get("country", None)
        final['postal_code'] = data.get("postal_code", None)
        final['neighborhood'] = data.get("neighborhood", None)
        final['sublocality'] = data.get("sublocality", None)
        final['housenumber'] = data.get("housenumber", None)
        final['postal_town'] = data.get("postal_town", None)
        final['subpremise'] = data.get("subpremise", None)
        final['latitude'] = \
            data.get("geometry", {}).get("location", {}).get("lat", None)
        final['longitude'] = \
            data.get("geometry", {}).get("location", {}).get("lng", None)
        final['location_type'] = \
            data.get("geometry", {}).get("location_type", None)
        final['postal_code_suffix'] = data.get("postal_code_suffix", None)
        final['street_number'] = data.get('street_number', None)
        final = {k: str(v).encode("utf-8") for k, v in final.items()}
    return final


def get_address_details(address):
    """Return a json geocoded address from Google given a string."""
    url = 'https://maps.googleapis.com/maps/api/geocode/json?' +\
          'components=&language=&region=&bounds=&key=' + API_KEY
    url = url + '&address=' + address.replace(" ", "+")
    response = get(url)
    data = address_resolver(response.json())
    data['address'] = address
    return data
