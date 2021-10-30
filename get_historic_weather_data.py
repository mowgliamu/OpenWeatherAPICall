import os
import sys
import json
import calendar
import requests
import logging
import contextlib
import subprocess
import configparser
import http.client as http_client
from datetime import datetime
from http.client import HTTPConnection
from requests.adapters import HTTPAdapter
from logging.handlers import TimedRotatingFileHandler
from urllib3.util.retry import Retry

# Command line input
n_days = int(sys.argv[1])

# API Key for openweather
API_KEY = ''

# I/O Filenames
logFileName = 'logging_data'
output_filename = 'historic_weather_data.json'

# Create a custom requests object, modifying the global module throws an error
http = requests.Session()
http_client_logger = logging.getLogger("http.client")

# Timeout parameter
DEFAULT_TIMEOUT = 5 #seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


def print_to_log(*args):
    http_client_logger.debug(" ".join(args))


def debug_requests_on():
    '''Switches on logging of the requests module.'''

    HTTPConnection.debuglevel = 1

    fileHandler = TimedRotatingFileHandler(
        os.path.join(os.path.dirname(os.path.abspath('__file__')), logFileName + ".log"),
        when="midnight"
    )
    fileHandler.setLevel(logging.DEBUG)
    handlers = [fileHandler]

    logformat = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    logging.basicConfig(
        format=logformat, datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers, level=logging.DEBUG
    )
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def debug_requests_off():
    '''Switches off logging of the requests module, might be some side-effects'''

    HTTPConnection.debuglevel = 0

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    root_logger.handlers = []
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.WARNING)
    requests_log.propagate = False


@contextlib.contextmanager
def debug_requests():
    '''Use with 'with'!'''

    debug_requests_on()
    yield
    debug_requests_off()


def get_past_weather(api_key, lat, lon, days):
    '''Returns unprocessed json with weather data for preselected coordinates
    Used for historical forecasts.
    To get historical data we need send request indicating for what datetime we are making it.'''

    unixtime = calendar.timegm(datetime.utcnow().utctimetuple()) - days * 86400
    url = "https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={}&lon={}&units=metric&dt={}&appid={}"\
        .format(lat, lon, unixtime, api_key)

    # Requests GET call
    with debug_requests():
        api_request = http.get(url)

    return api_request.json()


def parse_hourly_weather(weather):
    '''Processing and normalising received json'''
    combined_entries = []

    created_at = str(datetime.utcnow())

    for i in range(len(weather['hourly'])):

        period_data = weather['hourly'][i]
        weather_json = {'lat': weather['lat'], 'lon': weather['lon']}

        # Formatting unixtime to readable time
        if 'dt' in period_data:
            timestamp = period_data['dt']
            #dt_object = datetime.fromtimestamp(timestamp)
            dt_object = datetime.utcfromtimestamp(timestamp)
            weather_json['dt'] = str(dt_object)

        # Adding main weather forecast elements
        elements_to_extract = [
            'temp', 'feels_like', 'pressure', 'humidity',
            'clouds', 'visibility', 'wind_speed', 'wind_deg']

        for element in elements_to_extract:
            if element in period_data:
                weather_json[element] = period_data[element]

        # Adding precipitation amount within 1 hour if available
        precipitations = ['rain', 'snow']
        for precipitation in precipitations:
            if precipitation in period_data:
                weather_json['rain_1h'] = period_data[precipitation]['1h']

        # Adding readable weather elements
        weather_sub = ['id', 'main', 'description']
        for sub_element in weather_sub:
            if 'weather' in period_data and sub_element in period_data['weather'][0]:
                weather_json['weather_'+sub_element] = period_data['weather'][0][sub_element]

        weather_json['weather_icon_url'] = \
            'http://openweathermap.org/img/wn/' + period_data['weather'][0]['icon'] + '@2x.png'

        # Data created at
        weather_json['created_at'] = created_at

        # Creating combined list of entries with forecast per hour
        combined_entries.append(weather_json)

    return combined_entries


def get_weather(location, lat, lon):
    '''Requesting and processing weather data for specified locations'''
    # Formated list of locations to be added to the python code.
    # Later will be replaced with a direct connection to bigquery

    all_entries_hourly = []

    # getting historical weather for past 5 days. Need to send 5 requests per location.
    for i in range(1, n_days+1):
        weather = get_past_weather(API_KEY, lat, lon, i)
        parsed_weather_hourly = parse_hourly_weather(weather) if 'hourly' in weather else []
        for entry in parsed_weather_hourly:
            entry['location'] = location

        all_entries_hourly += parsed_weather_hourly

    return all_entries_hourly


def get_json_newline_string():
    result = main(write_to_file=False)
    data = ''
    for row in result:
        data += json.dumps(row, indent=None) + "\n"
    return data


def main(write_to_file=True):

    # Requests Hooks and Exception handling
    assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
    http.hooks["response"] = [assert_status_hook]

    # Mount Timeout Adapter in conjunction with retry mechanism
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    http.mount("https://", TimeoutHTTPAdapter(timeout=DEFAULT_TIMEOUT, max_retries=retries))

    # monkey-patch a `print` global into the http.client module; all calls to
    # print() in that module will then use our print_to_log implementation
    http_client.print = print_to_log

    # Get lat/long data
    with open('location_data.txt') as finput:
        loc_data = finput.read().split()
        location = loc_data[0]
        latitude = float(loc_data[1])
        longitude = float(loc_data[2])

    all_data = get_weather(location, latitude, longitude)
    if write_to_file:
        with open(output_filename, 'w') as file_object:
            json.dump(all_data, file_object, indent=2)

    return all_data


if __name__ == '__main__':
    main()
