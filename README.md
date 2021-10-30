## Goal

We want to retrieve the historic weather data for a given location using the openweather API.

## Input

A file named `location_data.txt` must be present in the working directory (path can be changed) which has the information about the latitude and longitude of the location you want to get the data for. The format `location latitude longitude`.

The program can be easily modified to add multiple locations in a single run!

## Execution

The python script is run to fetch the data from openweather API by running `python get_historic_weather_data.py ndays`, where ndays is number of days that we want the data for. This will generate an output file named `historic_weather_data.json`.

## Notes

1. A Retry mechanism has been added using the urllib3 retry functionality which is quite helpful in the event of a `ConnectionTimeout` or other HTTP errors. The number of retries can be controlled by the `max_retries` parameter, while the timeout parameter can be controlled by the `DEFAULT_TIMEOUT` parameter.

2. Logging has been used in context of http connection, and all the logs are stored in an output file `logging_data.log` which can be explored after the execution is completed.
