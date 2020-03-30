# Imports
import json
import os
import requests
import sched
import time
import pandas as pd
from datetime import datetime


def convert_timestamp_to_datetime(timestamp,
                                  return_format='%Y-%m-%d %H:%M:%S'):
    """
    Convert UNIX time to local time and return it as a string

    Parameters:
    timestamp: timestamp to convert to local time
    return_format: format of the string to be output by the function.
                default value is: '%Y-%m-%d %H:%M:%S'
    """

    return datetime.fromtimestamp(timestamp).strftime(return_format)


def convert_weatherdata_to_simpledict(weather_data):
    """
    Convert the data returned by Weather API into a dictionary
    :param weather_data: data extracted from OpenWeather
    :return: return a formatted dictionary
    """
    data_dict = {
        'extract_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'cityid': [weather_data['id']],
        'city': [weather_data['name']],
        'country': [weather_data['sys']['country']],
        'weather_id': [weather_data['weather'][0]['id']],
        'weather_description': [weather_data['weather'][0]['description']],
        'temp': [weather_data['main']['temp']],
        'feels_like': [weather_data['main']['feels_like']],
        'min_temp': [weather_data['main']['temp_min']],
        'max_temp': [weather_data['main']['temp_max']],
        'pressure': [weather_data['main']['pressure']],
        'humidity': [weather_data['main']['humidity']],
        'visibility': [weather_data['visibility']],
        'wind_speed': [weather_data['wind']['speed']],
        'wind_direction': [weather_data['wind']['deg']],
        'cloudiness': [weather_data['clouds']['all']],
        'record_timestamp': [weather_data['dt']],
        'record_datetime': convert_timestamp_to_datetime(weather_data['dt'])
    }
    return data_dict


def read_json_data(file_path):
    return pd.read_json(file_path, lines=True)


class Weather:
    # Constructor
    def __init__(self, cityid, openweather_key, json_path=None):
        """
        :param cityids: comma separated city ids from OpenWeather
        :param openweather_key: personal API key
        :param json_path[None]: the path where the data should be saved.
        If None, a file 'data.json' will be created in the current folder.

        The Weather constructor class will initiate a sched object which will
        be used for scheduling a data extraction from OpenWeather
        """
        self.cityid = cityid
        self.openweather_key = openweather_key
        if json_path is None:
            json_path = os.getcwd() + 'data.json'
        else:
            self.json_path = json_path
        self.sc = sched.scheduler(time.time, time.sleep)
        self.data = None
        self.complete_api_url = self.create_api_string()

    # Methods
    def create_api_string(self):
        """
        Create a string used to connect to OpenWeather API
        :return:
        """
        base_url = 'https://api.openweathermap.org/data/2.5/group?units=metric&'
        return base_url + 'id=' + self.cityid + '&appid=' + self.openweather_key

    def get_weather_record(self):
        """
        Call OpenWeather API and save the response to a local variable
        """
        response = requests.get(url=self.complete_api_url)
        self.data = response.json()

    def save_weather_record(self):
        """
        Call get_weather_method and save the resulting data to a local json file
        """
        self.get_weather_record()
        with open('data.json', 'a') as f:
            for city in self.data['list']:
                json.dump(convert_weatherdata_to_simpledict(city), f)
                f.write('\n')

    def scheduled_save(self, dt=600):
        """
        Run a scheduler to extract data from OpenWeather every dt
        :param dt: time delay, in seconds, between extracts.
        Minimum (and default) is 600 seconds (10 mins).
        """
        if dt < 600:
            dt = 600
        self.sc.enter(dt, 1, self.scheduled_save)
        self.save_weather_record()
        self.sc.run()
