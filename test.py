import pandas as pd
import numpy as np
import geocoder
import random
from time import sleep
import re, json
from datetime import datetime, timedelta
import sys, time
import os, math
import requests
from PyQt5.QtCore import QCoreApplication, Qt
from PyQt5.QtWidgets import QApplication, QTableWidgetItem, QTableWidget, QWidget, QMainWindow, QPushButton, QAction, QMessageBox, QVBoxLayout, QTabWidget, QFormLayout
from PyQt5.QtWidgets import QCalendarWidget, QFontDialog, QColorDialog, QTextEdit, QFileDialog, QComboBox
from PyQt5.QtWidgets import QCheckBox, QProgressBar, QComboBox, QLabel, QStyleFactory, QLineEdit, QInputDialog, QHBoxLayout, QGridLayout
from PyQt5.QtCore import pyqtSlot
import sqlite3
from sqlalchemy import create_engine
from pprint import pprint
from shapely.geometry import Point, Polygon
import urllib.parse
from pymongo import MongoClient
from decimal import Decimal as D


class P():
    @staticmethod
    def Google2Geo(address):
        '''

        :param address: string type
        :return: Return geo points (lng, lat)
        '''
        Google_geocode_api_url = "https://maps.googleapis.com/maps/api/geocode/json"
        api_keys = ['AIzaSyCJ69KvhuscmlIgr5IqyOideByOqJzZHcs', 'AIzaSyA-2V1w_acgbN4RO-40e2HJiwnzuMFtrrQ',
                    'AIzaSyA0b1WxrDmzoJFBuD6zua4CfVXJn1tvgko', 'AIzaSyB3K9wP-0U5EB2AeHsZIN4K5bk0MCBSW2s',
                    'AIzaSyD0OasuP_KjPwlSAc3kZrU8o4zLRh_bsrM', 'AIzaSyCMdT7Q3a178rNw6KqDt9jp8SSgud5V5gM',
                    'AIzaSyC-Axh7DKF4GGBkPYpOVrAP3IsAOpwRHkk', 'AIzaSyB_L0jCnP6hdPg8CDxIDnKp6YKGAZ7eQFM',
                    'AIzaSyAK5T_kCyb1r8aft0sRhy3KBZ1E5N4kcNM']
        random_key_index = random.randint(0, 8)
        params = {
            'address': address,
            'key': api_keys[random_key_index]
        }
        req = requests.get(Google_geocode_api_url, params=params)
        response = req.json()
        result = response['results']
        geometry = result[0]['geometry']['location']
        return geometry['lng'], geometry['lat']

    @staticmethod
    def clean_address(address):
        '''

        :param address: string type
        :return: clean address, remove apartment, floor, etc..
        '''
        if type(address) is float:
            return
        else:
            address = address.upper()
            address = address.replace(".", " ").split(" ")
            for i in address:
                if i in ['AV', 'AVE', 'AVENUE', 'BLD', 'BOULEVARD', 'BLVD', 'BLDG', 'BOWERY', 'BROADWAY', 'CI',
                         'CT', 'CIR', 'DR', 'DRIVER', 'EXP', 'EXPY', 'EXPRESSWAY', 'EXPWY', 'EXWY',
                         'HIGHWAY', 'HWY', 'LN', 'PL', 'PI', 'PARKWAY', 'PLACE', 'PLZ', 'PKWY', 'RD', 'ROAD',
                         'SQ', 'STR', 'ST', 'STREET', 'SQUARE', 'TNPK', 'TPKE', 'TURNPIKE', 'WAY', 'MALL']:
                    del address[address.index(i) + 1:]

            result = " ".join(address)
            result = re.sub(' +', " ", result)

            return result

    @staticmethod
    def google(func):
        def Google2Geo(address):
            '''

            :param address: string type
            :return: Return geo points (lng, lat)
            '''
            Google_geocode_api_url = "https://maps.googleapis.com/maps/api/geocode/json"
            api_keys = ['AIzaSyCJ69KvhuscmlIgr5IqyOideByOqJzZHcs', 'AIzaSyA-2V1w_acgbN4RO-40e2HJiwnzuMFtrrQ',
                        'AIzaSyA0b1WxrDmzoJFBuD6zua4CfVXJn1tvgko', 'AIzaSyB3K9wP-0U5EB2AeHsZIN4K5bk0MCBSW2s',
                        'AIzaSyD0OasuP_KjPwlSAc3kZrU8o4zLRh_bsrM', 'AIzaSyCMdT7Q3a178rNw6KqDt9jp8SSgud5V5gM',
                        'AIzaSyC-Axh7DKF4GGBkPYpOVrAP3IsAOpwRHkk', 'AIzaSyB_L0jCnP6hdPg8CDxIDnKp6YKGAZ7eQFM',
                        'AIzaSyAK5T_kCyb1r8aft0sRhy3KBZ1E5N4kcNM']
            random_key_index = random.randint(0, 8)
            params = {
                'address': address,
                'key': api_keys[random_key_index]
            }
            req = requests.get(Google_geocode_api_url, params=params)
            response = req.json()
            result = response['results']
            geometry = result[0]['geometry']['location']
            return geometry['lng'], geometry['lat']

        def func_wrapper(address):
            return func(Google2Geo(address))

        return func_wrapper

    @staticmethod
    def clean(func):
        def clean_address(address):
            '''

            :param address: string type
            :return: clean address, remove apartment, floor, etc..
            '''
            if type(address) is float:
                return
            else:
                address = address.upper()
                address = address.replace(".", " ").split(" ")
                for i in address:
                    if i in ['AV', 'AVE', 'AVENUE', 'BLD', 'BOULEVARD', 'BLVD', 'BLDG', 'BOWERY', 'BROADWAY', 'CI',
                             'CT', 'CIR', 'DR', 'DRIVER', 'EXP', 'EXPY', 'EXPRESSWAY', 'EXPWY', 'EXWY',
                             'HIGHWAY', 'HWY', 'LN', 'PL', 'PI', 'PARKWAY', 'PLACE', 'PLZ', 'PKWY', 'RD', 'ROAD',
                             'SQ', 'STR', 'ST', 'STREET', 'SQUARE', 'TNPK', 'TPKE', 'TURNPIKE', 'WAY', 'MALL']:
                        del address[address.index(i) + 1:]

                result = " ".join(address)
                result = re.sub(' +', " ", result)
                return result

        def func_wrapper(address):
            return func(clean_address(address))
        return func_wrapper


@P.clean
@P.google
def test2(address):
    return address
