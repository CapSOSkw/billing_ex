'''
  @Author: Keyuan
  @05/17/2018
'''

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
from functools import wraps
from ast import literal_eval
from collections import Counter, OrderedDict
import arrow
import xml.etree.ElementTree as ET
import logging
pd.options.mode.chained_assignment = None
logging.getLogger().setLevel(logging.INFO)      # Set logging level to logging.INFO, otherwise would only display warning level.


def CleanAddress(func):

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

    @wraps(func)
    def wrapper(*args):
        return func(clean_address(*args))
    return wrapper


def Google(func):
    def Google2Geo(address):
        '''

        :param address: string type
        :return: Return geo points (lng, lat)
        '''
        sq = Sqlite_Methods('ProcedureCodes.db')
        if_address_in_cache = sq.check_address_in_cache('address_cache_test', address)
        if if_address_in_cache != None:

            return if_address_in_cache[0], if_address_in_cache[1]
        else:

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

    @wraps(func)
    def wrapper(*args):
        return func(Google2Geo(*args))
    return wrapper


class info_locker(object):

    driver_information = None
    base_info = None

    NYSDOH = {
        'name': 'NYSDOH',
        'ETIN': 'EMEDNYBAT',
        'id': '141797357',
        'address': 'CORNING TOWER EMPIRE STATE PLAZA',
        'city': 'ALBANY',
        'state': 'NY',
        'zipcode': '12237',

    }

    version_code = {
        '837': '005010X222A1',
        '276': '005010X212',
        '270': '005010X279A1'
    }


class Process_Methods():
    '''
    Some basic processing functions
    '''
    def __init__(self):
        pass

    @staticmethod
    def _Google2Geo(address):
        '''

        :param address: string type
        :return: Return geo points (lng, lat)
        '''
        sq = Sqlite_Methods('ProcedureCodes.db')
        if_address_in_cache = sq.check_address_in_cache('address_cache_test', address)
        if if_address_in_cache != None:

            return if_address_in_cache[0], if_address_in_cache[1]
        else:

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
    def _clean_address(address):
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
    @CleanAddress
    def clean_address(address):
        '''Clean address by removing noise and unwanted information'''
        return address

    @staticmethod
    @Google
    def google_address(address):
        '''Use google geocoding API to get address geo-location'''
        return address

    @staticmethod
    def getPolygonIDs(address):
        sq = Sqlite_Methods('ProcedureCodes.db')
        if_address_in_cache = sq.check_address_in_cache('address_cache_test', address)

        if if_address_in_cache != None:
            '''If address is in the cache, then directly get the polygonIds.
            '''
            result = if_address_in_cache[2]
            anti_process_result = [int(i) for i in result.split(',')]
            return anti_process_result

        else:
            '''
            If address is not in the cache, then use MongoDB_Methods() to get polygonIds.
            Then store this address into cache.
            '''
            lng, lat = Process_Methods.google_address(address)
            # print(lng, lat)
            result = MongoDB_Methods(localhost=True).getPolygonID(lng=lng, lat=lat)
            # print(result)
            process_result = ','.join(map(str, result))
            sq.upsert_address_cache('address_cache_test', address, lng, lat, process_result)
            print(f'HOUSTON! {address} IS RECORDED!')
            return result

    @staticmethod
    def frange(start, end, step):
        '''
        Float range function.
        :param start:
        :param end:
        :param step:
        :return:
        '''
        while start < end:
            yield float('{0:.2f}'.format(start))
            start += step

    @staticmethod
    def generate_procedureCodes(dataframe, mileage, pickup_poly, dropoff_poly, **kwargs):
        '''
        Base on rules in database to generate corresponding codes
        :param dataframe: Commonly call from 'get_procedureCode_Rule_to_df()'
        :param kwargs: mileage, pickup_polygonIDs, dropoff_polygonIDs, etc.
        :return: 1. A list, the codes should be used in this trip.
                2. A dictionary, it contains code names and modifiers
        '''

        result = []
        res_to_dict = {}
        for r in range(len(dataframe)):
            row = dataframe.ix[r, :]
            mileage_start = row['Mileage_Start']
            mileage_end = row['Mileage_End']
            pickup = row['polygonID_pickup']
            dropoff = row['polygonID_dropoff']

            # Rule 1 considers mileage.
            Rule1 = mileage in Process_Methods().frange(mileage_start, mileage_end, 0.01) if mileage_end > 0 else mileage > mileage_start
            if not Rule1:
                continue

            # Rule 2 considers pickup polygon.
            if pickup == 0:
                Rule2 = True
            elif pickup > 0:
                Rule2 = True if pickup in pickup_poly else False
            else:
                Rule2 = True if -pickup not in pickup_poly else False
            if not Rule2:
                continue

            # Rule 3 considers dropoff polygon.
            if dropoff == 0:
                Rule3 = True
            elif dropoff > 0:
                Rule3 = True if dropoff in dropoff_poly else False
            else:
                Rule3 = True if -dropoff not in dropoff_poly else False
            if not Rule3:
                continue

            # Other Rules #

            result.append(row['CodeName'])
            res_to_dict[row['CodeName']] = {
                'Code': row['Code'],
                'Modifier': row['CodeModifier']
            }

        return result, res_to_dict

    @staticmethod
    def use_driver_id_to_find_drivername(driverid):

        for key, value in info_locker.driver_information.items():

            if value['DRIVER_ID'] == driverid:
                return value['FirstName'], value['LastName']

    @staticmethod
    def write_to_txt(data, output_file):
        with open(output_file, 'w') as f:
            f.write(data)

    @staticmethod
    def df2txt(input_file, output_name, delimiter='\t', fmt='%s'):
        if isinstance(input_file, pd.DataFrame):
            df = input_file
        elif input_file.endswith('xlsx'):
            df = pd.read_excel(input_file)
        elif input_file.endswith('csv'):
            df = pd.read_csv(input_file)
        else:
            raise TypeError(f'The type of file: {input_file} is not supported! \n     Only support ".xlsx", ".csv" & "dataframe"!')

        np.savetxt(output_name, df, delimiter=delimiter, fmt=fmt)

    @staticmethod
    def string2bin(text):
        return " ".join(format(ord(i), 'b') for i in text)

    @staticmethod
    def string2hex(text):
        return "".join(format(ord(i),'02X') for i in text)

    @staticmethod
    def hex2string(text):
        return bytearray.fromhex(text).decode()

    @staticmethod
    def generate_837(file, delay_claim):
        edi = EDI837P(file, delay_claim)
        stream_837data = edi.ISA_IEA()
        filename = edi.file_name + '.txt'

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        Process_Methods.write_to_txt(stream_837data, os.path.join(file_saving_path, filename))

    @staticmethod
    def generate_270(MAS_raw_file):
        '''
        function checked on 06/13/2018.
        :param MAS_raw_file:
        :return:
        '''
        raw_df = pd.read_table(MAS_raw_file) if MAS_raw_file.endswith('txt') else pd.read_csv(MAS_raw_file)

        result_df = pd.DataFrame()
        unique_invoice = raw_df["Invoice Number"].unique().tolist()
        invoice_number_list = []
        service_name = []
        service_npi = []
        client_lastname = []
        client_firstname = []
        CIN = []
        gender = []
        dob = []
        service_date = []

        for invoice_number in unique_invoice:
            idx = raw_df.loc[
                (raw_df['Invoice Number'] == invoice_number) & (raw_df['Record Type'] == 'Leg')].index.tolist()

            invoice_number_list.append(invoice_number)
            service_name.append(raw_df.ix[idx[0], 'Medical Provider'].upper())
            service_npi.append(int(raw_df.ix[idx[0], 'Ordering Provider ID']))
            client_lastname.append(raw_df.ix[idx[0], 'Last Name'].upper())
            client_firstname.append(raw_df.ix[idx[0], 'First Name'].upper())
            CIN.append(raw_df.ix[idx[0], 'CIN'])
            gender.append(raw_df.ix[idx[0], 'Gender'])
            dob.append(raw_df.ix[idx[0], 'Birthdate'])
            service_date.append(raw_df.ix[idx[0], 'Service Starts'])

        result_df['INVOICE NUMBER'] = invoice_number_list
        result_df['SVC NAME'] = service_name
        result_df['SVC NPI'] = service_npi
        result_df['CLIENT LAST NAME'] = client_lastname
        result_df['CLIENT FIRST NAME'] = client_firstname
        result_df['MEDICAID ID NUMBER'] = CIN
        result_df['GENDER'] = gender
        result_df['DOB'] = dob
        result_df['SVC DATE'] = service_date

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        result_df.to_excel(os.path.join(file_saving_path, '270-data-' + str(datetime.today().date()) + str(datetime.now().time().strftime('%H%M%S')) + '.xlsx'),
                           index=False)

        edi = EDI270(result_df)
        stream_270_data = edi.ISA_IEA()
        filename = edi.file_name

        Process_Methods.write_to_txt(stream_270_data, os.path.join(file_saving_path, filename))

    @staticmethod
    def process_271(receipt_file, lined_file=False):
        '''
        function checked on 06/13/2018.
        :param receipt_file:
        :param lined_file:
        :return:
        '''

        SQ =Sqlite_Methods('EDI.db')  # connect to sqlite3

        if lined_file == False:  # for raw receipt data
            receipt_df = pd.read_csv(receipt_file, delimiter="~", header=None, )
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna()
        else:  # already split into lines
            receipt_df = pd.read_csv(receipt_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))
        # print(receipt_df)

        result_dict = {}
        temp_dict = {}
        invoice_num = ""
        patient_lastname = ""
        patient_firstname = ""
        CIN = ""
        dob = ""
        gender = ""
        service_date = ""
        eligible_code = ""
        payer_name = ""
        plan_code = ""
        address = ""
        # contact_name = ""
        contact_tel = ""
        covered_service_codes = []
        other_payer_name = []
        other_payer_address = []
        other_payer_policy_number = ""
        other_payer_telephone = []
        other_payer_group_number = []

        other_payer_name1 = ""
        other_payer_address1 = ""
        other_payer_telephone1 = ""
        other_payer_group_number1 = ""
        other_payer_name2 = ""
        other_payer_address2 = ""
        other_payer_telephone2 = ""
        other_payer_group_number2 = ""
        eligible_result = ""

        for l in range(receipt_df.__len__()):
            row = receipt_df.ix[l, 1]

            if row[0] == 'BHT':
                invoice_num = row[3]

            elif row[0] == 'AAA' and row[1] == 'N':
                if row[3] == '15':
                    eligible_code = 'Required Application Data Missing'
                elif row[3] == '35':
                    eligible_code = 'Out of Network'
                elif row[3] == '42':
                    eligible_code = 'Unable to respond at current time'
                elif row[3] == '43':
                    eligible_code = 'Invalid/missing provider ID'
                elif row[3] == '45':
                    eligible_code = 'Invalid/missing provider specialty'
                elif row[3] == '47':
                    eligible_code = 'Invalid/missing provider state'
                elif row[3] == '48':
                    eligible_code = 'Invalid/missing referring provider ID number'
                elif row[3] == '49':
                    eligible_code = 'Provider is not primary care physician'
                elif row[3] == '51':
                    eligible_code = 'Provider not on file'
                elif row[3] == '52':
                    eligible_code = 'Service dates not within provider plan enrollment'
                elif row[3] == '56':
                    eligible_code = 'Inappropriate date'
                elif row[3] == '57':
                    eligible_code = 'Invalid/missing date of service'
                elif row[3] == '58':
                    eligible_code = 'Invalid/missing date of birth'
                elif row[3] == '60':
                    eligible_code = 'Date of birth follows date of service'
                elif row[3] == '61':
                    eligible_code = 'Date of death precedes date of service'
                elif row[3] == '62':
                    eligible_code = 'Date of service not within allowable inquiry period'
                elif row[3] == '63':
                    eligible_code = 'date of service in future'
                elif row[3] == '71':
                    eligible_code = 'Patient date of birth does not match patient in database'
                elif row[3] == '72':
                    eligible_code = 'Invalid/missing subscriber/insured ID'
                elif row[3] == '73':
                    eligible_code = 'Invalid/missing subscriber/insured name'
                elif row[3] == '74':
                    eligible_code = 'Invalid/missing subscriber/insured gender code'
                elif row[3] == '75':
                    eligible_code = 'Subscriber/insurer not found'
                elif row[3] == '76':
                    eligible_code = 'Duplicate subscriber/insurer ID number'
                elif row[3] == '78':
                    eligible_code = 'Subscriber/insured not in group/plan identified'
                else:
                    eligible_code = "AAA"

            elif row[0] == 'NM1' and row[1] == 'IL':
                patient_lastname, patient_firstname, CIN = row[3], row[4], row[9]

            elif row[0] == 'DMG' and row[1] == 'D8':
                dob = row[2]
                dob = datetime.strptime(dob, '%Y%m%d').date()
                gender = row[3]

            elif row[0] == 'DTP' and row[1] == '472':
                service_date = row[3]
                service_date = datetime.strptime(service_date, '%Y%m%d').date()

            elif (row[0] == 'EB' and row[1] == 'U' and row[2] == 'IND' and row[3] == '30') or \
                    (row[0] == 'EB' and row[1] == '1' and row[2] == 'IND' and row[3] == '30'):
                eligible_code = row[5]

            elif row[0] == 'EB' and row[1] == '6':
                eligible_code = 'Inactive'

            elif row[0] == 'NM1' and row[1] == 'Y2':
                payer_name = row[3]
                plan_code = row[9]

                address_row1 = receipt_df.ix[l + 1, 1]  # N3
                address_row2 = receipt_df.ix[l + 2, 1]  # N4
                contact_row = receipt_df.ix[l + 3, 1]  # PER

                address = address_row1[1] + " " + address_row2[1] + " " + address_row2[2] + " " + address_row2[3]
                # contact_name = contact_row[2]
                try:
                    contact_tel = contact_row[4]
                except:
                    contact_tel = '0000000000'

            elif row[0] == 'NM1' and row[1] == 'P4':
                other_payer_name.append(row[3])
                try:
                    other_payer_group_number.append(row[9])
                except:
                    other_payer_group_number.append("")

                N3_row = receipt_df.ix[l + 1, 1]
                N4_row = receipt_df.ix[l + 2, 1]
                PER_row = receipt_df.ix[l + 3, 1]

                if N3_row[0] == "N3" and N4_row[0] == "N4":
                    other_payer_address.append(N3_row[1] + " " + N4_row[1] + " " + N4_row[2] + " " + N4_row[3])
                    if PER_row[0] == 'PER' and PER_row[1] == 'IC':
                        other_payer_tel_tmp = PER_row[4]
                    else:
                        other_payer_tel_tmp = "0"

                    other_payer_telephone.append(other_payer_tel_tmp)
                else:
                    other_payer_address.append("")
                    other_payer_telephone.append("")

            elif row[0] == 'REF' and row[1] == '18':
                other_payer_policy_number = row[2]

            elif row[0] == 'EB' and row[1] == '1' and row[2] == 'IND':
                covered_service_codes.append(str(row[3]))

            if other_payer_name.__len__() == 1:
                other_payer_name1 = other_payer_name[0]
                other_payer_address1 = other_payer_address[0]
                other_payer_telephone1 = other_payer_telephone[0]
                other_payer_group_number1 = other_payer_group_number[0]

            if other_payer_name.__len__() == 2:
                other_payer_name2 = other_payer_name[1]
                other_payer_address2 = other_payer_address[1]
                other_payer_telephone2 = other_payer_telephone[1]
                other_payer_group_number2 = other_payer_group_number[1]

            temp_dict[str(invoice_num)] = {'Invoice number': invoice_num,
                                           'Patient lastname': patient_lastname,
                                           'Patient firstname': patient_firstname,
                                           'Patient DOB': dob,
                                           'Patient gender': gender,
                                           'CIN': CIN,
                                           'Service date': service_date,
                                           'Eligible': eligible_code,
                                           'Payer name': payer_name,
                                           'Payer address': address,
                                           # 'Contact name': contact_name,
                                           'Contact Tel.': contact_tel,
                                           'Plan code': plan_code,
                                           'Covered Codes': str(covered_service_codes),

                                           'Other Payer1 name': other_payer_name1,
                                           'Other Payer1 address': other_payer_address1,
                                           'Other Payer1 tel.': other_payer_telephone1,
                                           'Other Payer1 group number': other_payer_group_number1,
                                           'Other Payer2 name': other_payer_name2,
                                           'Other Payer2 address': other_payer_address2,
                                           'Other Payer2 tel.': other_payer_telephone2,
                                           'Other Payer2 group number': other_payer_group_number2,
                                           'Other Payer policy number': other_payer_policy_number,
                                           }

            if row[0] == 'SE':  # section ends
                # print(temp_dict)

                ifPlanCodeInDB = SQ.IfplancodeInDB(table='PlanCodeLib', plancode=plan_code)
                if ifPlanCodeInDB:
                    eligible_result = "OKAY"
                elif eligible_code in ["ELIGIBLE PCP", "Community Coverage w/CBLTC",
                                       "EP - Family Planning and Non Emerg Trans Only", 'MA Eligible',
                                       'Eligible Only Outpatient Care', 'Community Coverage No LTC',
                                       'Outpatient Coverage w/ CBLTC'] and ifPlanCodeInDB == False:
                    eligible_result = "OKAY"
                else:
                    eligible_result = 'PENDING'

                temp_dict[str(invoice_num)] = {'Invoice number': invoice_num,
                                               'Eligibility Result': eligible_result,
                                               'Patient lastname': patient_lastname,
                                               'Patient firstname': patient_firstname,
                                               'Patient DOB': dob,
                                               'Patient gender': gender,
                                               'CIN': CIN,
                                               'Service date': service_date,
                                               'Eligible': eligible_code,
                                               'Payer name': payer_name,
                                               'Payer address': address,
                                               # 'Contact name': contact_name,
                                               'Contact Tel.': contact_tel,
                                               'Plan code': plan_code,
                                               'Covered Codes': str(covered_service_codes),

                                               'Other Payer1 name': other_payer_name1,
                                               'Other Payer1 address': other_payer_address1,
                                               'Other Payer1 tel.': other_payer_telephone1,
                                               'Other Payer1 group number': other_payer_group_number1,
                                               'Other Payer2 name': other_payer_name2,
                                               'Other Payer2 address': other_payer_address2,
                                               'Other Payer2 tel.': other_payer_telephone2,
                                               'Other Payer2 group number': other_payer_group_number2,
                                               'Other Payer policy number': other_payer_policy_number,
                                               }

                result_dict.update(temp_dict)
                # Reset var.
                invoice_num = ""
                patient_lastname = ""
                patient_firstname = ""
                CIN = ""
                dob = ""
                gender = ""
                service_date = ""
                eligible_code = ""
                payer_name = ""
                plan_code = ""
                address = ""
                # contact_name = ""
                contact_tel = ""
                other_payer_name1 = ""
                other_payer_address1 = ""
                other_payer_telephone1 = ""
                other_payer_group_number1 = ""
                other_payer_name2 = ""
                other_payer_address2 = ""
                other_payer_telephone2 = ""
                other_payer_group_number2 = ""

                other_payer_name = []
                other_payer_address = []
                other_payer_policy_number = ""
                eligible_result = ""
                other_payer_telephone = []
                other_payer_group_number = []

                covered_service_codes = []
                temp_dict = {}

        result_dict.pop("")
        result_df = pd.DataFrame(result_dict)
        result_df = result_df.transpose()
        result_df = result_df[
            ['Invoice number', 'Eligibility Result', 'Service date', 'Patient firstname', 'Patient lastname',
             'Plan code', 'Eligible', 'CIN', 'Covered Codes', 'Patient DOB', 'Patient gender',
             'Payer name', 'Payer address', 'Contact Tel.', 'Other Payer1 name', 'Other Payer1 address',
             'Other Payer1 tel.', 'Other Payer1 group number',
             'Other Payer2 name', 'Other Payer2 address', 'Other Payer2 tel.', 'Other Payer2 group number',
             'Other Payer policy number']]

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        file_name_271 = str(datetime.today().date()) + str(datetime.now().time().strftime("%H%M%S"))
        result_df.to_excel(os.path.join(file_saving_path,'271-' + file_name_271 + '.xlsx'), index=False)

        SQ.upsert271(table='Eligibility271', data=result_df)   # Store all 271 eligibility result in Sqlite3
        PendingFromdf = result_df.loc[result_df['Eligibility Result'] == 'PENDING']
        PendingFromdf_cinList = PendingFromdf['CIN'].tolist()
        manual_df = SQ.generate_excel_from_manually271Lib(table='ManuallyCheck271', tofile=False)
        if manual_df.__len__() == 0:
            print("No records in database!")
        else:
            eligibleFrom_manual_df_cinList = manual_df.loc[manual_df['Eligible'] == 'Eligible', 'CIN'].tolist()
            if eligibleFrom_manual_df_cinList.__len__() != 0:
                for pendingCIN in PendingFromdf_cinList:
                    if pendingCIN in eligibleFrom_manual_df_cinList:
                        pendingCIN_idx = PendingFromdf.loc[PendingFromdf['CIN'] == pendingCIN].index.tolist()
                        PendingFromdf = PendingFromdf.drop(index=pendingCIN_idx)

            else:
                pass

            PendingFromdf.to_excel(os.path.join(file_saving_path, '271-Not eligible Trips' + file_name_271 + '.xlsx'),
                                   index=False)

    @staticmethod
    def generate_276(receipt837_file, edi837_data, lined_file=False):
        df_837 = pd.read_csv(edi837_data) if edi837_data[-1] == 'v' else pd.read_excel(edi837_data)

        if lined_file == False:  # for raw receipt data
            receipt_df = pd.read_csv(receipt837_file, delimiter="~", header=None, )
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna()
        else:  # already split into lines
            receipt_df = pd.read_csv(receipt837_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))

        result_dict = {}
        temp_dict = {}

        invoice_num = ""
        patient_lastname = ""
        patient_firstname = ""
        CIN = ""
        claim_number = ""
        service_date = ""

        for l in range(receipt_df.__len__()):
            row = receipt_df.ix[l, 1]

            if row[0] == 'BHT':
                invoice_num = row[3]

            elif row[0] == 'NM1' and row[1] == 'QC' and row[2] == '1':
                patient_lastname = row[3]
                patient_firstname = row[4]
                CIN = row[9]

            elif row[0] == 'REF' and row[1] == '1K':
                claim_number = row[2]

            elif row[0] == 'DTP' and row[1] == '472':
                service_date = row[3]

            temp_dict[str(invoice_num)] = {
                'INVOICE NUMBER': invoice_num,
                'CLIENT LAST NAME': patient_lastname,
                'CLIENT FIRST NAME': patient_firstname,
                'MEDICAID ID NUMBER': CIN,
                'CLAIM CONTROL NUMBER': claim_number,
                'SVC DATE': service_date,
            }

            if row[0] == 'SE':  # section ends
                idx_837 = df_837.loc[df_837['invoice number'] == int(invoice_num)].index.tolist()

                if idx_837.__len__() == 0:
                    patient_dob = "00001225"
                    patient_gender = "M"
                else:
                    patient_dob = df_837.ix[idx_837[0], 'patient dob']
                    patient_dob = datetime.strptime(str(patient_dob), '%m/%d/%Y').strftime('%Y%m%d')
                    patient_gender = df_837.ix[idx_837[0], 'patient gender']

                temp_dict[str(invoice_num)] = {
                    'INVOICE NUMBER': invoice_num,
                    'CLIENT LAST NAME': patient_lastname,
                    'CLIENT FIRST NAME': patient_firstname,
                    'MEDICAID ID NUMBER': CIN,
                    'CLAIM CONTROL NUMBER': claim_number,
                    'SVC DATE': service_date,
                    'GENDER': patient_gender,
                    'DOB': patient_dob
                }

                result_dict.update(temp_dict)
                invoice_num = ""
                patient_lastname = ""
                patient_firstname = ""
                CIN = ""
                claim_number = ""
                service_date = ""

        result_dict.pop("")
        result_df = pd.DataFrame(result_dict)
        result_df = result_df.transpose()
        result_df = result_df[['INVOICE NUMBER', 'DOB', 'GENDER', 'CLIENT LAST NAME',
                               'CLIENT FIRST NAME', 'MEDICAID ID NUMBER', 'CLAIM CONTROL NUMBER', 'SVC DATE']]

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        file_name_276data = str(datetime.today().date()) + str(datetime.now().time().strftime("%H%M%S"))

        result_df.to_excel(os.path.join(file_saving_path, '276-data-' + file_name_276data + '.xlsx'), index=False)

        edi = EDI276(result_df)
        stream_276_data = edi.ISA_IEA()
        filename = edi.file_name
        Process_Methods.write_to_txt(stream_276_data, os.path.join(file_saving_path, filename))

    @staticmethod
    def process_276_receipt(receipt_file, edi837=None, lined_file=False):
        if lined_file==False:    # for raw receipt data
            receipt_df = pd.read_csv(receipt_file, delimiter="~", header=None,)
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna().apply(lambda x: x)
        else:          # already split into lines
            receipt_df = pd.read_csv(receipt_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))

        if edi837:
            edi837_df = pd.read_excel(edi837)
            edi837_df['invoice number'] = edi837_df['invoice number'].astype(str)

        SQ = Sqlite_Methods('ProcedureCodes.db')

        result_dict = {}
        temp_dict = {}

        invoice_num = ""
        patient_lastname = ""
        patient_firstname = ""
        CIN = ""
        total_expected_amt = ""
        total_paid_amt = ""
        claim_ctrl_num = ""
        service_date = ""
        embedded_code_dict = {}
        description = []
        error_codes = []
        result = ''
        status_code = ""
        status_code2 = ""

        for l in range(receipt_df.__len__()):
            row = receipt_df.ix[l, 1]

            if row[0] == 'BHT':
                invoice_num = row[3]

            elif row[0] == 'NM1' and row[1] == 'IL' and row[2] == '1':
                patient_lastname = row[3]
                patient_firstname = row[4]
                CIN = row[9]

            elif row[0] == 'TRN' and row[1] == '2':
                next_row = receipt_df.ix[l + 1, 1]
                total_expected_amt = float(next_row[4])
                total_paid_amt = float(next_row[5])
                status_code = next_row[1]

                for code in status_code.split(":"):
                    SQ.cursor.execute(f'SELECT description FROM X12_external_code WHERE Code="{code}"')
                    resp = SQ.cursor.fetchone()
                    if resp != None:
                        description_status_code = f' -{resp[0]}; '
                    else:
                        description_status_code = ""


                if (abs(total_expected_amt - total_paid_amt) <= 0.02) and (total_expected_amt != 0):
                    result = 'Paid'
                elif total_paid_amt == 0:
                    if total_expected_amt == 0:
                        result = 'Error' + description_status_code
                    else:
                        result = 'Denied' + description_status_code
                else:
                    result = 'Partial Paid ' + description_status_code

            elif row[0] == 'REF' and row[1] == '1K':
                claim_ctrl_num = row[2]

            elif row[0] == 'DTP' and row[1] == '472' and row[2] == 'RD8':
                service_date = row[3]

            elif row[0] == 'SVC':
                service_code = row[1]
                expected_amount = float(row[2])
                paid_amount = float(row[3])
                service_code = "".join(service_code.split(":")[1:])
                embedded_code_dict[service_code] = {'Expected': expected_amount,
                                                    'Paid': paid_amount}
                if expected_amount != paid_amount:
                    error_codes.append(service_code)

                next_row = receipt_df.ix[l+1, 1]
                status_code2 = next_row[1]

                temp_description = []
                for code in status_code2.split(":"):
                    SQ.cursor.execute(f'SELECT description FROM X12_external_code WHERE Code="{code}"')
                    resp = SQ.cursor.fetchone()
                    if resp != None:
                        temp_description.append(resp[0])
                description += temp_description

            if row[0] == 'SE':

                description = list(set(description))
                if description.__len__() != 0:
                    result = result + '-' + '; '.join(description)

                if error_codes.__len__() != 0:
                    result += " (Wrong Codes: {0})".format(",".join(error_codes))

                if edi837:
                    npi_temp = edi837_df.loc[edi837_df['invoice number'] == invoice_num, 'service npi'].tolist()
                    NPI = int(npi_temp[0]) if len(npi_temp) != 0 else 0

                    driver_temp = edi837_df.loc[edi837_df['invoice number'] == invoice_num, 'driver license number'].tolist()
                    Driver_id = int(driver_temp[0]) if len(driver_temp) != 0 else 0

                    vehicle_temp = edi837_df.loc[edi837_df['invoice number'] == invoice_num, 'driver plate number'].tolist()
                    Vehicle_id = vehicle_temp[0] if len(vehicle_temp) != 0 else ""

                    claim_amount_temp = edi837_df.loc[
                        edi837_df['invoice number'] == invoice_num, 'claim_amount'].tolist()
                    claim_amount = float(claim_amount_temp[0]) if len(claim_amount_temp) != 0 else 0

                    if total_paid_amt >= claim_amount:
                        result = 'Paid'

                    temp_dict[str(invoice_num)] = {
                        'Invoice Number': invoice_num,
                        'Patient Lastname': patient_lastname,
                        'Patient Firstname': patient_firstname,
                        'CIN': CIN,
                        'Claim Ctrl Number': claim_ctrl_num,
                        'P1 Total Expected Amt': claim_amount,
                        'P2 Total Expected Amt': total_expected_amt,
                        'Total Paid Amt': total_paid_amt,
                        'Service Date': service_date,
                        'Comparison Codes': str(embedded_code_dict),
                        'Result': result,
                        'NPI': NPI,
                        'DRIVER ID': Driver_id,
                        'VEHICLE ID': Vehicle_id
                    }

                else:
                    temp_dict[str(invoice_num)] = {
                        'Invoice Number': invoice_num,
                        'Patient Lastname': patient_lastname,
                        'Patient Firstname': patient_firstname,
                        'CIN': CIN,
                        'Claim Ctrl Number': claim_ctrl_num,
                        'P2 Total Expected Amt': total_expected_amt,
                        'Total Paid Amt': total_paid_amt,
                        'Service Date': service_date,
                        'Comparison Codes': str(embedded_code_dict),
                        'Result': result,
                    }

                result_dict.update(temp_dict)

                invoice_num = ""
                patient_lastname = ""
                patient_firstname = ""
                CIN = ""
                total_expected_amt = ""
                total_paid_amt = ""
                claim_ctrl_num = ""
                service_date = ""
                embedded_code_dict = {}
                description = []
                error_codes = []
                result = ''
                status_code = ""
                status_code2 = ""

        result_df = pd.DataFrame(result_dict)
        result_df = result_df.transpose()
        if edi837:
            result_df = result_df[['Invoice Number', 'Result', 'P1 Total Expected Amt', 'P2 Total Expected Amt', 'Total Paid Amt', 'Comparison Codes',
                               'Patient Lastname', 'Patient Firstname', 'CIN', 'Claim Ctrl Number', 'Service Date', 'NPI', 'DRIVER ID', 'VEHICLE ID']]

        else:
            result_df = result_df[
                ['Invoice Number', 'Result', 'P2 Total Expected Amt', 'Total Paid Amt',
                 'Comparison Codes',
                 'Patient Lastname', 'Patient Firstname', 'CIN', 'Claim Ctrl Number', 'Service Date']]

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        file_name_276277 = str(datetime.today().date()) + str(datetime.now().time().strftime("%H%M%S"))

        result_df.to_excel(os.path.join(file_saving_path, '276-277-' + file_name_276277 + '.xlsx'),
                           index=False)

    @staticmethod
    def process_835(receipt_file, lined_file=False):
        if lined_file==False:    # for raw receipt data
            receipt_df = pd.read_csv(receipt_file, delimiter="~", header=None,)
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna()
        else:          # already split into lines
            receipt_df = pd.read_csv(receipt_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))

        clp_idx = [l for l in range(receipt_df.__len__()) if receipt_df.ix[l, 1][0] == 'CLP'] + [receipt_df.__len__()]

        invoice_number = []
        expect_amount = []
        paid_amount = []
        claim_number = []
        patient_ln = []
        patient_fn = []
        patient_medicaid = []
        service_date = []

        all_codes = []

        for i in range(0, len(clp_idx)-1):
            code = []

            for r in range(clp_idx[i], clp_idx[i+1]):
                row = receipt_df.ix[r, 1]
                if row[0] == "CLP":
                    invoice_number.append(row[1])
                    expect_amount.append(float(row[3]))
                    paid_amount.append(float(row[4]))
                    claim_number.append(row[7])

                elif row[0] == "NM1" and row[1] == "QC":
                    patient_ln.append(row[3])
                    patient_fn.append(row[4])
                    patient_medicaid.append(row[9])

                elif row[0] == 'DTM' and row[1] == "232":
                    service_date.append(arrow.get(row[2], 'YYYYMMDD').format('MM/DD/YYYY'))

                elif row[0] == 'SVC':
                    code.append(row[1][3:])

            all_codes.append(code)

        # print(invoice_number.__len__(), all_codes.__len__())
        result = pd.DataFrame()
        result['Invoice Number'] = invoice_number
        result['Claim Number'] = claim_number
        result['Expected Amount'] = expect_amount
        result['Paid Amount'] = paid_amount
        result['Patient Firstname'] = patient_fn
        result['Patient Lastname'] = patient_ln
        result['Patient Medicaid'] = patient_medicaid
        result['Service Date'] = service_date
        result['Code'] = all_codes

        last_line = len(result) + 1
        result.ix[last_line, 'Claim Number'] = 'Total:'
        result.ix[last_line, 'Expected Amount'] = sum(expect_amount)
        result.ix[last_line, 'Paid Amount'] = sum(paid_amount)
        result.ix[last_line, 'Patient Firstname'] = abs(result.ix[last_line, 'Expected Amount'] - result.ix[last_line, 'Paid Amount'])


        file_name_835= str(arrow.get().date()) + str(datetime.now().time().strftime("%H%M%S"))

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        result.to_excel(os.path.join(file_saving_path, '835-Decoding-' + file_name_835 + '.xlsx'), index=False)


class MongoDB_Methods():
    '''
    Methods with MongoDB
    '''
    def __init__(self, localhost=False):

        if localhost == True:
            local_uri = "mongodb://127.0.0.1:27017"
            self.conn = MongoClient(local_uri)
            self.db = self.conn.polygon
            self.mycollection = self.db.ali_polygon

        else:
            # 69.18.218.27; 192.168.130.62
            mongo_uri = "mongodb://root:" + urllib.parse.quote('0p8rr@Fruit') + '@192.168.130.62:27017/operr_v3_dev'
            self.conn = MongoClient(mongo_uri)
            self.db = self.conn.operr_v3_dev
            self.mycollection = self.db.polygon_boundary_keyuan

    def getPolygonID(self, lng, lat):
        '''

        :param lng:
        :param lat:
        :return: Return all polygonIDs containing this geo point, list type
        '''
        # t1 = time.time()
        response = self.mycollection.find(
            {
                'boundary': {
                    '$geoIntersects': {
                        '$geometry': {
                            'type': 'Point',
                            'coordinates': [lng, lat]
                        }
                    }
                }
            }
        )

        result = [i['polygonId'] for i in response]
        # self.conn.close()
        # print(time.time() - t1)
        return result


class ProcedureCode_Rules():
    '''
    Methods for Code rules
    '''
    pass


class Sqlite_Methods():
    def __init__(self, database):
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()

    def create_table_procedureCode_Rule(self, table):
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} '
                            f'(code_id INTEGER PRIMARY KEY AUTOINCREMENT, '
                            f'CodeName TEXT, Code TEXT, CodeModifier TEXT, '
                            f'Mileage_Start REAL, Mileage_End REAL DEFAULT NULL, '
                            f'Price REAL, polygonID_pickup INT, polygonID_dropoff INT, '
                            f'Calculation_Type TEXT, InHoliday INT DEFAULT (0))')

    #@To do
    def add_new_procedureCode_Rule(self):
        '''
        Should program in dynamic
        :return:
        '''
        pass

    def get_procedureCode_Rule_to_df(self, table, *args):
        '''
        Get corresponding codes data from database.
        Can select certain codes or all codes by default
        :return: Dataframe
        '''
        if not args:
            df = pd.read_sql(f"SELECT * FROM {table}", con=self.conn)
        else:
            codenames = ','.join(['"{}"'.format(i) for i in args])
            df = pd.read_sql(f"SELECT * FROM {table} WHERE CodeName in ({codenames})", con=self.conn)
        return df

    def create_table_cache_address(self, table):
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} '
                            f'(Address_id INTEGER PRIMARY KEY AUTOINCREMENT, '
                            f'Address TEXT, Longitude REAL, Latitude REAL, '
                            f'PolygonIDs TEXT)')

    def upsert_address_cache(self, table, address, lng, lat, poly_id):
        self.create_table_cache_address(table)
        self.cursor.execute(f'INSERT OR REPLACE INTO {table} (Address, Longitude, Latitude, PolygonIDs) VALUES ("{address}", "{lng}", "{lat}", "{poly_id}")')
        self.conn.commit()
        # self.cursor.close()

    def check_address_in_cache(self, table, address):
        '''

        :param table: table name
        :param address: address
        :return: Lng, lat, PolygonIDs if address is in cache
        '''
        self.create_table_cache_address(table)
        self.cursor.execute(f'SELECT Longitude, Latitude, PolygonIDs FROM {table} WHERE Address="{address}"')
        resp = self.cursor.fetchone()
        # self.cursor.close()
        return resp

    def IfplancodeInDB(self, table, plancode):
        self.cursor.execute('SELECT PlanCode FROM {0} WHERE PlanCode="{1}"'.format(table, plancode))
        res = self.cursor.fetchone()
        if not res:
            return False
        else:
            return True

    def create_table_271(self, table):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {0}(InvoiceNumber TEXT, EligibilityResult TEXT, ServiceDate DATE, PatientLN TEXT, PatientFN TEXT, PlanCode TEXT,\
                              Eligible TEXT, CIN TEXT, CoveredCodes TEXT,  DOB DATE, Gender TEXT, \
                               PayerName TEXT, PayerAddress TEXT, PayerTel TEXT, OtherPayer1Name TEXT, OtherPayer1Address TEXT, \
                               OtherPayer1Tel TEXT, OtherPayer1GroupNum TEXT, OtherPayer2Name TEXT, OtherPayer2Address TEXT, \
                               OtherPayer2Tel TEXT, OtherPayer2GroupNum TEXT, OtherPayerPolicyNum TEXT, UpdateDate DATE, PRIMARY KEY(InvoiceNumber))'.format(table))

    def upsert271(self, table, data):
        '''
        :param table: sql table name
        :param data: dataframe like
        :return: None
        '''
        self.create_table_271(table)
        data_len = data.__len__()

        for l in range(data_len):
            row_data = data.ix[l, :]

            invoice_num = row_data['Invoice number']
            eligibility_result = row_data['Eligibility Result']
            CIN = row_data['CIN']
            service_date = row_data['Service date']
            plan_code = row_data['Plan code']
            eligible = row_data['Eligible']
            covered_codes = row_data['Covered Codes']
            patient_ln = row_data['Patient firstname']
            patient_fn = row_data['Patient lastname']
            dob = row_data['Patient DOB']
            gender = row_data['Patient gender']
            payer_name = row_data['Payer name']
            payer_address = row_data['Payer address']
            payer_tel = row_data['Contact Tel.']
            otherpayer1name = row_data['Other Payer1 name']
            otherpayer1address = row_data['Other Payer1 address']
            otherpayer1tel = row_data['Other Payer1 tel.']
            otherpayer1groupnum = row_data['Other Payer1 group number']
            otherpayer2name = row_data['Other Payer2 name']
            otherpayer2address = row_data['Other Payer2 address']
            otherpayer2tel = row_data['Other Payer2 tel.']
            otherpayer2groupnum = row_data['Other Payer2 group number']
            otherpayerpolicynum = row_data['Other Payer policy number']
            update_date = datetime.today().date()


            self.cursor.execute('INSERT OR REPLACE INTO {0} (InvoiceNumber, EligibilityResult, ServiceDate, PatientLN, PatientFN, PlanCode, Eligible, CIN, CoveredCodes, DOB, \
                                    Gender, PayerName, PayerAddress, PayerTel, OtherPayer1Name, OtherPayer1Address, OtherPayer1Tel, OtherPayer1GroupNum, \
                                    OtherPayer2Name, OtherPayer2Address, OtherPayer2Tel, OtherPayer2GroupNum, OtherPayerPolicyNum, UpdateDate) \
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'.format(table),
                (invoice_num, eligibility_result, service_date, patient_ln, patient_fn, plan_code, eligible, CIN, covered_codes, dob, gender, payer_name, payer_address, payer_tel,
                 otherpayer1name, otherpayer1address, otherpayer1tel, otherpayer1groupnum, otherpayer2name, otherpayer2address, otherpayer2tel, otherpayer2groupnum, otherpayerpolicynum, update_date))

        self.conn.commit()

    def create_table_manuallyCheck_lib(self, table):
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS {0}(Eligible TEXT, PatientLN TEXT, PatientFN TEXT, CIN TEXT, Description TEXT, UpdateDate DATE, PRIMARY KEY(CIN))'.format(
                table))

    def manuallyUpsert271Lib(self, table, eligible, patient_ln, patient_fn, cin, description):
        self.create_table_manuallyCheck_lib(table)
        update_date = datetime.today().date()
        self.cursor.execute(
            'INSERT OR REPLACE INTO {0} (Eligible, PatientLN, PatientFN, CIN, Description, UpdateDate) VALUES (?,?,?,?,?,?)'.format(
                table),
            (eligible, patient_ln, patient_fn, cin, description, update_date))
        self.conn.commit()

    def delete_manually271Lib(self, table, cin):
        self.cursor.execute('DELETE FROM {0} WHERE cin="{1}"'.format(table, cin))
        self.conn.commit()

    def delete_lastmonth_manually271Lib(self, table):
        self.cursor.execute(f'SELECT UpdateDate, CIN FROM {table}')
        response = self.cursor.fetchall()
        current_month = arrow.now().date().month

        for res in response:
            date, cin = res
            month_of_date = arrow.get(date).date().month
            if month_of_date != current_month:
                self.delete_manually271Lib(table, cin)
                print(f'CIN: {cin} is expired!')
            else:
                pass

    def generate_excel_from_manually271Lib(self, table, tofile=True):
        date = datetime.today().date()
        df = pd.read_sql('SELECT * FROM {0}'.format(table), con=self.conn)
        if tofile==True:

            current_path = os.getcwd()
            daily_folder = str(datetime.today().date())
            basename = info_locker.base_info['BaseName']
            file_saving_path = os.path.join(current_path, basename, daily_folder)
            if not os.path.exists(file_saving_path):
                os.makedirs(file_saving_path)
                print('Save files to {0}'.format(file_saving_path))
            df.to_excel('Manually Checking Lib-' + str(date) + '.xlsx', index=False)
        return df

    def create_table_x12_external_code(self, table):
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table}(Code TEXT, Description TEXT, PRIMARY KEY(Code))')

    def upsert_x12_external_code(self, table, code, description):
        self.create_table_x12_external_code(table)
        self.cursor.execute(f'INSERT OR REPLACE INTO {table} (Code, Description) VALUES ("{code}", "{description}")')
        self.conn.commit()


class Process_MAS():
    def __init__(self, rawfile):
        # assert (rawfile.endswith('.txt')), "HOUSTON, WE'VE GOT A PROBLEM HERE! \n   ONLY TXT-FORMAT FILE!"
        self.P = Process_Methods()
        self.SQ = Sqlite_Methods('ProcedureCodes.db')

        self.raw_df = pd.read_table(rawfile) if rawfile.endswith('.txt') else pd.read_excel(rawfile)
        self.raw_df['Pick-up Zip'] = self.raw_df['Pick-up Zip'].fillna(0)
        self.raw_df['Drop-off Zip'] = self.raw_df['Drop-off Zip'].fillna(0)
        # pre-process address first
        self.raw_df['Pick-up Address'] = self.raw_df['Pick-up Address'].apply(lambda x: self.P.clean_address(x))
        self.raw_df['Drop-off Address'] = self.raw_df['Drop-off Address'].apply(lambda x: self.P.clean_address(x))
        self.raw_df['Pick-up Zip'] = self.raw_df['Pick-up Zip'].apply(lambda x: str(int(x)))
        self.raw_df['Drop-off Zip'] = self.raw_df['Drop-off Zip'].apply(lambda x: str(int(x)))

        self.Add_procedureCodes()

    def Add_abcd_legs(self, tofile=False):
        unique_invoice = self.raw_df['Invoice Number'].unique().tolist() # Get unique invoice number in list

        legs_name = ['A', 'B', 'C', 'D']
        for invoice_num in unique_invoice:
            # Get cooresponding invoice number's index (Only LEG type)
            idx = self.raw_df.loc[(self.raw_df['Invoice Number'] == invoice_num) & (self.raw_df['Record Type'] == 'Leg')].index
            count_legs = idx.__len__()

            leg_id = []

            if count_legs == 4:
                for i in idx:
                    leg_id.append(self.raw_df.ix[i, 'Leg ID'])  # Get all leg IDs with same invoice number

                leg_id.sort()
                sorted_leg_idx = [self.raw_df.loc[self.raw_df['Leg ID'] == l].index[0] for l in leg_id]

                for index, l in enumerate(sorted_leg_idx):
                    self.raw_df.ix[l, 'Invoice Number'] = str(self.raw_df.ix[l, 'Invoice Number']) + legs_name[index]

            else:
                for index, l in enumerate(idx):
                    self.raw_df.ix[l, 'Invoice Number'] = str(self.raw_df.ix[l, 'Invoice Number']) + legs_name[index]

        if tofile == True:
            self.raw_df.to_excel('MAS-1.xlsx', index=False)

        return self.raw_df

    def Add_procedureCodes(self):

        temp_df = pd.DataFrame()  # for cache usage

        # Change logic to Leg_ID
        # raw_df = self.Add_abcd_legs()
        raw_df = self.raw_df

        raw_df['processed_pickup_address'] = raw_df['Pick-up Address'] + ", " + raw_df['Pick-up City'] + ", " + \
                                             raw_df['Pick-up State'] + " " + raw_df['Pick-up Zip']

        raw_df['processed_dropoff_address'] = raw_df['Drop-off Address'] + ", " + raw_df['Drop-off City'] + ", " + \
                                             raw_df['Drop-off State'] + " " + raw_df['Drop-off Zip']

        ####### TO DO #########
        ####### RULE DATAFRAME #########
        ####### SHOULD IMPROVE FOR DIFFERENT BASES AND RULES ##########
        ####### Applied to following function ##########
        '''
        This function should be improved with args.
        '''
        Rule_df = self.SQ.get_procedureCode_Rule_to_df('Rule')
        ########################

        leg_idx = raw_df.loc[raw_df['Record Type'] == 'Leg'].index  # Get LEG-type trips

        # For counting down
        count_legs = leg_idx.__len__()
        display_flag = int(round(count_legs / 10))
        count = 0

        for i in leg_idx:
            processed_pickup_address = raw_df.ix[i, 'processed_pickup_address']
            processed_dropoff_address = raw_df.ix[i, 'processed_dropoff_address']
            mileage = float(raw_df.ix[i, 'Leg Mileage'])
            pickup_polygonIDs = self.P.getPolygonIDs(processed_pickup_address)
            dropoff_polygonIDs = self.P.getPolygonIDs(processed_dropoff_address)

            procedureCodes, _ = self.P.generate_procedureCodes(Rule_df, mileage, pickup_polygonIDs, dropoff_polygonIDs)

            procedureCodes = list(sorted(set(procedureCodes), key=procedureCodes.index))

            calculated_codes = ",".join(procedureCodes)
            raw_df.ix[i, 'Calculated Codes'] = calculated_codes

            # Display progress
            count += 1
            if count % display_flag == 0:
                progress = int(round(count / count_legs, 1) * 100)
                # print(f'PROCEDURE CODES ARE ADDING TO TRIPS......{progress}% COMPLETED.')
                logging.info(f'\nPROCEDURE CODES ARE ADDING TO TRIPS......{progress}% COMPLETED.')

        temp_df['service_date'] = raw_df['Service Starts'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())

        min_service_date, max_service_date = min(temp_df['service_date']), max(temp_df['service_date'])

        raw_df = raw_df[['Export ID', 'Record_Number', 'Invoice Number', 'Record Type',
                           'First Name', 'Middle Initial', 'Last Name', 'CIN', 'Gender',
                           'Telephone', 'Birthdate', 'Medical Provider', 'Provider ID',
                           'Ordering Provider ID', 'Transport Company', 'Transport Type',
                           'Procedure Code', 'Procedure Code Modifier', 'Service Starts',
                           'Service Ends', 'Standing Order', 'Trips Approved', 'Days Approved',
                           'Wheelchair', 'Contact Name', 'Contact Phone',
                           'Total/Calculated Mileage', 'Pick-up Date', 'Pick-up Time',
                           'Pick-up Address', 'Pick-up Ste/Apt', 'Pick-up City', 'Pick-up State',
                           'Pick-up Zip', 'Drop-off Date', 'Drop-off Time', 'Drop-off Address',
                           'Drop-off Ste/Apt', 'Drop-off City', 'Drop-off State', 'Drop-off Zip',
                           'Leg Mileage', 'Instructions', 'Secondary Service', 'Changed', 'Leg ID',
                           'Calculated Codes',
                         ]]

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        print('GENERATING THE FILE...')
        raw_df.to_excel(os.path.join(file_saving_path, 'Processed MAS-{0}-to-{1}.xlsx'.format(min_service_date, max_service_date)),
                            index=False)
        print('*YEAH! WE MADE IT!*')


class Signoff():
    def __init__(self):
        self.sq = Sqlite_Methods('ProcedureCodes.db')

    def signoff(self, processedMAS, TotalJob):
        print('LOADING FILES...PROGRESS 20%')
        signoff_df = pd.DataFrame()
        temp_df = pd.DataFrame()   #cache dataframe

        totalJob_df = pd.read_excel(TotalJob, header=None) if TotalJob.endswith('.xlsx') else pd.read_csv(TotalJob, header=None)
        totalJob_df.columns = ['FleetNumber', 'ServiceDate', 'CompanyCode', 'CustomerName', 'TripID', 'TollFee',
                                'Amount', 'pComany', 'pReserve', 'pPerson']
        totalJob_df = totalJob_df.dropna()
        totalJob_df['Amount'] = totalJob_df['Amount'].apply(lambda x: D(str(x)))

        if isinstance(processedMAS, pd.DataFrame):
            mas_df = processedMAS
        else:
            mas_df = pd.read_excel(processedMAS)

        # Drop service type
        service_idx = mas_df.loc[mas_df['Record Type'] == 'Service'].index
        mas_df = mas_df.drop(mas_df.index[service_idx])

        # Add driver id and vehicle id to TOTAL JOBS
        print("MATCHING DRIVERS' INFO...PROGRESS 40%")
        totalJob_df['driver id'] = totalJob_df['FleetNumber'].apply(lambda x: info_locker.driver_information[str(x)]['DRIVER_ID'])
        totalJob_df['vehicle id'] = totalJob_df['FleetNumber'].apply(lambda x:
                                                                     info_locker.driver_information[str(x)]['VEHICLE_ID'])

        MAS_data_in_TOTAL = mas_df[mas_df['Invoice Number'].isin(totalJob_df['TripID'])]
        invoice_number_list_in_both = MAS_data_in_TOTAL['Invoice Number'].tolist()

        unique_trip_in_TOTALJOB = totalJob_df['TripID'].unique().tolist()
        for trip in unique_trip_in_TOTALJOB:
            idx_in_totaljob = totalJob_df.loc[totalJob_df['TripID'] == trip].index.tolist()
            idx_in_processedMAS = mas_df.loc[mas_df['Invoice Number'] == trip].index.tolist()

            MAS_amount = D('0')
            if idx_in_processedMAS.__len__() != 0:
                calculated_codes_in_MAS = mas_df.ix[idx_in_processedMAS[0], 'Calculated Codes']
                leg_mileage = D(mas_df.ix[idx_in_processedMAS[0], 'Leg Mileage'])
                leg_mileage = D(float(leg_mileage))
                totalJob_df.ix[idx_in_totaljob[0], 'Codes'] = calculated_codes_in_MAS

                for code in calculated_codes_in_MAS.split(','):
                    self.sq.cursor.execute(f'SELECT Mileage_Start, Price, Calculation_Type FROM Rule WHERE CodeName="{code}"')
                    response = self.sq.cursor.fetchone()

                    if response != None:
                        MAS_amount += D(response[1]) if response[2] == "FLAT" else D(response[1]) * (leg_mileage - D(response[0]))

                    else:
                        print(f'HOUSTON! WE ARE FACING A TOTALLY NEW CODE {code} HERE!')

                MAS_amount = math.floor(float(MAS_amount) * 100) / 100.
                totalJob_df.ix[idx_in_totaljob[0], 'MAS amount'] = MAS_amount
                totalJob_df.ix[idx_in_totaljob[0], 'Difference'] = math.floor((MAS_amount - float(totalJob_df.ix[idx_in_totaljob[0], 'Amount'])) * 100) / 100.

        self.sq.cursor.close()

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        totalJob_df.to_excel(os.path.join(file_saving_path,
                                           f'Difference_Totaljobs&Claims_{datetime.today().date()}_{datetime.now().time().strftime("%H%M%S")}.xlsx'),
                              index=False)

        # Processing Functions #

        def get_tollfee_from_totaljob(invoice_number):
            res = totalJob_df.loc[totalJob_df['TripID'] == invoice_number, 'TollFee'].iloc
            try:
                result = res[0]
            except:
                result = 0
            return result

        def transfer2time(x):
            '''

            :param x: '%H%M this format
            :return: %H:%M or CA:LL
            '''
            try:
                result = datetime.strptime(x, '%H%M').strftime('%H:%M')
            except:
                result = 'CA:LL'

            return result

        def transfer2timeformat(x):
            try:
                result = datetime.strptime(x, '%H:%M').time()
            except:
                result = datetime.strptime('00:00', '%H:%M').time()
            return result

        def get_driver_id(x):
            res = totalJob_df.loc[totalJob_df['TripID'] == x, 'driver id'].iloc

            try:
                result = res[0]

            except:
                result = ""

            return result

        def get_vehicle_id(x):
            res = totalJob_df.loc[totalJob_df['TripID'] == x, 'vehicle id'].iloc

            try:
                result = res[0]

            except:
                result = ""

            return result

        # Create sign off
        print("PREPARING SIGN-OFF...PROGRESS 60%")

        signoff_df['SERVICE DAY'] = mas_df['Service Starts']
        signoff_df['INVOICE ID'] = mas_df['Invoice Number']
        signoff_df['LEG ID'] = mas_df['Leg ID'].astype(int)
        signoff_df['TOLL FEE'] = signoff_df['INVOICE ID'].apply(lambda x: get_tollfee_from_totaljob(x))
        signoff_df['PROCEDURE CODE'] = mas_df['Calculated Codes']

        mas_df['Leg Mileage'] = mas_df['Leg Mileage'].apply(lambda x: math.ceil(float(x)))
        signoff_df['TRIP MILEAGE'] = mas_df['Leg Mileage']

        signoff_df['PICK UP ADDRESS'] = mas_df['Pick-up Address']
        signoff_df['PICK UP CITY'] = mas_df['Pick-up City']
        signoff_df['PICK UP ZIPCODE'] = mas_df['Pick-up Zip'].astype(int)
        signoff_df['DROP OFF ADDRESS'] = mas_df['Drop-off Address']
        signoff_df['DROP OFF CITY'] = mas_df['Drop-off City']
        signoff_df['DROP OFF ZIPCODE'] = mas_df['Drop-off Zip'].astype(int)
        signoff_df['PICK UP TIME'] = mas_df['Pick-up Time'].apply(lambda x: str(x).zfill(4))
        signoff_df['PICK UP TIME'] = signoff_df['PICK UP TIME'].apply(lambda x: transfer2time(x))

        temp_df['delta_time'] = signoff_df['TRIP MILEAGE'].apply(lambda x: timedelta(minutes=(int(x) + 0.4) * 4))
        temp_df['pick_up time'] = signoff_df['PICK UP TIME'].apply(lambda x: transfer2timeformat(x))
        temp_df['dropoff_time1'] = temp_df['pick_up time'].apply(lambda x: datetime.combine(datetime.today().date(), x))
        temp_df['dropoff_time'] = temp_df['dropoff_time1'] + temp_df['delta_time']
        temp_df['dropoff_time'] = temp_df['dropoff_time'].apply(lambda x: x.time().strftime('%H:%M'))
        signoff_df['DROP OFF TIME'] = temp_df['dropoff_time']
        signoff_df['DRIVER ID'] = signoff_df['INVOICE ID'].apply(lambda x: get_driver_id(x))
        signoff_df['VEHICLE ID'] = signoff_df['INVOICE ID'].apply(lambda x: get_vehicle_id(x))
        signoff_df['LEG STATUS'] = signoff_df['INVOICE ID'].apply(lambda x: 0 if x in invoice_number_list_in_both else 1)

        # output any missed trips in TOTAL JOB but not in SIGNOFF

        signoff_data_not_in_TOTALJOB = totalJob_df[~totalJob_df['TripID'].isin(signoff_df['INVOICE ID'])]
        missed_trip_concat_to_signoff_df = pd.DataFrame()

        if signoff_data_not_in_TOTALJOB.__len__() != 0:
            print("SOME MISSING TRIPS FOUND! GENERATING REPORTS...")
            current_path = os.getcwd()
            daily_folder = str(datetime.today().date())
            # basename = info_locker.base_info['BaseName']
            file_saving_path = os.path.join(current_path, daily_folder)
            if not os.path.exists(file_saving_path):
                os.makedirs(file_saving_path)
                print('Save files to {0}'.format(file_saving_path))

            # Missed trips merge to sign off
            missed_trip_concat_to_signoff_df['SERVICE DAY'] = signoff_data_not_in_TOTALJOB['ServiceDate']
            missed_trip_concat_to_signoff_df['INVOICE ID'] = signoff_data_not_in_TOTALJOB['TripID']
            missed_trip_concat_to_signoff_df['LEG ID'] = "NA"
            missed_trip_concat_to_signoff_df['PROCEDURE CODE'] = "NA"
            missed_trip_concat_to_signoff_df['TRIP MILEAGE'] = "NA"
            missed_trip_concat_to_signoff_df['PICK UP ADDRESS'] = "NA"
            missed_trip_concat_to_signoff_df['PICK UP CITY'] = "NA"
            missed_trip_concat_to_signoff_df['PICK UP ZIPCODE'] = "NA"
            missed_trip_concat_to_signoff_df['DROP OFF ADDRESS'] = "NA"
            missed_trip_concat_to_signoff_df['DROP OFF CITY'] = "NA"
            missed_trip_concat_to_signoff_df['DROP OFF ZIPCODE'] = "NA"
            missed_trip_concat_to_signoff_df['PICK UP TIME'] = "NA"
            missed_trip_concat_to_signoff_df['DROP OFF TIME'] = "NA"
            missed_trip_concat_to_signoff_df['DRIVER ID'] = signoff_data_not_in_TOTALJOB['driver id']
            missed_trip_concat_to_signoff_df['VEHICLE ID'] = signoff_data_not_in_TOTALJOB['vehicle id']
            missed_trip_concat_to_signoff_df['LEG STATUS'] = "NA"
            missed_trip_concat_to_signoff_df['CIN'] = "NA"
            missed_trip_concat_to_signoff_df['NPI'] = "NA"

            signoff_data_not_in_TOTALJOB.to_excel(os.path.join(file_saving_path,
                                                               'Missed Trips in TotalJob but not in signoff-' +
                                                               str(datetime.today().date()) +
                                                               str(datetime.now().time().strftime("%H%M%S")) + '.xlsx'),
                                                  index=False)

        # Change "CALL" in pickup or dropoff time
        print('CLEANING SIGN-OFF FILE...PROGRESS 80%')
        delta_time = timedelta(minutes=45)
        unique_invoice_in_signoff = signoff_df['INVOICE ID'].unique().tolist()

        for invoice_num in unique_invoice_in_signoff:
            idx = signoff_df.loc[signoff_df['INVOICE ID'] == invoice_num].index.tolist()
            leg_id = [signoff_df.ix[i, 'LEG ID'] for i in idx]
            leg_id.sort()

            sorted_idx = [signoff_df.loc[signoff_df['LEG ID'] == l].index[0] for l in leg_id]

            for index, i in enumerate(sorted_idx):
                if signoff_df.ix[i, 'PICK UP TIME'] == 'CA:LL' and index != 0:
                    last_row_idx = sorted_idx[index - 1]
                    last_row_dropoff_time = signoff_df.ix[last_row_idx, 'DROP OFF TIME']
                    temp_last_dropoff_time = datetime.strptime(last_row_dropoff_time, '%H:%M').time()
                    temp_last_dropoff_time_date = datetime.combine(datetime.today().date(), temp_last_dropoff_time)
                    new_pickup_time = datetime.combine(datetime.today().date(), temp_last_dropoff_time) + delta_time
                    new_dropoff_time = new_pickup_time + delta_time

                    if temp_last_dropoff_time_date.date() == new_pickup_time.date():
                        signoff_df.ix[i, 'PICK UP TIME'] = new_pickup_time.strftime('%H:%M')
                        signoff_df.ix[i, 'DROP OFF TIME'] = new_dropoff_time.strftime('%H:%M')
                    else:
                        pass

        # Service date range
        temp_df['service_date'] = mas_df['Service Starts'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())
        self.min_service_date = min(temp_df['service_date'])
        self.max_service_date = max(temp_df['service_date'])

        # Finally clean sign off
        signoff_df = signoff_df.sort_values(by='LEG STATUS')
        signoff_df['INVOICE ID'] = signoff_df['INVOICE ID'].apply(lambda x: x[:-1])
        signoff_df['CIN'] = mas_df['CIN']
        signoff_df['NPI'] = mas_df['Ordering Provider ID']

        # Merge
        print("GENERATING SIGN-OFF...PROGRESS 90%")
        signoff_df = pd.concat([signoff_df, missed_trip_concat_to_signoff_df], 0, sort=False)
        signoff_df = signoff_df[['SERVICE DAY', 'INVOICE ID', 'LEG ID', 'TOLL FEE', 'PROCEDURE CODE',
                                   'TRIP MILEAGE', 'PICK UP ADDRESS', 'PICK UP CITY', 'PICK UP ZIPCODE',
                                   'DROP OFF ADDRESS', 'DROP OFF CITY', 'DROP OFF ZIPCODE', 'PICK UP TIME',
                                   'DROP OFF TIME', 'DRIVER ID', 'VEHICLE ID', 'LEG STATUS', 'CIN', 'NPI']]

        # Output file
        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        signoff_df.to_excel(os.path.join(file_saving_path,
                                         'MAS Sign-off-{0}-to-{1}.xlsx'.format(self.min_service_date, self.max_service_date)),
                            index=False)
        print("SIGN-OFF FILE GENERATED!")
        return signoff_df


class Compare_Signoff_PA():
    def __init__(self, signoff, PA, Processed_MAS=None):
        self.signoff_df = pd.read_excel(signoff)
        self.PA_df = pd.read_table(PA)
        if Processed_MAS:
            self.MAS_df = pd.read_excel(Processed_MAS)

        self.PA_df = self.PA_df.fillna("")
        self.PA_df['Invoice Number'] = self.PA_df['Invoice Number'].astype(str)

        self.signoff_df = self.signoff_df.loc[self.signoff_df['LEG STATUS'] == 0]
        self.sq = Sqlite_Methods('ProcedureCodes.db')

    def compare_signoff_pa(self):
        unique_invoice_number_in_signoff = self.signoff_df['INVOICE ID'].unique().tolist()

        missed_trips_df = pd.DataFrame()

        # Arguments
        missed_trips = []   # Trips in signoff but not in PA
        invoice_number_to_output = []
        PA_number = []
        service_NPI = []
        PA_code_list = []
        service_data_list = []
        CIN_list = []
        driver_id = []
        vehicle_id = []
        signoff_code_list = []
        signoff_amount_no_tollfee = []
        signoff_tollfee_list = []

        for invoice_number in unique_invoice_number_in_signoff:
            PA_code_dict = {}
            idx_PA = self.PA_df.loc[self.PA_df['Invoice Number'] == str(invoice_number)].index.tolist()

            if len(idx_PA) == 0:
                missed_trips.append(invoice_number)
            else:
                invoice_number_to_output.append(invoice_number)
                PA_number.append(self.PA_df.ix[idx_PA[0], 'Prior Approval Number'])
                service_NPI.append(self.PA_df.ix[idx_PA[0], 'Ordering Provider'])

                for i in idx_PA:
                    code = self.PA_df.ix[i, 'Item Code'] + self.PA_df.ix[i, 'Item Code Mod']
                    unit = self.PA_df.ix[i, 'Qty']
                    if code != "A0170CG":
                        PA_code_dict[code] = unit
                    else:
                        pass
                # print(PA_code_dict, invoice_number)
                PA_code_list.append(str(PA_code_dict))

                # process sign off data

                idx_signoff = self.signoff_df.loc[self.signoff_df['INVOICE ID'] == invoice_number].index.tolist()
                service_data_list.append(self.signoff_df.ix[idx_signoff[0], 'SERVICE DAY'])
                CIN_list.append(self.signoff_df.ix[idx_signoff[0], 'CIN'])
                driver_id.append(self.signoff_df.ix[idx_signoff[0], 'DRIVER ID'])
                vehicle_id.append(self.signoff_df.ix[idx_signoff[0], 'VEHICLE ID'])

                signoff_amount_without_tollfee = 0
                counter_signoff_codes = Counter()
                signoff_tollfee = []

                for i in idx_signoff:
                    signoff_codes = self.signoff_df.ix[i, 'PROCEDURE CODE']
                    signoff_mile = self.signoff_df.ix[i, 'TRIP MILEAGE']
                    signoff_tollfee.append(self.signoff_df.ix[i, 'TOLL FEE'])

                    splited_signoff_codes = [code for code in signoff_codes.split(",")]
                    temp_counter_dict = dict(Counter(splited_signoff_codes))

                    for c in splited_signoff_codes:
                        self.sq.cursor.execute(f'SELECT Mileage_Start, Calculation_Type FROM Rule WHERE CodeName="{c}"')
                        response = self.sq.cursor.fetchone()

                        if response[1] == 'UNIT':
                            temp_counter_dict[c] = float(format(float(D(signoff_mile - response[0])), '.2f'))
                        else:
                            pass

                    counter_signoff_codes += Counter(temp_counter_dict)
                    counter_signoff_codes = dict(counter_signoff_codes)
                    for key, value in counter_signoff_codes.items():
                        counter_signoff_codes[key] = float(format(value, '.2f'))
                    counter_signoff_codes = Counter(counter_signoff_codes)

                # Calculate amount without toll fee
                for key, value in dict(counter_signoff_codes).items():
                    self.sq.cursor.execute(f'SELECT Price FROM Rule WHERE CodeName="{key}"')
                    resp = self.sq.cursor.fetchone()
                    if resp != None:

                        signoff_amount_without_tollfee += math.floor(float(format(value * resp[0] * 100, '.2f'))) / 100.


                signoff_amount_without_tollfee = math.floor(float(format(signoff_amount_without_tollfee * 100, '.2f'))) / 100.
                signoff_amount_no_tollfee.append(signoff_amount_without_tollfee)

                signoff_code_list.append(str(dict(counter_signoff_codes)))
                signoff_tollfee_list.append(sum(signoff_tollfee))


        missed_trips_df['MISSED TRIPS'] = missed_trips
        correction_df = pd.DataFrame()
        correction_df['Service Date'] = service_data_list
        correction_df['Invoice Number'] = invoice_number_to_output
        correction_df['CIN'] = CIN_list
        correction_df['PA Number'] = PA_number
        correction_df['Driver ID'] = driver_id
        correction_df['Vehicle ID'] = vehicle_id
        correction_df['Service NPI'] = service_NPI
        correction_df['Encode PA'] = PA_code_list
        correction_df['Encode Signoff'] = signoff_code_list
        correction_df['Comparison'] = np.where(correction_df['Encode PA'].apply(lambda x: set(literal_eval(x).items())) -
                                               correction_df['Encode Signoff'].apply(lambda x: set(literal_eval(x).items())) == set({}), "", 'Different')

        correction_df['Signoff Amount Without Toll'] = signoff_amount_no_tollfee
        correction_df['Signoff Tollfee'] = signoff_tollfee_list
        correction_df['Signoff Total Amount'] = correction_df['Signoff Amount Without Toll'] + correction_df['Signoff Tollfee']
        correction_df['Signoff Total Amount'] = correction_df['Signoff Total Amount'].apply(lambda x: float(format(x, '.2f')))

        temp_df = pd.DataFrame()
        temp_df['service_date'] = self.signoff_df['SERVICE DAY'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())

        self.min_service_date = min(temp_df['service_date'])
        self.max_service_date = max(temp_df['service_date'])

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        correction_df.to_excel(os.path.join(file_saving_path,
                                            'MAS Correction-{0}-to-{1}.xlsx'.format(self.min_service_date, self.max_service_date)),
                               index=False)

        if missed_trips_df.__len__() != 0:
            current_path = os.getcwd()
            daily_folder = str(datetime.today().date())
            # basename = info_locker.base_info['BaseName']
            file_saving_path = os.path.join(current_path, daily_folder)
            if not os.path.exists(file_saving_path):
                os.makedirs(file_saving_path)
                print('Save files to {0}'.format(file_saving_path))

            missed_trips_df.to_excel(os.path.join(file_saving_path,
                                              'MISSED TRIPS-{0}-to-{1}.xlsx'.format(self.min_service_date,
                                                                                    self.max_service_date)),
                                 index=False)

        return correction_df

    def EDI_837_excel(self):
        self.correction_df = self.compare_signoff_pa()
        correction_invoice_number = self.correction_df['Invoice Number'].tolist()

        edi_837_dict = {}
        for invoice_number in correction_invoice_number:
            temp_dict = OrderedDict([
                ('patient last name', ""),
                ('patient first name', ""),
                ('patient address', ""),
                ('patient city', ""),
                ('patient state', ""),
                ('patient zip code', ""),
                ('patient gender', ""),
                ('patient pregnant', 'N'),
                ('patient dob', ""),
                ('patient medicaid number', ''),
                ('invoice number', ''),
                ('pa number', 0),
                ('driver last name', ""),
                ('driver first name', ""),
                ('driver license number', ""),
                ('driver plate number', ''),
                ('service facility name', ""),
                ('service address', ""),
                ('service city', ""),
                ('service state', ""),
                ('service zip code', ""),
                ('service date', ""),
                ('service npi', 0),
                ('claim_amount', 0),
                ('service code 1', ""),
                ('modifier code 1', ""),
                ('amount 1', ""),
                ('unit 1', ""),
                ('service code 2', ""),
                ('modifier code 2', ""),
                ('amount 2', ""),
                ('unit 2', ""),
                ('service code 3', ""),
                ('modifier code 3', ""),
                ('amount 3', ""),
                ('unit 3', ""),
                ('service code 4', ""),
                ('modifier code 4', ""),
                ('amount 4', ""),
                ('unit 4', ""),
                ('service code 5', ""),
                ('modifier code 5', ""),
                ('amount 5', ""),
                ('unit 5', ""),
                ('service code 6', ""),
                ('modifier code 6', ""),
                ('amount 6', ""),
                ('unit 6', ""),
            ])

            invoice_number_for_MAS = str(invoice_number) + 'A'

            idx_MAS = self.MAS_df.loc[self.MAS_df['Invoice Number'] == invoice_number_for_MAS].index.tolist()
            idx_correction = self.correction_df.loc[self.correction_df['Invoice Number'] == invoice_number].index.tolist()

            temp_dict['patient last name'] = self.MAS_df.ix[idx_MAS[0], 'Last Name'].upper()
            temp_dict['patient first name'] = self.MAS_df.ix[idx_MAS[0], 'First Name'].upper()
            temp_dict['patient address'] = self.MAS_df.ix[idx_MAS[0], 'Pick-up Address']
            temp_dict['patient city'] = self.MAS_df.ix[idx_MAS[0], 'Pick-up City'].upper()
            temp_dict['patient state'] = self.MAS_df.ix[idx_MAS[0], 'Pick-up State']
            temp_dict['patient zip code'] = str(self.MAS_df.ix[idx_MAS[0], 'Pick-up Zip'])
            temp_dict['patient gender'] = self.MAS_df.ix[idx_MAS[0], 'Gender']
            temp_dict['patient dob'] = self.MAS_df.ix[idx_MAS[0], 'Birthdate']
            temp_dict['patient medicaid number'] = self.MAS_df.ix[idx_MAS[0], 'CIN']
            temp_dict['invoice number'] = invoice_number
            temp_dict['service facility name'] = self.MAS_df.ix[idx_MAS[0], 'Medical Provider'].replace(",", "").upper()
            temp_dict['service address'] = self.MAS_df.ix[idx_MAS[0], 'Drop-off Address']
            temp_dict['service city'] = self.MAS_df.ix[idx_MAS[0], 'Drop-off City'].upper()
            temp_dict['service state'] = self.MAS_df.ix[idx_MAS[0], 'Drop-off State']
            temp_dict['service zip code'] = str(self.MAS_df.ix[idx_MAS[0], 'Drop-off Zip'])
            temp_dict['service date'] = self.MAS_df.ix[idx_MAS[0], 'Service Starts']
            temp_dict['service npi'] = self.MAS_df.ix[idx_MAS[0], 'Ordering Provider ID']

            temp_dict['pa number'] = int(re.findall(r'\d+', self.correction_df.ix[idx_correction[0], 'PA Number'])[0]) if re.findall(r'\d+', self.correction_df.ix[idx_correction[0], 'PA Number']).__len__() != 0 else 0
            temp_dict['driver license number'] = int(self.correction_df.ix[idx_correction[0], 'Driver ID'])
            temp_dict['driver plate number'] = self.correction_df.ix[idx_correction[0], 'Vehicle ID']
            temp_dict['driver first name'], temp_dict['driver last name'] = Process_Methods.use_driver_id_to_find_drivername(int(self.correction_df.ix[idx_correction[0], 'Driver ID']))

            code_dict = literal_eval(self.correction_df.ix[idx_correction[0], 'Encode Signoff'])

            count = 1
            for code, unit in code_dict.items():
                self.sq.cursor.execute(f"SELECT Code, CodeModifier, Price FROM Rule WHERE CodeName='{code}'")
                resp = self.sq.cursor.fetchone()
                service_code = resp[0]
                modifier = resp[1]
                unit_price = D(resp[2])
                amount = math.floor(float(format(D(unit) * unit_price * 100, '.2f'))) / 100.

                self.code_position = f"service code {count}"
                self.modifier_position = f"modifier code {count}"
                self.amount_position = f"amount {count}"
                self.unit_position = f"unit {count}"

                temp_dict[self.code_position] = service_code
                temp_dict[self.modifier_position] = modifier
                temp_dict[self.amount_position] = amount
                temp_dict[self.unit_position] = unit

                count += 1

            if self.correction_df.ix[idx_correction[0], 'Signoff Tollfee'] != 0:
                self.code_position = f"service code {count}"
                self.modifier_position = f"modifier code {count}"
                self.amount_position = f"amount {count}"
                self.unit_position = f"unit {count}"

                temp_dict[self.code_position] = 'A0170'
                temp_dict[self.modifier_position] = 'CG'
                temp_dict[self.amount_position] = self.correction_df.ix[idx_correction[0], 'Signoff Tollfee']
                temp_dict[self.unit_position] = 1

            temp_dict['claim_amount'] = self.correction_df.ix[idx_correction[0], 'Signoff Total Amount']

            edi_837_dict[str(invoice_number)] = temp_dict

        edi_837_df = pd.DataFrame.from_dict(edi_837_dict, 'index')

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        # basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        edi_837_df.to_excel('837 test.xlsx', index=False)


class Correction_compare_with_PDF():
    def __init__(self, correction_file, PDF):
        self.signoff_compare_PA_file = correction_file
        self.payment_raw_file = PDF

    def check_PDF_payment(self):
        new_compare_filename = re.findall(r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', self.signoff_compare_PA_file)[0]
        new_compare_filename = "Check Payment-" + new_compare_filename + '.xlsx'

        def split_CIN_receipt(x):
            # print(x)
            splited = str(x).split(" ")
            CIN, receipt_num = splited[0], splited[1]
            receipt_num = receipt_num.replace("-", "")
            return CIN, int(receipt_num)

        def reverse_minus(x):

            x = str(x)
            if x[-1] == '-':
                x = '-' + x.replace("-", '')
                return float(x)
            else:
                return float(x)

        def remove_leg_from_invoice_number(x):
            if type(x) == str:
                x = x[:-1]
                return int(x)
            else:
                return x

        payment_df = pd.read_excel(self.payment_raw_file, names=['useless', 'invoice number', 'patient name', 'CIN and receipt number', 'service date',
                              'code', 'code unit', 'claim amount', 'paid amount', 'note'], header=None)

        # payment_df.columns = ['useless', 'invoice number', 'patient name', 'CIN and receipt number', 'service date',
        #                       'code', 'code unit', 'claim amount', 'paid amount', 'note']

        payment_df['CIN'], payment_df['receipt number'] = zip(*payment_df['CIN and receipt number'].map(split_CIN_receipt))
        payment_df = payment_df.drop(['CIN and receipt number'], axis=1)

        payment_df['code unit'] = payment_df['code unit'].apply(lambda x: reverse_minus(x))
        payment_df['claim amount'] = payment_df['claim amount'].apply(lambda x: reverse_minus(x))
        payment_df['paid amount'] = payment_df['paid amount'].apply(lambda x: reverse_minus(x))
        payment_df['service date'] = payment_df['service date'].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").strftime('%m/%d/%Y'))
        payment_df['invoice number'] = payment_df['invoice number'].apply(lambda x: remove_leg_from_invoice_number(x))
        ##########################################################################################################

        signoff_compare_PA_df = pd.read_excel(self.signoff_compare_PA_file)

        signoff_compare_PA_df['Encode Payment'] = ""
        signoff_compare_PA_df['Payment Paid Amt'] = ""
        signoff_compare_PA_df['Payer Claim Number'] = ""
        signoff_compare_PA_df['Signoff-Payment comparison'] = ""
        signoff_compare_PA_df['Payment Result'] = ""

        unique_invoice_number_payment = payment_df['invoice number'].unique().tolist()

        for invoice_number in unique_invoice_number_payment:

            idx_payment = payment_df.loc[payment_df['invoice number'] == invoice_number].index.tolist()
            idx_signoff_compare_PA = signoff_compare_PA_df.loc[signoff_compare_PA_df['Invoice Number'] == invoice_number].index.tolist()

            if idx_signoff_compare_PA.__len__() == 0:
                continue
            else:
                signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Payer Claim Number'] = payment_df.ix[idx_payment[0], 'receipt number']

                temp_payment_code_dict = {}
                temp_paid_amount = []

                for i in idx_payment:

                    temp_paid_amount.append(payment_df.ix[i, 'paid amount'])
                    code = payment_df.ix[i, 'code']
                    unit = payment_df.ix[i, 'code unit']
                    temp_payment_code_dict[code] = unit

                signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Encode Payment'] = str(temp_payment_code_dict)
                signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Payment Paid Amt'] = round(
                    sum(temp_paid_amount), 2)

                encoded_signoff = signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Encode Signoff']
                if set(temp_payment_code_dict.items()) - set(literal_eval(encoded_signoff).items()) != set({}):
                    signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Signoff-Payment comparison'] = 'DIFFERENT'
                else:
                    pass

                if signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Signoff Total Amount'] <= signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Payment Paid Amt']:
                    signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Payment Result'] = 'OKAY'
                else:
                    signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'Payment Result'] = 'DIFFERENT'

        none_encode_payment_idx = signoff_compare_PA_df.loc[signoff_compare_PA_df['Encode Payment'] == ""].index.tolist()

        for i in none_encode_payment_idx:
            notFoundServiceDate = signoff_compare_PA_df.ix[i, 'Service Date']
            notFoundCIN = signoff_compare_PA_df.ix[i, 'CIN']
            maybeReplacedInvoice = payment_df.loc[((payment_df['CIN'] == notFoundCIN) & (payment_df['service date'] == notFoundServiceDate)), 'invoice number'].tolist()

            if maybeReplacedInvoice.__len__() == 0:
                signoff_compare_PA_df.ix[i, 'Payment Result'] = 'Not Found'

            else:
                # print(maybeReplacedInvoice[0])
                idx_maybeReplacedInvoice = payment_df.loc[payment_df['invoice number'] == maybeReplacedInvoice[0]].index.tolist()
                replacedReceiptNumber = payment_df.ix[idx_maybeReplacedInvoice[0], 'receipt number']
                replacedPaidamount = [payment_df.ix[r, 'paid amount'] for r in idx_maybeReplacedInvoice]
                replacedTotalPaidAmount = round(sum(replacedPaidamount), 2)

                signoff_compare_PA_df.ix[i, 'Payment Result'] = 'Replaced ' + str(maybeReplacedInvoice[0])
                signoff_compare_PA_df.ix[i, 'Payer Claim Number'] = replacedReceiptNumber
                signoff_compare_PA_df.ix[i, 'Payment Paid Amt'] = replacedTotalPaidAmount

        orderedColumns = ['Service Date', 'Invoice Number', 'PA Number', 'Encode PA', 'Encode Signoff',
                          'Comparison', 'Encode Payment', 'Signoff-Payment comparison', 'Signoff Amount Without Toll',
                          'Signoff Tollfee', 'Signoff Total Amount', 'Payment Paid Amt', 'Payment Result',
                          'Payer Claim Number', 'CIN', 'Driver ID', 'Vehicle ID', 'Service NPI']

        signoff_compare_PA_df = signoff_compare_PA_df[orderedColumns]

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, basename, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        signoff_compare_PA_df.to_excel(os.path.join(file_saving_path, new_compare_filename), index=False)


class EDI837P():

    def __init__(self, file, replace=False):
        self.replace = replace
        self.df = pd.read_csv(file, dtype=object) if file[-1] == 'v' else pd.read_excel(file, dtype=object)
        # self.df = self.df.fillna("")

        self.df['service date'] = self.df['service date'].apply(lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))
        self.df['patient dob'] = self.df['patient dob'].apply(lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))
        self.df['patient pregnant'] = self.df['patient pregnant'].apply(lambda x: x == "Y")

        self.transaction_num = self.df.__len__()
        self.basic_line = 33
        self.lx_lines = 0

        self.submitter_info = info_locker.base_info
        self.bill_provider = info_locker.base_info
        self.driver_info = info_locker.driver_information
        self.receiver_info = info_locker.NYSDOH
        self.version_code = info_locker.version_code['837']

        self.date_format1 = datetime.today().date().strftime("%y%m%d")  #used in ISA
        self.date_format2 = datetime.today().date().strftime("%Y%m%d")
        self.time_format = datetime.now().time().strftime("%H%M")
        self.interchange_ctrl_number = "0" + str(self.date_format1) + str(self.time_format[2:])
        # self.file_name = self.interchange_ctrl_number + '.txt'
        self.all_invoice_number = []
        self.invoice_ST_SE_dict = {}
        if self.replace:
            self.file_name = '837-Replace-' + re.findall(r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', file)[0] if re.findall(
                r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', file).__len__() != 0 else '837-' + str(
                datetime.today().date())
        else:
            self.file_name = '837-' + re.findall(r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', file)[0]  if re.findall(r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', file).__len__() != 0 else '837-' + str(datetime.today().date())


        self.today_datetime = arrow.now().datetime
        self.delayDate_line = arrow.now().shift(days=-90).datetime

    def ISA(self, prod=True):
        if prod==False:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '0', 'T', ':~']

        else:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '1', 'P', ':~']

        return '*'.join(ISA)

    def GS(self):
        GS = ["GS", "HC", self.submitter_info['ETIN'], self.receiver_info['ETIN'], self.date_format2,
              self.time_format, "1", "X", self.version_code]

        return '*'.join(GS) + "~"

    def transaction_header(self, iterations, invoice_number):
        ST = ["ST", "837", str('{:>04d}'.format(iterations)), self.version_code]
        BHT = ["BHT", "0019", "00", str(invoice_number), self.date_format2, self.time_format, "CH"]
        self.all_invoice_number.append(invoice_number)

        return '*'.join(ST) + "~" + '*'.join(BHT) + "~"

    def loop1000a(self):
        NM1 = ["NM1", "41", "2", self.submitter_info['BaseName'], "", "", "", "", "46", self.submitter_info['ETIN']]
        PER = ["PER", "IC", self.submitter_info['ContactName'], "TE", self.submitter_info['ContactTel'] ]

        return '*'.join(NM1) + "~" + '*'.join(PER) + "~"

    def loop1000b(self):
        NM1 = ["NM1", "40", "2", self.receiver_info['name'], "", "", "", "", "46", self.receiver_info['id']]

        return '*'.join(NM1) + "~"

    def loop2000a(self):   # billing provider HL
        HL = ["HL", "1", "", "20", "1"]

        return '*'.join(HL) + "~"

    def loop2010aa(self): #Billing provider info
        NM1 = ["NM1", "85", "2", self.bill_provider['BaseName']]
        N3 = ["N3", self.bill_provider['BaseAddress']]
        N4 = ["N4", self.bill_provider['City'], self.bill_provider['State'], self.bill_provider['zipcode']]
        REF = ["REF", "EI", self.bill_provider['TaxID']]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~" + '*'.join(REF) + "~"

    def loop2000b(self):  # subscriber HL
        HL = ["HL", "2", "1", "22", "0"]
        SBR = ["SBR", "P", "18", "", "", "", "", "", "", "MC"]

        return '*'.join(HL) + "~" + '*'.join(SBR) + "~"

    def loop2010ba(self, first, last, medi_num, address, city, state, zipcode, dob, gender):
        if first == "":
            first = "NoRecord"
        if last == "":
            last = "NoRecord"
        if medi_num == "":
            medi_num = "NoRecord"
        if address == "":
            address = 'NoRecord'
        if city == "":
            city = "NoRecord"
        if state == "":
            state = "NoRecord"
        if zipcode == "":
            zipcode = "00000"
        if dob == "":
            dob = "19000101"
        if gender == "":
            gender = "M"

        NM1 = ["NM1", "IL", "1", last.upper(), first.upper(), "", "", "", "MI", medi_num]
        N3 = ["N3", address.upper()]
        N4 = ["N4", city.upper(), state.upper(), str(zipcode)]
        DMG = ["DMG", "D8", str(dob), gender]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~" + '*'.join(DMG) + "~" # subscriber name

    def loop2010bb(self): # payer info
        NM1 = ["NM1", "PR", "2", self.receiver_info['name'], "", "", "", "", "PI", self.receiver_info['id']]
        N3 = ["N3", self.receiver_info['address']]
        N4 = ["N4", self.receiver_info['city'], self.receiver_info['state'], self.receiver_info['zipcode']]
        REF1 = ["REF", "G2", self.submitter_info['MedicaidProviderNum']]
        REF2 = ["REF", "LU", "003"]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~" + '*'.join(REF1) + "~" + '*'.join(REF2) + "~"

    def loop2300(self, invoice_number, amount, pa_num, delay_claim=False, payer_control_num=None): #claim info
        if amount == "":
            amount = "0"
        if pa_num == "":
            pa_num = 0

        if self.replace == False:
            replace_code = '99:B:1'
        else:
            replace_code = '99:B:7'

        if delay_claim == True:
            CLM = ["CLM", str(invoice_number), str(amount), "", "", replace_code, "Y", "A", "Y", "Y", "P", "","","","","","","","","","11"]
        else:
            CLM = ["CLM", str(invoice_number), str(amount), "", "", replace_code, "Y", "A", "Y", "Y", "P"]


        REF = ['REF', "G1", str('{:>011d}'.format(int(pa_num)))]
        HI = ["HI", "ABK:R69"]

        if self.replace == False:
            return '*'.join(CLM) + "~" + '*'.join(REF) + "~" + '*'.join(HI) + "~"
        else:
            REF_replace = ['REF', 'F8', str(payer_control_num)]
            return '*'.join(CLM) + "~" + '*'.join(REF_replace) + '~' + '*'.join(REF) + "~" + '*'.join(HI) + "~"

    def loop2310a(self, driver_first, driver_last, driver_lic, service_name, service_NPI): #referring provider
        if driver_first == "":
            driver_first = "NoRecord"
        if driver_last == "":
            driver_last = "NoRecord"
        if driver_lic == "":
            driver_lic = 000000000
        if service_name == "":
            service_name = "NoRecord"
        if service_NPI == "":
            service_NPI = 000000000
        NM1 = ["NM1", "DN", "1", service_name.upper(), "", "", "", "", "XX", str(service_NPI)]
        NM1_1 = ["NM1", "P3", "1", driver_last.upper(), driver_first.upper()]
        REF = ["REF", "0B", str(driver_lic)]

        return '*'.join(NM1) + "~" + '*'.join(NM1_1) + "~" + '*'.join(REF) + "~"

    def loop2310b(self, driver_plate): #rendering provider name
        if driver_plate == "":
            driver_plate = 'A000000A'
        NM1 = ["NM1", "82", "2", self.submitter_info['BaseName']]
        REF = ["REF", "G2", driver_plate.upper()]

        return '*'.join(NM1) + "~" + "*".join(REF) + "~"

    def loop2310c(self, service_name, service_NPI, service_address, service_city, service_state, service_zip):
        NM1 = ["NM1", "77", "2", service_name.upper().replace(",", ""), "", "", "", "", "XX", str(service_NPI)]
        N3 = ["N3", service_address.upper()]
        N4 = ["N4", service_city.upper(), service_state.upper(), str(service_zip)]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~"

    def lx1(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV1_01 = "HC:{0}".format(code)

        else:
            SV1_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV1_04 = str(int(unit))
        else:
            SV1_04 = str(unit)

        LX1 = ["LX", "1"]
        SV1 = ["SV1", SV1_01, str(amount), "UN", SV1_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX1) + "~" + '*'.join(SV1) + "~" + '*'.join(DTP) + "~"

    def lx2(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV2_01 = "HC:{0}".format(code)

        else:
            SV2_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV2_04 = str(int(unit))
        else:
            SV2_04 = str(unit)

        LX2 = ["LX", "2"]
        SV2 = ["SV1", SV2_01, str(amount), "UN", SV2_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX2) + "~" + '*'.join(SV2) + "~" + '*'.join(DTP) + "~"

    def lx3(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV3_01 = "HC:{0}".format(code)

        else:
            SV3_01 = "HC:{0}:{1}".format(code, modifier)


        if isinstance(unit, np.int64) == True:
            SV3_04 = str(int(unit))
        else:
            SV3_04 = str(unit)

        LX3 = ["LX", "3"]
        SV3 = ["SV1", SV3_01, str(amount), "UN", SV3_04 , "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX3) + "~" + '*'.join(SV3) + "~" + '*'.join(DTP) + "~"

    def lx4(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV4_01 = "HC:{0}".format(code)

        else:
            SV4_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV4_04 = str(int(unit))
        else:
            SV4_04 = str(unit)

        LX4 = ["LX", "4"]
        SV4 = ["SV1", SV4_01, str(amount), "UN", SV4_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX4) + "~" + '*'.join(SV4) + "~" + '*'.join(DTP) + "~"

    def lx5(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV5_01 = "HC:{0}".format(code)

        else:
            SV5_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV5_04 = str(int(unit))
        else:
            SV5_04 = str(unit)

        LX5 = ["LX", "5"]
        SV5 = ["SV1", SV5_01, str(amount), "UN", SV5_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX5) + "~" + '*'.join(SV5) + "~" + '*'.join(DTP) + "~"

    def lx6(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV6_01 = "HC:{0}".format(code)

        else:
            SV6_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV6_04 = str(int(unit))
        else:
            SV6_04 = str(unit)

        LX6 = ["LX", "6"]
        SV6 = ["SV1", SV6_01, str(amount), "UN", SV6_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX6) + "~" + '*'.join(SV6) + "~" + '*'.join(DTP) + "~"

    def loop2400(self, row_data):

        lx1 = self.lx1(code=row_data['service code 1'].values[0], amount=row_data['amount 1'].values[0], unit=row_data['unit 1'].values[0],
                       service_date=row_data['service date'].values[0], modifier=row_data['modifier code 1'].values[0])

        if row_data['service code 2'].isnull().values[0] == True:
            lx2 = lx3 = lx4 = lx5 = lx6 = ""
        else:
            lx2 = self.lx2(code=row_data['service code 2'].values[0], amount=row_data['amount 2'].values[0], unit=row_data['unit 2'].values[0],
                           service_date=row_data['service date'].values[0], modifier=row_data['modifier code 2'].values[0])

            if row_data['service code 3'].isnull().values[0] == True:
                lx3 = lx4 = lx5 = lx6 = ""

            else:
                lx3 = self.lx3(code=row_data['service code 3'].values[0], amount=row_data['amount 3'].values[0], unit=row_data['unit 3'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 3'].values[0])

                if row_data['service code 4'].isnull().values[0] == True:
                    lx4 = lx5 = lx6 = ""

                else:
                    lx4 = self.lx4(code=row_data['service code 4'].values[0], amount=row_data['amount 4'].values[0], unit=row_data['unit 4'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 4'].values[0])

                    if row_data['service code 5'].isnull().values[0] == True:
                        lx5 = lx6 = ""

                    else:
                        lx5 = self.lx5(code=row_data['service code 5'].values[0], amount=row_data['amount 5'].values[0], unit=row_data['unit 5'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 5'].values[0])

                        if row_data['service code 6'].isnull().values[0] == True:
                            lx6 = ""
                        else:
                            lx6 = self.lx6(code=row_data['service code 6'].values[0], amount=row_data['amount 6'].values[0], unit=row_data['unit 6'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 6'].values[0])

        if len(lx1) > 0:
            self.lx_lines += 3

        if len(lx2) > 0:
            self.lx_lines += 3

        if len(lx3) > 0:
            self.lx_lines += 3

        if len(lx4) > 0:
            self.lx_lines += 3

        if len(lx5) > 0:
            self.lx_lines += 3

        if len(lx6) > 0:
            self.lx_lines += 3

        return lx1+lx2+lx3+lx4+lx5+lx6

    def transaction_trailer(self, count_line, iterations):
        SE = ["SE", str(count_line), str('{:>04d}'.format(iterations))]
        return '*'.join(SE) + "~"

    def GE(self, count_ST):
        GE = ["GE", str(count_ST), "1"]

        return '*'.join(GE) + "~"

    def IEA(self):
        IEA = ["IEA", "1", self.interchange_ctrl_number]

        return '*'.join(IEA) + "~"

    def ST_SE_loop(self):

        result = []
        temp_invoice_num = []
        temp_ST_SE = []
        temp_patient_fn = []
        temp_patient_ln = []
        temp_patient_medicaid_num = []
        temp_service_date = []
        temp_837_name = []

        for row in range(self.transaction_num):
            self.lx_lines = 0
            df_row = self.df.ix[[row]]   # get row data

            service_date = df_row['service date'].values[0]
            arrow_serviceDate = arrow.get(service_date, 'YYYYMMDD').datetime

            delayClaim_switch = True if arrow_serviceDate <= self.delayDate_line else False
            payerControlNum = df_row['payer control number'].values[0] if delayClaim_switch == True else None

            ST = self.transaction_header(iterations=row+1, invoice_number= df_row['invoice number'].values[0])
            loop1000a = self.loop1000a()
            loop1000b = self.loop1000b()
            loop2000a = self.loop2000a()
            loop2010aa = self.loop2010aa()
            loop2000b = self.loop2000b()
            loop2010ba = self.loop2010ba(first=df_row['patient first name'].values[0], last=df_row['patient last name'].values[0], medi_num=df_row['patient medicaid number'].values[0],
                                         address=df_row['patient address'].values[0], city=df_row['patient city'].values[0], state=df_row['patient state'].values[0],
                                         zipcode=df_row['patient zip code'].values[0], dob=df_row['patient dob'].values[0], gender=df_row['patient gender'].values[0])
            loop2010bb = self.loop2010bb()
            loop2300 = self.loop2300(invoice_number=df_row['invoice number'].values[0], amount=df_row['claim_amount'].values[0], pa_num=df_row['pa number'].values[0], delay_claim=delayClaim_switch, payer_control_num=payerControlNum)
            loop2310a = self.loop2310a(driver_first=df_row['driver first name'].values[0], driver_last=df_row['driver last name'].values[0],
                                       driver_lic=df_row['driver license number'].values[0], service_name=df_row['service facility name'].values[0],
                                       service_NPI=df_row['service npi'].values[0])
            loop2310b = self.loop2310b(driver_plate=df_row['driver plate number'].values[0])
            loop2310c = self.loop2310c(service_name=df_row['service facility name'].values[0], service_NPI=df_row['service npi'].values[0],
                                       service_address=df_row['service address'].values[0], service_city=df_row['service city'].values[0],
                                       service_state=df_row['service state'].values[0], service_zip=df_row['service zip code'].values[0])
            loop2400 = self.loop2400(df_row)

            lines_st_se = self.basic_line + self.lx_lines
            SE = self.transaction_trailer(count_line=lines_st_se, iterations=row+1)

            merge_loop = ST + loop1000a + loop1000b + loop2000a + loop2010aa + loop2000b + loop2010ba + loop2010bb + loop2300 + loop2310a + loop2310b + loop2310c + loop2400 + SE

            # self.invoice_ST_SE_dict[str(df_row['invoice number'].values[0])] = {'ST_SE loop': merge_loop,
            #                                                                     'Patient FN': df_row['patient first name'].values[0],
            #                                                                     'Patient LN': df_row['patient last name'].values[0],
            #                                                                     'Patient medicaid number': df_row['patient medicaid number'].values[0],
            #                                                                     'Service date': df_row['service date'].values[0],
            #                                                                     '837 file name': self.file_name,
            #                                                                     }
            temp_837_name.append(self.file_name)
            temp_invoice_num.append(df_row['invoice number'].values[0])
            temp_patient_fn.append(df_row['patient first name'].values[0])
            temp_patient_ln.append(df_row['patient last name'].values[0])
            temp_patient_medicaid_num.append(df_row['patient medicaid number'].values[0])
            temp_ST_SE.append(merge_loop)
            temp_service_date.append(df_row['service date'].values[0])

            result.append(merge_loop)

        self.invoice_ST_SE_dict = {'837 file name': temp_837_name,
                                   'Invoice number': temp_invoice_num,
                                   'Patient FN': temp_patient_fn,
                                   'Patient LN': temp_patient_ln,
                                   'Patient medicaid number': temp_patient_medicaid_num,
                                   'Service date': temp_service_date,
                                   'ST_SE': temp_ST_SE}
        return "".join(result)

    def ISA_IEA(self):
        ISA = self.ISA()
        GS = self.GS()
        ST_SE = self.ST_SE_loop()
        GE = self.GE(self.transaction_num)
        IEA = self.IEA()

        return ISA + GS + ST_SE + GE + IEA


class EDI270():

    def __init__(self, file):

        if isinstance(file, pd.DataFrame):     #if input is pandas dataframe type, use it directly or read from csv or excel
            self.df = file
        else:
            self.df = pd.read_csv(file, dtype=object) if file[-1] == 'v' else pd.read_excel(file, dtype=object)

        self.df['SVC DATE'] = self.df['SVC DATE'].apply(lambda x: arrow.get(str(x), ['YYYY-MM-DD HH:mm:ss', 'MM/DD/YYYY']).format('YYYYMMDD'))

        self.df['DOB'] = self.df['DOB'].apply(lambda x: arrow.get(str(x), ['YYYY-MM-DD HH:mm:ss', 'MM/DD/YYYY']).format('YYYYMMDD'))

        self.transaction_num = self.df.__len__()   # the number of trips in data
        self.st_se_fixed_lines = 13   # fixed number of lines between ST and SE(each section)

        self.date_format1 = datetime.today().date().strftime("%y%m%d")  # used in ISA
        self.date_format2 = datetime.today().date().strftime("%Y%m%d")
        self.time_format = datetime.now().time().strftime("%H%M%S")
        self.interchange_ctrl_number = "0" + str(self.date_format1) + str(self.time_format[2:4])

        self.version_code = info_locker.version_code['270']
        self.submitter_info = info_locker.base_info
        self.receiver_info = info_locker.NYSDOH

        self.file_name = '270-' + self.date_format2 + self.time_format + ".txt"

    def ISA(self, prod=True):
        if prod==False:    # PTE mode

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format[:-2]), '^', '00501', self.interchange_ctrl_number, '0', 'T', ':~']

        else:     # production mode

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format[:-2]), '^', '00501', self.interchange_ctrl_number, '1', 'P', ':~']

        return '*'.join(ISA)

    def GS(self):
        GS = ["GS", "HS", self.submitter_info['ETIN'], self.receiver_info['ETIN'], self.date_format2,
              self.time_format[:-2], "1", "X", self.version_code]

        return '*'.join(GS) + "~"

    def transaction_header(self, iterations, invoice_number):
        ST = ["ST", "270", str('{:>04d}'.format(iterations)), self.version_code]
        BHT = ["BHT", "0022", "13", str(invoice_number).strip(), self.date_format2, self.time_format[:-2]]

        return '*'.join(ST) + "~" + '*'.join(BHT) + "~"

    def first_HL(self):
        HL = ["HL", "1", "", "20", "1"]
        NM1 = ["NM1", "PR", "2", self.receiver_info['name'], "", "", "", "", "FI", self.receiver_info['id']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def second_HL(self, service_name, service_npi):
        HL = ["HL", "2", "1", "21", "1"]
        NM1 = ["NM1", "1P", "2", service_name.strip().replace(",", ""), "", "", "", "", "XX", str(service_npi).strip()]
        REF = ["REF", "EO", self.submitter_info['MedicaidProviderNum']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~" + "*".join(REF) + "~"

    def third_HL(self, patient_lastname, patient_firstname, medicaid_number, dob, gender, service_date):
        HL = ["HL", "3", "2", "22", "0"]
        NM1 = ["NM1", "IL", "1", patient_lastname.strip().upper(), patient_firstname.strip().upper(), "", "", "", "MI", medicaid_number.strip().upper()]
        DMG = ["DMG", "D8", str(dob), gender.upper()]
        DTP = ["DTP", "291", "D8", str(service_date)]
        EQ = ["EQ", "30"]

        return "*".join(HL) + "~" + "*".join(NM1) + "~" + "*".join(DMG) + "~" + "*".join(DTP) + "~" + "*".join(EQ) + "~"

    def transaction_trailer(self, iterations):
        SE = ["SE", "13", str('{:>04d}'.format(iterations))]
        return '*'.join(SE) + "~"

    def GE(self, count_ST):
        GE = ["GE", str(count_ST), "1"]

        return '*'.join(GE) + "~"

    def IEA(self):
        IEA = ["IEA", "1", self.interchange_ctrl_number]

        return "*".join(IEA) + "~"

    def ST_SE_loop(self):

        result = []
        for row in range(self.transaction_num):
            df_row = self.df.ix[[row]]
            ST = self.transaction_header(iterations=row+1, invoice_number=df_row['INVOICE NUMBER'].values[0])
            first_HL = self.first_HL()
            second_HL = self.second_HL(service_name=df_row['SVC NAME'].values[0], service_npi=df_row['SVC NPI'].values[0])
            third_HL = self.third_HL(patient_lastname=df_row['CLIENT LAST NAME'].values[0], patient_firstname=df_row['CLIENT FIRST NAME'].values[0],
                                     medicaid_number=df_row['MEDICAID ID NUMBER'].values[0], dob=df_row['DOB'].values[0],
                                     gender=df_row['GENDER'].values[0], service_date=df_row['SVC DATE'].values[0])
            SE = self.transaction_trailer(iterations=row+1)

            merged_loop = ST + first_HL + second_HL + third_HL + SE
            result.append(merged_loop)

        return "".join(result)

    def ISA_IEA(self):
        ISA = self.ISA()
        GS = self.GS()
        ST_SE = self.ST_SE_loop()
        GE = self.GE(self.transaction_num)
        IEA = self.IEA()

        return ISA + GS + ST_SE + GE + IEA


class EDI276():

    def __init__(self, file):
        if isinstance(file, pd.DataFrame):
            self.df = file
        else:
            self.df = pd.read_csv(file) if file[-1] == 'v' else pd.read_excel(file)

        # print(self.df)
        # try:
        #     self.df['SVC DATE'] = self.df['SVC DATE'].apply(lambda x: datetime.strptime(str(x), "%Y%m%d").strftime("%Y%m%d"))
        # except:
        #     self.df['SVC DATE'] = self.df['SVC DATE'].apply(
        #         lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))
        #
        # try:
        #     self.df['DOB'] = self.df['DOB'].apply(lambda x: datetime.strptime(str(x), "%Y%m%d").strftime("%Y%m%d"))
        # except:
        #     self.df['DOB'] = self.df['DOB'].apply(
        #         lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))

        self.transaction_num = self.df.__len__()
        self.st_se_fixed_lines = 15

        self.date_format1 = datetime.today().date().strftime("%y%m%d")  # used in ISA
        self.date_format2 = datetime.today().date().strftime("%Y%m%d")
        self.time_format = datetime.now().time().strftime("%H%M")
        self.interchange_ctrl_number = "0" + str(self.date_format1) + str(self.time_format[2:])

        self.version_code = info_locker.version_code['276']
        self.submitter_info = info_locker.base_info
        self.bill_provider = info_locker.base_info
        self.receiver_info = info_locker.NYSDOH

        self.file_name = '276-' + self.date_format2 + self.time_format + ".txt"

    def ISA(self, prod=True):
        if prod==False:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '0', 'T', ':~']

        else:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '1', 'P', ':~']

        return '*'.join(ISA)

    def GS(self):
        GS = ["GS", "HR", self.submitter_info['ETIN'], self.receiver_info['ETIN'], self.date_format2,
              self.time_format, "1", "X", self.version_code]

        return '*'.join(GS) + "~"

    def transaction_header(self, iterations, invoice_number):
        ST = ["ST", "276", str('{:>04d}'.format(iterations)), self.version_code]
        BHT = ["BHT", "0010", "13", str(invoice_number).strip(), self.date_format2, self.time_format]

        return '*'.join(ST) + "~" + '*'.join(BHT) + "~"

    def first_HL(self): # payer info
        HL = ["HL", "1", "", "20", "1"]
        NM1 = ["NM1", "PR", "2", self.receiver_info['name'], "", "", "", "", "PI", self.receiver_info['id']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def second_HL(self): # submitter info
        HL = ["HL", "2", "1", "21", "1"]
        NM1 = ['NM1', '41', '2', self.submitter_info['BaseName'], '', '', '', '', '46', self.submitter_info['ETIN']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def third_HL(self): # PROVIDER INFO  SV: medicaid #, XX: NPI
        HL = ['HL', '3', '2', '19', '1']
        # print(self.bill_provider['MedicaidProviderNum'])
        NM1 = ['NM1', '1P', '2', self.bill_provider['BaseName'], '', '', '', '', 'SV', self.bill_provider['MedicaidProviderNum']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def fourth_HL(self, dob, gender, patient_lastname, patient_firstname, medicaid_number, claim_ctl_number, service_date, invoice_number):
        HL = ['HL', '4', '3', '22', '0']
        DMG = ['DMG', 'D8', str(dob), gender.upper()]
        NM1 = ['NM1', 'IL', '1', patient_lastname.strip().upper(), patient_firstname.strip().upper(), '', '', '', 'MI', medicaid_number]
        TRN = ['TRN', '1', str(invoice_number).strip()]
        REF = ['REF', '1K', str(claim_ctl_number)]
        DTP = ['DTP', '472', 'D8', service_date.__str__()]

        return "*".join(HL) + '~' + "*".join(DMG) + "~" + "*".join(NM1) + "~" + "*".join(TRN) + "~" + "*".join(REF) + "~" + "*".join(DTP) + "~"

    def transaction_trailer(self, iterations):
        SE = ['SE', '15', str('{:>04d}'.format(iterations))]
        return '*'.join(SE) + "~"

    def GE(self, count_ST):
        GE = ['GE', count_ST.__str__(), '1']
        return '*'.join(GE) + '~'

    def IEA(self):
        IEA = ["IEA", "1", self.interchange_ctrl_number]

        return "*".join(IEA) + "~"

    def ST_SE_loop(self):

        result = []
        for row in range(self.transaction_num):
            df_row = self.df.ix[[row]]
            ST = self.transaction_header(iterations=row+1, invoice_number=df_row['INVOICE NUMBER'].values[0])
            first_HL = self.first_HL()
            second_HL = self.second_HL()
            third_HL = self.third_HL()
            fourth_HL = self.fourth_HL(dob=df_row['DOB'].values[0], gender=df_row['GENDER'].values[0],
                                       patient_lastname=df_row['CLIENT LAST NAME'].values[0], patient_firstname=df_row['CLIENT FIRST NAME'].values[0],
                                       medicaid_number=df_row['MEDICAID ID NUMBER'].values[0], claim_ctl_number=df_row['CLAIM CONTROL NUMBER'].values[0],
                                       service_date=df_row['SVC DATE'].values[0], invoice_number=df_row['INVOICE NUMBER'].values[0])
            SE = self.transaction_trailer(iterations=row+1)

            merged_loop = ST + first_HL + second_HL + third_HL + fourth_HL + SE
            result.append(merged_loop)

        return ''.join(result)

    def ISA_IEA(self):
        ISA = self.ISA()
        GS = self.GS()
        ST_SE = self.ST_SE_loop()
        GE = self.GE(self.transaction_num)
        IEA = self.IEA()

        return ISA + GS + ST_SE + GE + IEA


class MASProtocol():
    '''
    Work Flow:
    StartSession --> GetSessionID --> InvoiceAttest --> (OtherProcess) --> EndSession
    Doc website: https://www.medanswering.com/wp-content/uploads/2017/12/MAS-API-Information.pdf

    '''
    def __init__(self, signoff_file):
        self._api_key = 'PLMZTWNS11GU8P5276J12GHNW1KHDMW42OZ6W6VT4XTXQ76OT1OBBFE5ZF006JUZ'
        self._address = 'https://www.medanswering.com/Provider_API.taf'
        self._headers = {'Content-Type': 'application/xml'}
        self.sessId = self.parseStartSession()

        if isinstance(signoff_file, pd.DataFrame):
            self.df = signoff_file
        else:
            self.df = pd.read_excel(signoff_file) if signoff_file.endswith('.xlsx') else pd.read_csv(signoff_file)

        self.df = self.df.loc[self.df['LEG STATUS'] == 0]

    def _makeStartSession(self):
        xml = []

        xml.append('<?xml version="1.0" encoding="utf-8" ?>')
        xml.append('<TPRequest>')
        xml.append(f'<authentication>{self._api_key}</authentication>')
        xml.append('<startSession>')
        xml.append('<attributes></attributes>')
        xml.append('</startSession>')
        xml.append('</TPRequest>')

        return ''.join(xml)

    def requestStartSession(self):
        response = requests.post(self._address, data=self._makeStartSession(), headers=self._headers)
        return response

    def parseStartSession(self):
        startSessionResponse = self.requestStartSession()
        try:
            root = ET.fromstring(startSessionResponse.text.encode('utf-8'))
            return root.findall('.//sessionIdentifier')[0].text
        except:
            raise ValueError('INVALID RESPONSE!')

    def _makeEndSession(self):
        xml = []

        xml.append('<?xml version="1.0" encoding="utf-8" ?>')
        xml.append('<TPRequest>')
        xml.append(f'<authentication>{self._api_key}</authentication>')
        xml.append('<endSession>')
        xml.append(f'<sessionIdentifier>{self.sessId}</sessionIdentifier>')
        xml.append('</endSession>')
        xml.append('</TPRequest>')

        return ''.join(xml)

    def requestEndSession(self):
        response = requests.post(self._address, data=self._makeEndSession(), headers=self._headers)
        return

    def _makeInvoiceAttest(self):
        uniqueInvoiceNumberList = self.df['INVOICE ID'].unique().tolist()
        xml = []

        # Fixed part1
        xml.append('<?xml version="1.0" encoding="utf-8" ?>')
        xml.append('<TPRequest>')
        xml.append(f'<authentication>{self._api_key}</authentication>')
        xml.append('<InvoiceAttest version="2">')
        xml.append(f'<sessionIdentifier>{self.sessId}</sessionIdentifier>')
        xml.append('<Invoices>')

        for i in uniqueInvoiceNumberList:
            idx_i = self.df.loc[self.df['INVOICE ID'] == i].index.tolist()

            xml.append('<Invoice>')
            xml.append('<Status>0</Status>')
            xml.append(f'<invoicenumber>{i}</invoicenumber>')
            xml.append('<legs>')

            for idx in idx_i:
                legId = self.df.ix[idx, 'LEG ID']
                mileage = self.df.ix[idx, 'TRIP MILEAGE']
                driverId = self.df.ix[idx, 'DRIVER ID']
                vehicleId = self.df.ix[idx, 'VEHICLE ID']


                '''
                # Use address cache to get geolocation, prevent from calling too many times APIs.
                # Should add corresponding XML information in the following lines.
                
                pickupAddress = self.df.ix[idx, 'PICK UP ADDRESS'] + ', ' + self.df.ix[idx, 'PICK UP CITY'] + ', ' +\
                                self.df.ix[idx, 'PICK UP ZIPCODE']
                dropoffAddress = self.df.ix[idx, 'DROP OFF ADDRESS'] + ', ' + self.df.ix[idx, 'DROP OFF CITY'] + ', ' + \
                                self.df.ix[idx, 'DROP OFF ZIPCODE']

                pickupAddress = Process_Methods.clean_address(pickupAddress)
                dropoffAddress = Process_Methods.clean_address(dropoffAddress)
                pickup_lng, pickup_lat = Process_Methods.google_address(pickupAddress)
                dropoff_lng, dropoff_lat = Process_Methods.google_address(dropoffAddress)
                
                '''

                xml.append('<leg>')
                xml.append(f'<legnumber>{legId}</legnumber>')
                xml.append('<legstatus>0</legstatus>')
                xml.append(f'<usemileage>{mileage}</usemileage>')
                xml.append(f'<driverid>{driverId}</driverid>')
                xml.append(f'<vehicleid>{vehicleId}</vehicleid>')

                '''
                # Add geolocation info here.    <-------
                
                xml.append('<pickupGPSData>')
                xml.append(f'<latitude>{pickup_lat}</latitude><longitude>{pickup_lng}</longitude>')
                xml.append('</pickupGPSData>)
                xml.append('<dropoffGPSData>')
                xml.append(f'<latitude>{dropoff_lat}</latitude><longitude>{dropoff_lng}</longitude>')
                xml.append('</dropoffGPSData>)
                
                '''

                xml.append('</leg>')

            xml.append('</legs>')
            xml.append('<services/>')
            xml.append('</Invoice>')

        # Fixed part2
        xml.append('</Invoices>')
        xml.append('</InvoiceAttest>')
        xml.append('</TPRequest>')

        # If want to pretty print XML, using the following codes.
        # from bs4 import BeautifulSoup
        # print(BeautifulSoup(xml_str, 'xml').prettify())

        return ''.join(xml)

    def requestInvoiceAttest(self):
        response = requests.post(self._address, data=self._makeInvoiceAttest(), headers=self._headers)
        # print(response.text)
        try:
            root = ET.fromstring(response.text.encode('utf-8'))
            correct = root.findall('.//InvoicesCorrect')[0].text
            error = root.findall('.//InvoiceErrors')[0].text

            # print(f'Success: {correct};\nFailure (Or already attested): {error}')
            logging.info(f'\nSuccess: {correct};\nFailure (Or already attested): {error}')

        except ValueError:
            raise

        return correct, error

    def main(self):
        correct, error = self.requestInvoiceAttest()
        self.requestEndSession()

        return correct, error


if __name__ == '__main__':
    alpaca = '''
             ┌─┐       ┌─┐
          ┌──┘ ┴───────┘ ┴──┐
          │                 │
          │       ───       │
          │  ─┬┘       └┬─  │
          │                 │
          │       ─┴─       │
          │                 │
          └───┐         ┌───┘
              │         │
              │         │
              │         │
              │         └──────────────┐
              │                        │
              │                        ├─┐
              │                        ┌─┘
              │                        │
              └─┐  ┐  ┌───────┬──┐  ┌──┘
                │ ─┤ ─┤       │ ─┤ ─┤
                └──┴──┘       └──┴──┘

            '''
    print(alpaca)

    # sq = Sqlite_Methods('ProcedureCodes.db')
    #
    # mileage = 20.9
    # pick_address = '25 PINE ST, New York, NY 10005'
    # drop_address = '430 LAKEVILLE RD, New York, NY 11042'
    # # #
    # p = Process_Methods()
    # df = sq.get_procedureCode_Rule_to_df('Rule')
    # # #
    # pick_poly = p.getPolygonIDs(pick_address)
    # drop_ploy = p.getPolygonIDs(drop_address)
    # # #
    # codes, _ = p.generate_procedureCodes(df, mileage, pick_poly, drop_ploy)
    # print(codes)
    # print(sorted(set(codes), key=codes.index))
    # print(",".join(list(set(codes))))
####################


    conn = sqlite3.connect('EDI.db')
    driver_df = pd.read_sql('SELECT * FROM driver_info WHERE Base="CLEAN AIR CAR SERVICE AND PARKING COR"', conn)
    driver_df.set_index(['Fleet'], inplace=True)
    dict_driver_df = driver_df.to_dict('index')
    info_locker.driver_information = dict_driver_df if dict_driver_df else None

    base_df = pd.read_sql("SELECT * FROM AllBases WHERE BaseName='CLEAN AIR CAR SERVICE AND PARKING COR'", conn)
    dict_base_df = base_df.to_dict('records')
    info_locker.base_info = dict_base_df[0] if dict_base_df else None

    p = Process_MAS('Processed Vendor.xlsx')

    # print(info_locker.driver_information)

    # y = Signoff().signoff('./CLEAN AIR CAR SERVICE AND PARKING COR/2018-06-14/Processed MAS-2018-01-01-to-2018-01-31.xlsx', './TestData/Jan.2018 total jobs.xlsx')

    # c = Compare_Signoff_PA('./2018-06-12/MAS Sign-off-2018-01-01-to-2018-01-31.xlsx', './TestData/Roster-Export-2018-04-16-13-15-24.txt', './2018-06-12/Processed MAS-2018-01-01-to-2018-01-31.xlsx')
    # c.compare_signoff_pa()
    # c.EDI_837_excel()
    # Process_Methods.generate_837('837 test.xlsx', delay_claim=False)
    # Process_Methods.generate_270('./TestData/Vendor-31226-2018-05-07-09-55-59.txt')
    # Process_Methods.process_271('Reglible180415202622.100001（0326-0416）.x12')
    # Process_Methods.process_276_receipt('R180525165538.090001.x12', edi837='837P-1 Data-for-2018-04-30-to-2018-05-06 (1).xlsx')

    # c = Correction_compare_with_PDF('./2018-06-12/MAS Correction-2018-01-01-to-2018-01-31.xlsx', './TestData/NEW_Jan-March 2018 Payment new .xlsx')
    # c.check_PDF_payment()
