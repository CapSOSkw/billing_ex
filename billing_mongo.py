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


class info_locker(object):

    driver_information = None


class Process_Methods():
    '''
    Some basic processing functions
    '''
    def __init__(self):
        pass

    @staticmethod
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
    def getPolygonIDs(address):
        sq = Sqlite_Methods('ProcedureCodes.db')
        if_address_in_cache = sq.check_address_in_cache('address_cache_test', address)

        if if_address_in_cache != None:
            result = if_address_in_cache[2]
            anti_process_result = [int(i) for i in result.split(',')]
            return anti_process_result

        else:
            lng, lat = Process_Methods.Google2Geo(address)
            result = MongoDB_Methods(localhost=True).getPolygonID(lng=lng, lat=lat)
            process_result = ','.join(map(str, result))
            sq.upsert_address_cache('address_cache_test', address, lng, lat, process_result)
            print(f'HOUSTON! {address} IS RECORDED!')
            return result

    @staticmethod
    def frange(start, end, step):
        while start < end:
            yield float('{0:.2f}'.format(start))
            start += step

    @staticmethod
    def generate_procedureCodes(dataframe, mileage, pickup_poly, dropoff_poly, **kwargs):
        '''

        :param dataframe: Commonly call from 'get_procedureCode_Rule_to_df()'
        :param kwargs: mileage, pickup_polygonIDs, dropoff_polygonIDs, etc.
        :return: 1. A list, the codes should be used in this trip.
                2. A dictionary type, contains code names and modifiers
        '''

        result = []
        res_to_dict = {}
        for r in range(len(dataframe)):
            row = dataframe.ix[r, :]
            mileage_start = row['Mileage_Start']
            mileage_end = row['Mileage_End']
            pickup = row['polygonID_pickup']
            dropoff = row['polygonID_dropoff']

            Rule1 = mileage in Process_Methods().frange(mileage_start, mileage_end, 0.01) if mileage_end > 0 else mileage > mileage_start
            if not Rule1:
                continue

            if pickup == 0:
                Rule2 = True
            elif pickup > 0:
                Rule2 = True if pickup in pickup_poly else False
            else:
                Rule2 = True if -pickup not in pickup_poly else False
            if not Rule2:
                continue

            if dropoff == 0:
                Rule3 = True
            elif dropoff > 0:
                Rule3 = True if dropoff in dropoff_poly else False
            else:
                Rule3 = True if -dropoff not in dropoff_poly else False
            if not Rule3:
                continue

            result.append(row['CodeName'])
            res_to_dict[row['CodeName']] = {
                'Code': row['Code'],
                'Modifier': row['CodeModifier']
            }

        return result, res_to_dict


class Decorator:

    def __init__(self):
        pass

    @staticmethod
    def clean_address(func):
        '''
        Decorator for cleaning address
        :param func:
        :return:
        '''
        def wrapper(address):
            return func(Process_Methods.clean_address(address))
        return wrapper

    @staticmethod
    def Google2Geo(func):
        def wrapper(address):
            return func(Process_Methods.Google2Geo(address))
        return wrapper

    @staticmethod
    def getPolygonID(func):
        def wrapper(address):
            return func(Process_Methods.getPolygonIDs(address))
        return wrapper


class MongoDB_Methods():
    '''
    Methods with MongoDB
    '''
    def __init__(self, localhost=False):
        mongo_uri = "mongodb://root:" + urllib.parse.quote('0p8rr@Fruit') + '@192.168.130.62:27017/operr_v3_dev'
        self.conn = MongoClient(mongo_uri)
        self.db = self.conn.operr_v3_dev
        self.mycollection = self.db.polygon_boundary_keyuan_copy

        if localhost == True:
            local_uri = "mongodb://127.0.0.1:27017"
            self.conn = MongoClient(local_uri)
            self.db = self.conn.polygon
            self.mycollection = self.db.polygon_local

    def getPolygonID(self, lng, lat):
        '''

        :param lng:
        :param lat:
        :return: Return all polygonIDs containing this geo point, list type
        '''
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
        self.conn.close()
        return result


class ProcedureCode_Rules():
    '''
    Methods for Code rules
    '''


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
        Get cooresponding codes data from database.
        Can use certain codes or all codes by default
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

    def check_address_in_cache(self, table, address):
        '''

        :param table: table name
        :param address: address
        :return: Lng, lat, PolygonIDs if address is in cache
        '''
        self.create_table_cache_address(table)
        self.cursor.execute(f'SELECT Longitude, Latitude, PolygonIDs FROM {table} WHERE Address="{address}"')
        resp = self.cursor.fetchone()
        return resp


class Process_MAS():
    def __init__(self, rawfile):
        assert (rawfile.endswith('.txt')), "HOUSTON, WE'VE GOT A PROBLEM HERE! \nONLY PROCESS TXT-FORMAT FILE!"
        self.P = Process_Methods()
        self.SQ = Sqlite_Methods('ProcedureCodes.db')

        self.raw_df = pd.read_table(rawfile)
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

        raw_df = self.Add_abcd_legs()
        raw_df['processed_pickup_address'] = raw_df['Pick-up Address'] + ", " + raw_df['Pick-up City'] + ", " + \
                                             raw_df['Pick-up State'] + " " + raw_df['Pick-up Zip']

        raw_df['processed_dropoff_address'] = raw_df['Drop-off Address'] + ", " + raw_df['Drop-off City'] + ", " + \
                                             raw_df['Drop-off State'] + " " + raw_df['Drop-off Zip']


        ####### TO DO #########
        ####### RULE DATAFRAME #########
        ####### SHOULD IMPROVE FOR DIFFERENT BASES AND RULES ##########
        ####### Applied to following function ##########
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

            calculated_codes = ",".join(procedureCodes)
            raw_df.ix[i, 'Calculated Codes'] = calculated_codes

            # Display progress
            count += 1
            if count % display_flag == 0:
                progress = int(round(count / count_legs, 1) * 100)
                print(f'PROCEDURE CODES ARE ADDING TO TRIPS......{progress}% COMPLETED.')

        temp_df['service_date'] = raw_df['Service Starts'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())

        min_service_date, max_service_date  = min(temp_df['service_date']), max(temp_df['service_date'])

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
        # basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, daily_folder)
        if not os.path.exists(file_saving_path):
            os.makedirs(file_saving_path)
            print('Save files to {0}'.format(file_saving_path))

        raw_df.to_excel(os.path.join(file_saving_path, 'Processed MAS-{0}-to-{1}.xlsx'.format(min_service_date, max_service_date)),
                            index=False)


class Signoff():
    def __init__(self):
        pass

    def signoff(self, processedMAS, TotalJob):

        signoff_df = pd.DataFrame()
        temp_df = pd.DataFrame()   #cache dataframe

        totalJob_df = pd.read_excel(TotalJob, header=None) if TotalJob.endswith('.xlsx') else pd.read_csv(TotalJob, header=None)
        totalJob_df.columns = ['FleetNumber', 'ServiceDate', 'CompanyCode', 'CustomerName', 'TripID', 'TollFee',
                                'Amount', 'pComany', 'pReserve', 'pPerson']
        totalJob_df = totalJob_df.dropna()

        if isinstance(processedMAS, pd.DataFrame):
            mas_df = processedMAS
        else:
            mas_df = pd.read_excel(processedMAS)

        # Drop service type
        service_idx = mas_df.loc[mas_df['Record Type'] == 'Service'].index
        mas_df = mas_df.drop(mas_df.index[service_idx])

        # Add driver id and vehicle id to TOTAL JOBS
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
                totalJob_df.ix[idx_in_totaljob[0], 'Codes'] = calculated_codes_in_MAS

                for code in calculated_codes_in_MAS.split(','):
                    if code == 'A0100':
                        MAS_amount += D('25.95')
                    elif code == 'A0100TN':
                        MAS_amount += D('35')
                    elif code == 'S0215':
                        MAS_amount += D('3.21') * (leg_mileage - D(8.0))
                    elif code == 'S0215TN':
                        MAS_amount += D('2.25') * leg_mileage
                    elif code == 'A0100SC':
                        MAS_amount += D('25')
                    else:
                        MAS_amount += D('0')

                totalJob_df.ix[idx_in_totaljob[0], 'MAS amount'] = float(MAS_amount)
                totalJob_df.ix[idx_in_totaljob[0], 'Difference'] = float(MAS_amount - \
                                                                         D(totalJob_df.ix[idx_in_totaljob[0], 'Amount']))

        current_path = os.getcwd()
        daily_folder = str(datetime.today().date())
        # basename = info_locker.base_info['BaseName']
        file_saving_path = os.path.join(current_path, daily_folder)
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

        signoff_df['SERVICE DAY'] = mas_df['Service Starts']
        signoff_df['INVOICE ID'] = mas_df['Invoice Number']
        signoff_df['LEG ID'] = mas_df['Leg ID'].astype(int)
        signoff_df['TOLL FEE'] = signoff_df['INVOICE ID'].apply(lambda x: get_tollfee_from_totaljob(x))
        signoff_df['PROCEDURE CODE'] = mas_df['Calculated Codes']
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


if __name__ == '__main__':

    # sq = Sqlite_Methods('ProcedureCodes.db')
    #
    # mileage = 18
    # pick_address = '287 Grand St, New York, NY 10002'
    # drop_address = '130-30 31st Ave, Flushing, NY 11354'
    #
    # p = Process_Methods()
    # df = sq.get_procedureCode_Rule_to_df('Rule')
    #
    # pick_poly = p.getPolygonIDs(pick_address)
    # drop_ploy = p.getPolygonIDs(drop_address)
    #
    # codes, _ = p.generate_procedureCodes(df, mileage, pick_poly, drop_ploy)
    # print(",".join(codes))
####################
    # p = Process_MAS('/Users/keyuanwu/Desktop/MACBACKUP/Merged_autobilling/0507/Vendor-31226-2018-05-07-09-55-59.txt')
    conn = sqlite3.connect('EDI.db')

    driver_df = pd.read_sql('SELECT * FROM driver_info WHERE Base="CLEAN AIR CAR SERVICE AND PARKING COR"', conn)
    driver_df.set_index(['Fleet'], inplace=True)
    dict_driver_df = driver_df.to_dict('index')
    info_locker.driver_information = dict_driver_df if dict_driver_df else None
    # print(info_locker.driver_information)

    # y = Signoff().signoff('./2018-05-16/Processed MAS-2018-03-26-to-2018-04-29.xlsx', '/Users/keyuanwu/Desktop/MACBACKUP/Merged_autobilling/0507/TOTAL JOBS 0326-0429.xlsx')

