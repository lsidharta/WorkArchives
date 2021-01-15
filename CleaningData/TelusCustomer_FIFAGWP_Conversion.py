"""
Convert export of TelusCustomer table to FIFACustomersMaster.csv and FIFACustomerOrders.csv, GWPCustomersMaster.csv and GWPCustomerOrders.csv
"""

import os
import csv
import pandas as pd
import numpy as np
import shutil
import re
import string
from zipfile import ZipFile, ZipInfo
from datetime import datetime
import dateutil
import logging
import tkinter as tk
from tkinter import filedialog

DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_DATE_FORMAT = '%Y-%m-%d'

def read_xlsx_database_filename(fname_xlsxdb):
    use_cols = ['TelusCustomerID', 'offer', 'IAN', 'phone', 'orderdate', 'active', 'firstname', \
        'lastname', 'addr1', 'addr2', 'city', 'province', 'postal', 'email', 'loaddate', 'cell_phone', \
        'email_primary', 'OfferID', 'CustomerID', 'ServiceAgreementId', 'Additional1', 'Additional2']
    date_cols = ['orderdate', 'loaddate']
    type_cols = {'IAN': 'object', 'phone': 'object', 'cell_phone': 'object', 'OfferID': 'object', \
        'CustomerID': 'object', 'ServiceAgreementId': 'object'}
    # read xlsx file
    df_origin = pd.read_excel(fname_xlsxdb, usecols=use_cols, parse_dates=date_cols, dtype=type_cols)

    return (df_origin)

def read_xlsx_database(folder_csvdb, fname_xlsxdb):
    use_cols = ['TelusCustomerID', 'offer', 'IAN', 'phone', 'orderdate', 'active', 'firstname', \
        'lastname', 'addr1', 'addr2', 'city', 'province', 'postal', 'email', 'loaddate', 'cell_phone', \
        'email_primary', 'OfferID', 'CustomerID', 'ServiceAgreementId', 'Additional1', 'Additional2']
    date_cols = ['orderdate', 'loaddate']
    type_cols = {'IAN': 'object', 'phone': 'object', 'cell_phone': 'object', 'OfferID': 'object', \
        'CustomerID': 'object', 'ServiceAgreementId': 'object'}
    # read xlsx file
    df_origin = pd.read_excel('/'.join([folder_csvdb, fname_xlsxdb]), usecols=use_cols, parse_dates=date_cols, dtype=type_cols)

    return (df_origin)

def read_csv_database(folder_csvdb, fname_csvdb):
    use_cols = ['TelusCustomerID', 'offer', 'IAN', 'phone', 'orderdate', 'active', 'firstname', \
        'lastname', 'addr1', 'addr2', 'city', 'province', 'postal', 'email', 'loaddate', 'cell_phone', \
        'email_primary', 'OfferID', 'CustomerID', 'ServiceAgreementId', 'Additional1', 'Additional2']
    date_cols = ['orderdate', 'loaddate']
    type_cols = {'IAN': 'object', 'phone': 'object', 'cell_phone': 'object', 'OfferID': 'object', \
        'CustomerID': 'object', 'ServiceAgreementId': 'object'}
    # read xlsx file
    df_origin = pd.read_csv('/'.join([folder_csvdb, fname_csvdb]), usecols=use_cols, parse_dates=date_cols, dtype=type_cols)

    return (df_origin)

def convert_database(df_origin):
    global DEFAULT_DATETIME_FORMAT, DEFAULT_DATE_FORMAT
    # add Filename columns
    df_origin['Filename'] = None
    df_origin['OrderRefID'] = None
    df_origin['ZenCustOrderID'] = None
    df_origin['ZenPlaceOnDate'] = None
    df_origin['ZenCanceledDate'] = None
    df_origin['ZenCompleteDate'] = None
    df_origin['SSShipDate'] = None
    df_origin['SSTrackingNumber'] = None
    df_origin['SSShipper'] = None
    df_origin['OrderStatus'] = None
    df_origin['ModifiedDate'] = None     
    # reorder columns
    use_cols = ['TelusCustomerID', 'OfferID', 'offer', 'orderdate', \
        'CustomerID', 'ServiceAgreementId', 'addr1', 'addr2', 'city', 'province', 'postal', \
        'email', 'firstname', 'lastname', 'cell_phone', 'phone', 'email_primary', \
        'Additional1', 'Additional2', 'IAN', 'active', 'Filename', 'loaddate', 'OrderRefID', \
        'ZenCustOrderID', 'ZenPlaceOnDate', 'ZenCanceledDate', 'ZenCompleteDate', 'SSShipDate', \
        'SSTrackingNumber', 'SSShipper', 'OrderStatus', 'ModifiedDate']
    df_reordered = df_origin[use_cols]
    # rename columns
    new_name = {'offer':'PromoCode', 'orderdate':'DateSent', 'ServiceAgreementId':'SvcAgreeID', \
        'addr1':'Address1', 'addr2':'Address2', 'city':'City', 'province':'PR', 'postal':'Postal', \
        'email':'EmailAddress', 'firstname':'Firstname', 'lastname':'Lastname', 'cell_phone':'CellPhone', \
        'phone':'HomePhone', 'email_primary':'EmailPrimary', 'active':'Active', 'loaddate':'LoadDate'}
    df_reordered.rename(columns=new_name, inplace=True)
    # FIFA Customers Master and GWP Customers Master
    df_fifa = df_reordered.loc[df_reordered['OfferID'] == 9999]
    df_gwp = df_reordered.loc[df_reordered['OfferID'] != 9999]
    df_fifa.rename(columns={'TelusCustomerID':'FIFACustomerID'}, inplace=True)
    df_gwp.rename(columns={'TelusCustomerID':'GWPCustomerID'}, inplace=True)
    # Eliminate unneeded columns
    customers_drop_cols = ['IAN', 'OrderRefID', 'ZenCustOrderID', 'ZenPlaceOnDate', 'ZenCanceledDate', \
        'ZenCompleteDate', 'SSShipDate', 'SSTrackingNumber', 'SSShipper', 'OrderStatus', 'ModifiedDate']
    df_fifa_customers = df_fifa.drop(customers_drop_cols, axis=1)
    df_gwp_customers = df_gwp.drop(customers_drop_cols, axis=1)
    df_fifa_orders = df_fifa.drop(['Filename'], axis=1)
    df_gwp_orders = df_gwp.drop(['Filename'], axis=1)

    # add one dummy data with teluscustomerid = 0
    FIFA_dict_dummy = {'FIFACustomerID': 0, \
        'OrderRefID': "CR3000000", \
        'IAN': "00", \
        'DateSent': datetime.today().strftime(DEFAULT_DATETIME_FORMAT), \
        'Active': False, \
        'OfferID': 0, \
        'PromoCode': "TEST", \
        'DateSent': datetime.today().strftime(DEFAULT_DATE_FORMAT), \
        'CustomerID': 0, \
        'SvcAgreeID': 0, \
        'Address1': "TEST", \
        'Address2': None, \
        'City': "TEST", \
        'PR': "BC", \
        'Postal': "V0V0V0", \
        'EmailAddress': "noemail@telus.net", \
        'Firstname': "TEST", \
        'Lastname': "TEST", \
        'CellPhone': "5555555555", \
        'HomePhone': "5555555555", \
        'EmailPrimary': "noemail@telus.net", \
        'Additional1': None, \
        'Additional2': None}
    GWP_dict_dummy = {'GWPCustomerID': 0, \
        'OrderRefID': "CR4000000", \
        'IAN': "00", \
        'DateSent': datetime.today().strftime(DEFAULT_DATETIME_FORMAT), \
        'Active': False, \
        'OfferID': 0, \
        'PromoCode': "TEST", \
        'DateSent': datetime.today().strftime(DEFAULT_DATE_FORMAT), \
        'CustomerID': 0, \
        'SvcAgreeID': 0, \
        'Address1': "TEST", \
        'Address2': None, \
        'City': "TEST", \
        'PR': "BC", \
        'Postal': "V0V0V0", \
        'EmailAddress': "noemail@telus.net", \
        'Firstname': "TEST", \
        'Lastname': "TEST", \
        'CellPhone': "5555555555", \
        'HomePhone': "5555555555", \
        'EmailPrimary': "noemail@telus.net", \
        'Additional1': None, \
        'Additional2': None}
    #df_dummy = pd.DataFrame(dict_dummy)
    df_fifa_orders = df_fifa_orders.append(FIFA_dict_dummy, ignore_index=True)
    df_gwp_orders = df_gwp_orders.append(GWP_dict_dummy, ignore_index=True)
    return (df_fifa_customers, df_gwp_customers, df_fifa_orders, df_gwp_orders)

"""
main program
"""
# Find file
root = tk.Tk()
root.withdraw()
fname_teluscustomerdb = filedialog.askopenfilename()

folder_fifacsvdb = r"C:\\Users\\lsidh\\Python\\QMedia\\FIFAdb"
folder_gwpcsvdb = r"C:\\Users\\lsidh\\Python\\QMedia\\GWPdb"
# Steve's database
#fname_teluscustomerdb = r"TelusCustomer_20210104.xlsx" #TelusCustomers6Months.xlsx
# csv database to use
#df1 = read_xlsx_database(folder_csvdb, fname_teluscustomerdb)
df1 = read_xlsx_database_filename(fname_teluscustomerdb)
df_fifa_customers, df_gwp_customers, df_fifa_orders, df_gwp_orders = convert_database(df1)
# save to the new database
fname_csvdb = "FIFACustomersMaster" + datetime.today().strftime("%Y%m%d") + ".csv"#%Y-%m-%d
df_fifa_customers.to_csv('/'.join([folder_fifacsvdb, fname_csvdb]), index=False, encoding='utf-8')
fname_csvdb = "GWPCustomersMaster" + datetime.today().strftime("%Y%m%d") + ".csv"#%Y-%m-%d
df_gwp_customers.to_csv('/'.join([folder_gwpcsvdb, fname_csvdb]), index=False, encoding='utf-8')
fname_csvdb = "FIFACustomerOrders" + datetime.today().strftime("%Y%m%d") + ".csv"#%Y-%m-%d
df_fifa_orders.to_csv('/'.join([folder_fifacsvdb, fname_csvdb]), index=False, encoding='utf-8')
fname_csvdb = "GWPCustomersOrders" + datetime.today().strftime("%Y%m%d") + ".csv"#%Y-%m-%d
df_gwp_orders.to_csv('/'.join([folder_gwpcsvdb, fname_csvdb]), index=False, encoding='utf-8')
