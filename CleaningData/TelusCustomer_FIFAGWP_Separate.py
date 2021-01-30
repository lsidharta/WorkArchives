"""
TelusCustomer_FIFAGWP_Separate.py

Input: XLSX exported from TelusCustomers of FFDB
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
    use_cols = ['TelusCustomerID', 'offer', 'phone', 'orderdate', 'active', \
        'firstname', 'lastname', 'addr1', 'addr2', 'city', \
        'province', 'postal', 'email', 'loaddate', 'cell_phone', \
        'email_primary', 'OfferID', 'CustomerID', 'ServiceAgreementId', 'Additional1', \
        'Additional2', 'IAN'] # 22 columns
    date_cols = ['orderdate', 'loaddate']
    type_cols = {'addr2': object, 'phone': object, 'cell_phone': object, 'OfferID': object, 'CustomerID': object, \
        'ServiceAgreementId': object, 'Additional1': object, 'Additional2': object, 'IAN': object}
    # read xlsx file
    df_origin = pd.read_excel(fname_xlsxdb, usecols=use_cols, parse_dates=date_cols, dtype=type_cols)
    return (df_origin)

def read_csv_database(folder_csvdb, fname_csvdb):
    use_cols = ['TelusCustomerID', 'offer', 'IAN', 'phone', 'orderdate', 'active', 'firstname', \
        'lastname', 'addr1', 'addr2', 'city', 'province', 'postal', 'email', 'loaddate', 'cell_phone', \
        'email_primary', 'OfferID', 'CustomerID', 'ServiceAgreementId', 'Additional1', 'Additional2']
    date_cols = ['orderdate', 'loaddate']
    type_cols = {'IAN': object, 'phone': object, 'cell_phone': object, 'OfferID': object, \
        'CustomerID': object, 'ServiceAgreementId': object}
    # read xlsx file
    df_origin = pd.read_csv('/'.join([folder_csvdb, fname_csvdb]), usecols=use_cols, parse_dates=date_cols, dtype=type_cols)

    return (df_origin)

def convert_database(df_origin):
    global DEFAULT_DATETIME_FORMAT, DEFAULT_DATE_FORMAT
    # reorder columns
    use_cols = ['TelusCustomerID', 'OfferID', 'offer', 'orderdate', 'CustomerID', \
        'ServiceAgreementId', 'addr1', 'addr2', 'city', 'province', \
        'postal', 'email', 'firstname', 'lastname', 'cell_phone', \
        'phone', 'email_primary', 'Additional1', 'Additional2', 'IAN', 'active', \
        'loaddate'] # 21 columns
    df_origin = df_origin[use_cols]
    # rename columns
    new_name = {'offer':'PromoCode', 'orderdate':'DateSent', 'ServiceAgreementId':'SvcAgreeID', \
        'addr1':'Address1', 'addr2':'Address2', 'city':'City', 'province':'PR', 'postal':'Postal', \
        'email':'EmailAddress', 'firstname':'Firstname', 'lastname':'Lastname', 'cell_phone':'CellPhone', \
        'phone':'HomePhone', 'email_primary':'EmailPrimary', 'active':'Active', 'loaddate':'LoadDate'}
    df_origin.rename(columns=new_name, inplace=True)
    # Add Filename columns
    df_origin['Filename'] = "Fulfilment20_TelusCustomer"
    # Format the LoadDate to datetime
    df_origin['LoadDate'] = df_origin['LoadDate'].dt.strftime(DEFAULT_DATETIME_FORMAT)
    print(df_origin.info())
    # Separate into FIFA Customers Master and GWP Customers Master
    df_fifa = df_origin.loc[df_origin['OfferID'] == 9999].copy()
    df_gwp = df_origin.loc[df_origin['OfferID'] != 9999].copy()
    df_fifa.rename(columns={'TelusCustomerID':'FIFACustomerID'}, inplace=True)
    df_gwp.rename(columns={'TelusCustomerID':'GWPCustomerID'}, inplace=True)
    return (df_fifa, df_gwp)

"""
main program
"""
# Find file
root = tk.Tk()
root.withdraw()
fname_teluscustomerdb = filedialog.askopenfilename()

folder_fifacsvdb = r"C:\\Users\\lsidh\\Python\\QMedia\\FIFAdb"
folder_gwpcsvdb = r"C:\\Users\\lsidh\\Python\\QMedia\\GWPdb"
df1 = read_xlsx_database_filename(fname_teluscustomerdb)
print(df1.info())
df_fifa_customers, df_gwp_customers = convert_database(df1)
# save to the new database
fname_csvdb = "FIFACustomers" + datetime.today().strftime("%Y%m%d") + ".csv"#%Y-%m-%d
df_fifa_customers.to_csv('/'.join([folder_fifacsvdb, fname_csvdb]), index=False, encoding='utf-8')
fname_csvdb = "GWPCustomers" + datetime.today().strftime("%Y%m%d") + ".csv"#%Y-%m-%d
df_gwp_customers.to_csv('/'.join([folder_gwpcsvdb, fname_csvdb]), index=False, encoding='utf-8')
