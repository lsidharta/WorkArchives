"""
This script cleans and tidies up the data in a csv file:
- merge columns
- combine columns
- replace values
- trim whitespaces in strings
- add suffix (-1, -2, -3, ...) to the key of duplicates based on the date column 
"""

import pandas as pd
import numpy as np
import os
import re

# csv file name
filename = ""
# select columns
usedColumns = ['IAN', 'OfferID', 'CustRefID', 'loaddate', 'orderdate', 'firstname', 'lastname', 'addr1', 'city', 'province', 'postal', 'email_primary', 'email', 'phone', 'cell_phone', 'ItemDesc', 'OHstatus', 'ShipDate', 'Carrier', 'Tracking']
df = pd.read_csv(filename, usecols = usedColumns, parse_dates=['loaddate', 'orderdate', 'ShipDate'], dtype={'IAN':str, 'phone':str, 'cell_phone':str, 'Tracking':str})

# Change OfferID
df.loc[df.OfferID == 9999, 'OfferID'] = "FIFA"
df.loc[df.OfferID != "FIFA", 'OfferID'] = "GWP"

# merge addresses
df['postal'] = df['postal'].str.upper()
df['postal'] = df['postal'].str.replace(' ', '')
df['province'] = df['province'].str.upper()
df['province'] = df['province'].str.replace(' ', '')
df['addr'] = df['addr1'] + ';' + df['city'] + ';' + df['province'] + ';' + df['postal']
df.drop(['addr1', 'city', 'province', 'postal'], axis='columns', inplace=True)

# combine phone and cell-phone
np.where(df['cell_phone'].isnull(), df['phone'], df['cell_phone'])
# combine email and email_primary
np.where(df['email_primary'].isnull(), df['email'], df['email_primary'])
# fill None values with default email and default cellphone
default_email = "noemail@telus.net"
default_cellphone = "5555555555"
values = {'email_primary': default_email, 'cell_phone': default_cellphone}
df.fillna(value = values, inplace = True)
df.drop(['email', 'phone'], axis='columns', inplace=True)

# rename columns
colNames = {'OfferID':'program', 'CustRefID':'orderRefId', 'IAN':'inboundOrderId', 'orderdate':'inboundOrderDate', 'loaddate':'orderDate', 'firstname':'fname', 'lastname':'lname', 'email_primary':'email', 'cell_phone':'phone', 'ItemDesc':'sku', 'OHstatus':'status',  'ShipDate':'shipDate', 'Carrier':'shipper', 'Tracking':'trackingId'}
df.rename(columns=colNames, inplace=True)

# rearrange columns
reorderColumns = ['program','orderDate','shipDate','shipper','trackingId','status','orderRefId','inboundOrderId','inboundOrderDate','fname','lname','addr','phone','email','sku']
df = df[reorderColumns]

# update duplicate orderRedId

# find the duplicate orderRefId
list_orderRefId = df[df.duplicated(subset='orderRefId', keep=False)]['orderRefId'].values.tolist()
list_unique_orderRefId = list(set(list_orderRefId))

# change the orderRefId
for i in list_unique_orderRefId:
    temp = df.loc[df['orderRefId'] == i, :]
    temp = temp.sort_values(by='shipDate')
    j = 0
    for index in temp.index:
        df.loc[index, 'orderRefId'] = i + '-' + str(j)
        j += 1

# remove '-0' from orderRefId
df['orderRefId'] = df['orderRefId'].str.replace('-0', '')

# save to file
