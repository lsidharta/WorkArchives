"""
input:              output:
1. IAN              - inboundOrderId
2. OfferID          - program
3. CustRefID        - orderRefId
4. loaddate         - orderDate (DATETIME)
5. orderdate        - inboundOrderDate (DATE)
6. firstname        - fname
7. lastname         - lname
8. addr1            - addr (addr1;city;province;postal)
9. city 
10. province 
11. postal 
12. email_primary   - email (email_primary or email)
13. email 
14. phone           - phone (cell_phone or phone)
15. cell_phone 
16. ItemDesc        - sku
17. OHstatus        - status
18. ShipDate        - shipDate (DATE)
19. Carrier         - shipper
20. Tracking        - trackingId

Process:
- change the format of datetime to date
- create addr
- create phone
- create email
- rename columns
- rearrange columns
- duplicated orderRedId, add suffix -1, -2, -3 based on chronological order
"""
import pandas as pd
import numpy as np
import os
import re

# file path 
folder = "/telus"
filename = "telus/TelusCustomers6MonthsWithCRandTrackingV2.xlsx"

# select columns
usedColumns = ['IAN', 'OfferID', 'CustRefID', 'loaddate', 'orderdate', 'firstname', 'lastname', 'addr1', 'city', 'province', 'postal', 'email_primary', 'email', 'phone', 'cell_phone', 'ItemDesc', 'OHstatus', 'ShipDate', 'Carrier', 'Tracking']
#df = pd.read_csv(filename, usecols = usedColumns, parse_dates=['loaddate', 'orderdate', 'ShipDate'], dtype={'IAN':str, 'phone':str, 'cell_phone':str, 'Tracking':str})
df = pd.read_excel(filename, sheet_name="Sheet1", usecols = usedColumns, parse_dates=['loaddate','orderdate','ShipDate'], dtype={'IAN':str,'phone':str,'cell_phone':str,'Tracking':str,'OfferID':str})

# change the format of datetime to date
df['ShipDate'] = df['ShipDate'].dt.date
df['orderdate'] = df['orderdate'].dt.date

# create addr from 
df['postal'] = df['postal'].str.upper()
df['postal'] = df['postal'].str.replace(' ', '')
df['province'] = df['province'].str.upper()
df['province'] = df['province'].str.replace(' ', '')
df['addr'] = df['addr1'] + ';' + df['city'] + ';' + df['province'] + ';' + df['postal']
df.drop(['addr1', 'city', 'province', 'postal'], axis='columns', inplace=True)

# create phone; use cell-phone if available, else use phone
np.where(df['cell_phone'].isnull(), df['phone'], df['cell_phone'])
# create email; use email_primary if available, else use email
np.where(df['email_primary'].isnull(), df['email'], df['email_primary'])
# fill Null values with default email and default cellphone
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

# duplicated orderRedId, add suffix
# find the duplicate orderRefId
list_orderRefId = df[df.duplicated(subset='orderRefId', keep=False)]['orderRefId'].values.tolist()
list_unique_orderRefId = list(set(list_orderRefId))
# update the orderRefId, add suffix with -1, -2, ... based of chronological order
for i in list_unique_orderRefId:
    temp = df.loc[df['orderRefId'] == i, :]
    temp = temp.sort_values(by='shipDate')
    j = 0
    for index in temp.index:
        df.loc[index, 'orderRefId'] = i + '-' + str(j)
        j += 1
# remove '-0' from orderRefId
df['orderRefId'] = df['orderRefId'].str.replace('-0', '')
# save
df.to_csv('telus/TelusCustomers6MonthsV2Clean.csv', index=False)

"""
cr = ['CR2379836', 'CR2379945', 'CR2380052', 'CR2384279', 'CR2385188', 'CR2385770', 'CR2382612']
filtered_df = df[df.orderRefId.isin(cr)]
filtered_df.to_csv('telus/TelusCustomers6MonthsV2Filtered.csv', index=False)
"""