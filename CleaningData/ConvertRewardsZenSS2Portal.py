"""
Zenventory export:
- Placed On         - orderDate (DATETIME)
- Completed On      - status
- Already Shipped
- CO#               - orderRefId
- Name              - fname
- Surname           - lname
- Shipping Address 1- addr
- Shipping City
- Shipping State
- Shipping Zip
- Shipping Phone    - phone
- Customer Email    - email
- SKU               - sku
Clapton inbound:
- TRANSACTION ID    - inboundOrderId
- REDEMPTION DATE   - inboundOrderDate (DATE)
ShipStation:
- orderNumber       - orderRefId
- ShipDate          - shipDate (DATE)
- trackingNumber    - trackingId
- carrierCode       - shipper

Process:
1. dfzen <== tidy the exported data from Zenventory
2. dfclap <== tidy the data from Claptop inbound - to extract the REDEMPTION DATE
3. dfss <== get the data from ShipStation - to update the shipDate, shipper, tracking number
4. merge inner (inner join) dfzen and dfclap into dfzenclap to include REDEMPTION DATE
5. merge left (left outer join) dfzenclap and dfss into dfzenclapss to update the Shipdate, Tracking number and Shipper
6. rearrange columns
"""
import pandas as pd
import numpy as np
import csv
import os
import re
import json
import requests 
from requests.auth import HTTPBasicAuth 

def tidy_zen_data():
    # read zenventory export order details from ZenExportOrderDetails_20201201.csv
    usedColumns = ['Placed On', 'Completed On', 'Already Shipped', 'CO#', 'Name', 'Surname', \
        'Shipping Address 1', 'Shipping City', 'Shipping State', 'Shipping Zip', 'Shipping Phone', 'Customer Email', \
        'SKU']
    dfzen = pd.read_csv("telus/ZenExportOrderDetails_20201201.csv", usecols=usedColumns, index_col=None, \
        parse_dates=['Placed On', 'Completed On'], dtype={'Shipping Phone':str})

    # create column 'status' and create data based on 'Completed On' and 'Already Shipped'
    # create 'inboundOrderId' column from 'CO#'
    dfzen['status'] = ""
    dfzen.loc[(dfzen['Already Shipped']==0) & (dfzen['Completed On']=='Incomplete'), 'status'] = "pend"
    dfzen.loc[(dfzen['Already Shipped']==0) & (dfzen['Completed On']=='Cancelled'), 'status'] = "canc"
    dfzen.loc[(dfzen['Already Shipped']==1), 'status'] = "ship"

    # create 'inboundOrderId' from 'CO#'
    dfzen['inboundOrderId'] = dfzen['CO#'].str.replace('CR', '')

    # create column 'addr'
    dfzen['addr'] = dfzen['Shipping Address 1'] + ';' + dfzen['Shipping City'] + ';' + dfzen['Shipping State'] + ';' + dfzen['Shipping Zip']

    # create column 'program'
    dfzen['program'] = "0"

    # delete unused columns
    dfzen.drop(['Completed On', 'Already Shipped', 'Shipping Address 1', 'Shipping City', 'Shipping State', 'Shipping Zip'], axis='columns', inplace=True)

    # rename columns
    colNames = {'Placed On':'orderDate','CO#':'orderRefId',\
        'Name':'fname','Surname':'lname','Shipping Phone':'phone','Customer Email':'email', 'SKU':'sku'}
    dfzen.rename(columns=colNames, inplace=True)

    return(dfzen)

def tidy_clapton_inbound():
    file_path = r"C:\\Users\\lsidh\Documents\\Projects\\QMedia\\TelusRewards_Inbound_Outbound_WebPortal\\Inbound"
    usecolums = ['TRANSACTION ID', 'REDEMPTION DATE']

    # find the files from August to November
    files = os.listdir(file_path)
    filtered_tr = []
    for file in files:
        with open(file_path+r"\\"+file, mode='r') as csv_file:
            for row in csv.DictReader(csv_file, skipinitialspace=True):
                filtered_row = [row[i] for i in usecolums if i in row]
                filtered_tr.append(filtered_row)

    # rename columns
    usecolums = ['inboundOrderId', 'inboundOrderDate']
    dfclapton = pd.DataFrame(filtered_tr, columns=usecolums)

    return(dfclapton)

def get_shipstation_shipments():

    ShipStation_username = ''
    ShipStation_password = ''
    ShipStation_API_get_shipments = r"HTTPS://ssapi.shipstation.com/shipments?pageSize=500&shipDateStart="
    startdate = "2020-08-01"
    enddate = "2020-11-30"
    usecolums = ['orderNumber', 'shipDate', 'trackingNumber', 'carrierCode']

    url = ShipStation_API_get_shipments + startdate + "&shipDateEnd=" + enddate + "&orderNumber=CR"
    try:
        response = requests.get(url, auth = HTTPBasicAuth(ShipStation_username, ShipStation_password))
        decoded_json = json.loads(response.content)
        shipments = decoded_json['shipments']
        if (len(shipments) > 0):
            #orders.extend(shipments)
            num_pages = decoded_json["pages"]
            if (num_pages > 1):
                for page in range(2, num_pages+1):
                    #print page
                    url_page = url + "&page=%s"%page
                    #print url_page
                    response = requests.get(url_page, auth = HTTPBasicAuth(ShipStation_username, ShipStation_password))
                    decoded_json = json.loads(response.content)
                    shipments.extend(decoded_json["shipments"])   # shipments is list of list
    except requests.RequestException as e:
        print ("Error HTTP Requests to ShipStation %s" % e)
    return(shipments)

def tidy_shipstation():
    # get the list of shipments 
    shipments = get_shipstation_shipments()
    # filter the columns
    filtered_shipments = []
    usecolums = ['orderNumber', 'shipDate', 'trackingNumber', 'carrierCode']
    for row in shipments:
        filtered_row = [row[i] for i in usecolums if i in row] 
        filtered_shipments.append(filtered_row)
    # rename the columns
    usecolums = ['orderRefId', 'shipDate', 'trackingId', 'shipper']
    dfss = pd.DataFrame(filtered_shipments, columns=usecolums)
    dfss.loc[dfss['shipper']=='canada_post', 'shipper'] = "Canada Post"
    dfss.loc[dfss['shipper']=='purolator', 'shipper'] = "Purolator"
    return dfss

# tidy data exported from zenventory
dfzen = tidy_zen_data()
# tidy data inbound from clapton
dfclap = tidy_clapton_inbound()
# inner join 
dfzenclap = pd.merge(dfzen, dfclap, how='inner', on='inboundOrderId')

# tidy shipstation
dfss = tidy_shipstation()

# get shipment info of orders crated in Zenventory
dfzenclapss = pd.merge(dfzenclap, dfss, how='left', on='orderRefId')

# update status
for index in dfzenclapss.index:
    if (pd.isnull(dfzenclapss.loc[index,'trackingId'])) & (pd.isnull(dfzenclapss.loc[index,'shipDate'])) & (dfzenclapss.loc[index,'status']=='pend'):
        dfzenclapss.loc[index,'status'] = "pend"
#dfzenclapss.loc[((dfzenclapss['trackingId'])>0) & (len(dfzenclapss['shipDate'])>0 & (dfzenclapss['status']=='pend')), 'status'] = "ship"
# reorder columns
reorderColumns = ['program','orderDate','shipDate','shipper','trackingId','status','orderRefId','inboundOrderId','inboundOrderDate','fname','lname','addr','phone','email','sku']
dfzenclapss = dfzenclapss[reorderColumns]
dfzenclapss.to_excel("telus/ClaptonZenShipMergeInner.xlsx", index=False)
dfzenclapss.to_csv("telus/ClaptonZenShipMergeInner.csv", index=False)
