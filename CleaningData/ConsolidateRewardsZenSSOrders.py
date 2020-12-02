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
    dfzen['inboundOrderId'] = dfzen['CO#'].replace('CR', '')

    # create column 'addr'
    dfzen['addr'] = dfzen['Shipping Address 1'] + ';' + dfzen['Shipping City'] + ';' + dfzen['Shipping State'] + ';' + dfzen['Shipping Zip']

    # create column 'program'
    dfzen['program'] = "Rewards"

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

    ShipStation_username = '-'
    ShipStation_password = '-'
    ShipStation_API_get_shipments = r"-"
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
        logging.error ("Error HTTP Requests to ShipStation %s" % e)
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

    return dfss

dfzen = tidy_zen_data()
print ("dfzen: " + str(dfzen.shape))
print (dfzen.columns)
dfclap = tidy_clapton_inbound()
print ("dfclap: " + str(dfclap.shape))
print (dfclap.columns)
# get orders which are created in Zenventory
dfzenclap = pd.merge(dfzen, dfclap, how='left', on='inboundOrderId')
print ("dfzenclap: " + str(dfzenclap.shape))
dfss = tidy_shipstation()
print ("dfss: " + str(dfss.shape))
# get shipment info of orders crated in Zenventory
dfzenclapss = pd.merge(dfzenclap, dfss, how='left', on='orderRefId')
print ("dfzenclapss: " + str(dfzenclapss.shape))
# update status
for index in dfzenclapss.index:
    if (pd.isnull(dfzenclapss.loc[index,'trackingId'])) & (pd.isnull(dfzenclapss.loc[index,'shipDate'])) & (dfzenclapss.loc[index,'status']=='pend'):
        dfzenclapss.loc[index,'status'] = "pend"
#dfzenclapss.loc[((dfzenclapss['trackingId'])>0) & (len(dfzenclapss['shipDate'])>0 & (dfzenclapss['status']=='pend')), 'status'] = "ship"
# reorder columns
reorderColumns = ['program','orderDate','shipDate','trackingId','status','orderRefId','inboundOrderId','fname','lname','addr','phone','email','sku']
dfzenclapss = dfzenclapss[reorderColumns]
dfzenclapss.to_excel("telus/ClaptonZenShipMergeInner.xlsx", index=False)
