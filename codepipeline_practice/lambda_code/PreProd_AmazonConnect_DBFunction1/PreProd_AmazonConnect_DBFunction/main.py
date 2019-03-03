
# A lambda function to interact with AWS RDS MySQL
from __future__ import print_function
from UserHistory import *
from FetchTransactionInfo import *
import pymysql
import sys
import boto3
import os
import time

REGION = 'us-east-1'
rds_host= "rds-cnvacpaur01-cluster.cluster-cap97krsw3kz.us-east-1.rds.amazonaws.com"
name = "svcacpaurdb"
password = "Ka07EeSSxeT5guyBxxpz"
db_name = "POCDBAurora"
conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    
def main(event, context):
    print ("Event data is :")
    print (event)
    #functionName='Fetch'
    functionName = event["FunctionName"]
    if functionName == 'FetchCardBalance':
        print("inside fetchbalance")
        result = FetchCardBalance(event)
        CardBalance=result[0][0]
        CardNumber=result[0][1]
        response={"CardNumber":CardNumber,"CardBalance":CardBalance}
        return response
    if functionName == 'FetchAmountSpentOnVendorClass':
        print("inside FetchAmountSpentOnVendorClass")
        result = FetchAmountSpentOnVendorClass(event)
        if not result:
            response={"Amount":"0"} 
        else:
            Amount=int(result[0][0])
            print (Amount)
            response={"Amount":Amount}
        return response
    elif functionName == 'FetchTransactionList':
        print("inside FetchTransactionList")
        result = FetchTransactionList(event)
        #CardBalance=result[0][0]
        responseArray = []
        #response={"CardNumber":CardNumber,"CardBalance":CardBalance}
        print ('result is :')
        print (len(result))
        for item in result:
            #print ('item is :')
            #print (item)
            transactionid= item[0]
            vendorName = item[1]
            amount=item[2]
            date=item[3]
            card=item[4]
            TransactionType=item[5]
            response={"transactionId":transactionid,"vendorName":vendorName,"amount":amount,"DT":date,"card":card,"TransactionType":TransactionType}
            responseArray.append(response)
        print ('responseArray is :------------------------------------')
        print (responseArray)
        return responseArray
    elif functionName == 'FetchTransactionsByType':
        result = FetchTransactionsByType(event)
        startDate=event["startDate"]
        responseArray = []
        if (startDate == 'null'):
            dateAvailable = False;
        else:
            dateAvailable = True;
        if (dateAvailable==False):
            for item in result:
                #print ('item is :')
                #print (item)
                transactionid= item[0]
                vendorName = item[1]
                amount=item[2]
                date=item[3]
                card=item[4]
                TransactionType=item[5]
                response={"transactionId":transactionid,"vendorName":vendorName,"amount":amount,"DT":date,"card":card,"TransactionType":TransactionType}
                responseArray.append(response)
        else:
            print(result)
            
            if not result:
                response = {"Amount":"0"}
            else:
                Amount=result[0]
                response = {"Amount":Amount}
            responseArray.append(response)
            print(responseArray);
        print ('responseArray is :------------------------------------')
        return responseArray
    elif functionName == 'FetchAccountId':
        print("inside fetchaccountid")
        result = FetchAccountId(event)
        AccountId=result[0][0]
        response={"AccountId":AccountId}
        return response
    elif functionName == 'FetchVendorSpent':
        #function to fetch the total amount spend on a particular vendor for a period of time
        print("inside FetchVendorSpent")
        result = FetchAmountSpentOnVendor(event)
        if not result:
            response={"Amount":"0"} 
        else:
            Amount=int(result[0][0])
            print (Amount)
            response={"Amount":Amount} 
        return response
    elif functionName == 'FetchNetEarnings':
        print("inside FetchNetEarnings")
        result = FetchNetEarnings(event)
        Amount=int(result[0])
        response={"Amount": Amount} 
        return response
    elif functionName == 'FetchEventInfo':
        print("inside FetchNetEarnings")
        result = FetchInfo(event)
        print (result)
        return result
    elif functionName == 'UpdateEventInfo':
        print("inside UpdateEventInfo")
        result = UpdateInfo(event)
        return result
    elif functionName == 'PayCardBalance':
        result = PayCardBalance(event)
        return {"result":result}
        
