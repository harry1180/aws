
# A lambda function to interact with AWS RDS MySQL
from __future__ import print_function
from UserHistory import *
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
 

def FetchTransactionList(event):
    result = []
    customerId=event["CustomerId"]
    VendorName=event["vendor"]
    #customerId = '105';
    step=event["Step"]
    #step='2';
    limit='5';
    limit_number = int(limit)
    stepCount = (int(step)-1)*limit_number;
    stepNumber = str(stepCount);
    with conn.cursor() as cur:
        if VendorName == 'all':
            cur.execute("""SELECT idTransaction,MerchantName,Ammount,DATE_FORMAT(DateTime, "%M %e %Y %k %i %s"),CardNumber,TransactionType FROM BankDBAurora.Transaction as tt WHERE  tt.CustomerId = """+customerId+  """ ORDER BY DateTime DESC LIMIT """+limit+""" OFFSET """+ stepNumber);
        else:
            cur.execute("""SELECT idTransaction,MerchantName,Ammount,DATE_FORMAT(DateTime, "%M %e %Y %k %i %s"),CardNumber,TransactionType FROM BankDBAurora.Transaction as tt WHERE UPPER(tt.MerchantName) = UPPER('""" + VendorName + """')  AND  tt.CustomerId = """+customerId+  """ ORDER BY DateTime DESC LIMIT """+limit+""" OFFSET """+ stepNumber);
        #cur.execute("""SELECT idTransaction,MerchantName,Ammount,DATE_FORMAT(DateTime, "%M %e %Y %k %i %s"),CardNumber,TransactionType FROM BankDBAurora.Transaction as tt WHERE  tt.CustomerId = """+customerId+  """ ORDER BY DateTime DESC LIMIT """+limit+""" OFFSET """+ stepNumber);
        conn.commit()
        cur.close()
        for row in cur:
            result.append(list(row))
        print ("Data from FetchTransactionList...");
        print (result)
    return result
#----------------------------------------------FetchNetEarnings start-------------------------------------------#

def FetchNetEarnings(event):
    total=[];
    earning = 0;
    result = []
    customerId=event["CustomerId"]
    startDate=event["startDate"]
    endDate=event["endDate"]
    '''customerId='105'
    startDate = '2018-05-23'
    endDate = '2018-06-26'''
    with conn.cursor() as cur:
        cur.execute("""SELECT SUM(Ammount),TransactionType FROM BankDBAurora.Transaction as tt 
                        WHERE  tt.CustomerId = """+customerId+  """ 
                        AND tt.DateTime BETWEEN '""" + startDate + """' AND '""" + endDate + """' GROUP BY tt.TransactionType;""");
        conn.commit()
        cur.close()
        for row in cur:
            result.append(list(row))
        for r in result:
            if r[1] == 'Debit':
                earning = earning - r[0]
            if r[1] == 'Credit':
                earning = earning + r[0]
            if r[1] == 'Withdrawal':
                earning = earning - r[0]
            if r[1] == 'Deposit':
                earning = earning + r[0]
        total.append(earning)
    return total
#----------------------------------------------FetchNetEarnings End-------------------------------------------#

#------------------------------------FetchTransactionsByType Start---------------------------------------#
def FetchTransactionsByType(event):
    result = []
    customerId=event["CustomerId"]
    transactionType=event["TransactionType"]
    startDate=event["startDate"]
    endDate=event["endDate"]
    '''customerId='105'
    startDate = 'null'
    #startDate = '2018-05-23'
    endDate = '2018-06-26'
    transactionType = 'Debit'''
    '''dateAvailable = True;'''
    if (startDate == 'null'):
        dateAvailable = False;
    else:
        dateAvailable = True;
    print ("Data from FetchTransactionsByType..False.");
    with conn.cursor() as cur:
        if dateAvailable == True:
            cur.execute("""SELECT SUM(Ammount) FROM BankDBAurora.Transaction as tt 
                            WHERE  tt.CustomerId = """+customerId+  """ AND UPPER(tt.TransactionType) = UPPER('""" + transactionType + """') 
                            AND tt.DateTime BETWEEN '""" + startDate + """' AND '""" + endDate + """' GROUP BY tt.TransactionType;""");
        else:
            cur.execute("""SELECT idTransaction,MerchantName,Ammount,DATE_FORMAT(DateTime, "%M %e %Y %k %i %s"),CardNumber,TransactionType FROM BankDBAurora.Transaction as tt 
                            WHERE  tt.CustomerId = """ +customerId +""" AND UPPER(tt.TransactionType) = UPPER('""" + transactionType + """')""");
            
        conn.commit()
        cur.close()
        for row in cur:
            result.append(list(row))
        print (result)
    return result
#------------------------------------------------FetchTransactionsByType End-------------------------------#

def FetchCardBalance(event):
    result = []
    customerId=event["CustomerId"]
    #customerId='107';
    with conn.cursor() as cur: 
        cur.execute("""SELECT c.Balance,c.CardNumber FROM BankDBAurora.Card as c INNER JOIN BankDBAurora.Account as a ON c.AccountNumber = a.AccountNumber INNER JOIN BankDBAurora.Customer as ct ON ct.CustomerId = a.CustomerId WHERE  ct.CustomerId = '"""+customerId+"""' AND c.Type = 'Credit' ORDER BY a.AccountNumber LIMIT 1""")
        conn.commit()
        cur.close()
        for row in cur:
            result.append(list(row))
        print ("Data from FetchCardBalance...")
        print (result)
    return result
    
def FetchAccountId(event):
    result = []
    #customerId='107'
    customerId=event["CustomerId"]
    with conn.cursor() as cur: 
        cur.execute("""SELECT b.AccountNumber FROM BankDBAurora.Account as b WHERE CustomerId='"""+ customerId +"""'order by b.AccountNumber LIMIT 1""")
        conn.commit()
        cur.close()
        for row in cur:
            result.append(list(row))
        print ("Data from FetchAccountId...")
        print (result)
    return result
    
def PayCardBalance(event):
    #CardNumber='12341234'
    #Accountid='1001'
    #Amount='200'
    CardNumber=event["CardNumber"]
    Accountid=event["AccountId"]
    Amount=event["Amount"]
    result=PayCard(CardNumber,Amount)
    result2=PayAccountBalance(Accountid,Amount)
    if(result and result2):
        return True
        
def FetchAmountSpentOnVendor(event):
    result = []
    
    customerId=event["CustomerId"]
    VendorName=event["Vendor"]
    StartDate=event["startDate"]
    EndDate=event["endDate"]
    '''
    customerId='105'
    VendorName='uber'
    StartDate = '2018-05-23'
    EndDate = '2018-05-23'
    '''
    with conn.cursor() as cur:
        
        cur.execute("""SELECT SUM(T.Ammount) 'Total Ammount' FROM BankDBAurora.Transaction as T WHERE UPPER(T.MerchantName) = UPPER('""" + VendorName + """') AND T.DateTime BETWEEN '""" + StartDate + """' AND '""" + EndDate + """' AND T.CustomerId = '""" + customerId + """' GROUP BY T.MerchantName""")
        conn.commit()
        cur.close()
        for row in cur:
            result.append(list(row))
        print ("Data from FetchAmmountSpentOnVendor...")
        print (result)
    return result
    
def FetchAmountSpentOnVendorClass(event):
    result = []
    
    customerId=event["CustomerId"]
    VendorClass=event["VendorClass"]
    StartDate=event["startDate"]
    EndDate=event["endDate"]
    '''
    customerId='105'
    VendorClass='groceries'
    StartDate = '2018-06-01'
    EndDate = '2018-06-30'
    '''
    with conn.cursor() as cur: 
        cur.execute("""SELECT SUM(T.Ammount) 'Total Ammount' FROM BankDBAurora.Transaction as T 
	                    WHERE UPPER(T.MerchantName) IN 
		                                    (SELECT UPPER(v.Name) FROM BankDBAurora.Vendor as v 
			                                    INNER JOIN BankDBAurora.VendorClass as vc ON v.Class = vc.id 
				                                    WHERE UPPER(vc.ClassName) = UPPER('""" +VendorClass+ """ ') ) 
	                    AND T.DateTime BETWEEN '"""+StartDate+"""' AND '"""+EndDate+"""' AND T.CustomerId = '"""+customerId+"""' GROUP BY T.MerchantName""")
        conn.commit()
        cur.close()
        for row in cur:
            result.append(list(row))
        print ("Data from FetchAmountSpentOnVendor...")
        print (result)
    return result
##################################################################################################################    
    
def PayCard(CardNumber,Amount):
    with conn.cursor() as cur: 
        cur.execute("""UPDATE `BankDBAurora`.`Card` SET `Balance`=Balance - '"""+Amount+"""' WHERE `CardNumber`='"""+CardNumber+"""'""")
        conn.commit()
        cur.close()
        print ("Data from PayCard...")
        return True
    return False
    
def PayAccountBalance(Accountid,Amount):
    with conn.cursor() as cur: 
        cur.execute("""UPDATE `BankDBAurora`.`Account` SET `AccountBalance`=AccountBalance - '"""+Amount+"""' WHERE `AccountNumber`='"""+Accountid+"""'""")
        conn.commit()
        cur.close()
        print ("Data from PayAccountBalance...")
        return True
    return False
