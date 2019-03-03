from __future__ import print_function
import boto3
import os
import time



def FetchInfo(event):
    CustomerId=event["CustomerId"]
    DBClient= boto3.client('dynamodb')
    DBResponse =DBClient.get_item(
                TableName= 'UserHistory' ,
                Key={
                    'CustomerId': {						
                               'S': CustomerId,
            				      }
                    },
                	AttributesToGet=['CustomerId','Event','UserStatus',],
                )    
    print(DBResponse)
    
    g_info={"CustomerId":"",                                        
                "Event":"",
                "Status":"False"
                }
    if 'Item' in DBResponse:        
        print("Status : success")
        CustomerId=DBResponse['Item']['CustomerId']['S']                        
        Event=DBResponse['Item']['Event']['S']
        Status=DBResponse['Item']['UserStatus']['S']                                                      
        l_Status='success'                                                      
        g_isRegisteredNumber='Yes'
        print("CustomerId : " + CustomerId)                                     
        print("Event : " + Event)
        print("Status : "+ Status)
        g_info={"CustomerId":CustomerId,                                        
                "Event":Event,
                "Status":Status
                }
        print(g_info)  
    else:
        print('Status : failed')
        l_Status='failed'
    return(g_info)
    
def UpdateInfo(event):
    print(event)
    CustomerId=event["CustomerId"]
    Event = str(event["lastEvent"])
    
    UserStatus = str(event["Status"])
    DBclient = boto3.client('dynamodb')
    DBResponse =DBclient.update_item(
                ExpressionAttributeValues={
                    ':Event': {
                            'S': Event,
                          },
                    ':Status': {
                            'S': UserStatus,
                          }      
                                            },
                Key={
                    'CustomerId': {
                        'S': CustomerId,
                    }
                },
                ReturnValues='UPDATED_NEW',
                TableName = 'UserHistory',
                UpdateExpression='SET Event = :Event,UserStatus =:Status',
            )
    print(DBResponse)
    return(True)