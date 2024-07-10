import json
import mysql.connector
import boto3


db = mysql.connector.connect(
    host="<HOST>",
    user="<USER_NAME>",
    password="<PWD>",
    database="email"
)
cursor = db.cursor()
client = boto3.client('sesv2')
def lambda_handler(event, context):
    # TODO implement
    #print(event)


    if(event['Records'][0]['Sns']['Subject'] == None or 'SES' not in event['Records'][0]['Sns']['Subject']):
        return


    #print(repr(event['Records'][0]['Sns']['Message']))
    message = json.loads(event['Records'][0]['Sns']['Message'], strict=False)


    if(message['mail']['tags'].get('compaignid') is None):
        return
    #message = event['Records'][0]['Sns']['Message']
    emailadd = message['mail']['destination'][0]
    #print('email address.....', message['mail']['destination'][0])
    compaignid = ''
    try:
        if(message['mail']['tags']['compaignid'] != None):
            compaignid = message['mail']['tags']['compaignid'][0]
            #print('tags.......', message['mail']['tags']['compaignid'][0])
        if(message['eventType'] == 'Delivery'):
            #cursor.execute("START TRANSACTION")
            #print('Delivery......')
            #被其它compaign更新的add，更新成2；当前compaign更新的，为1
            #update_stmt = "UPDATE email_stats SET delivered = 2 WHERE emailadd = %s"
            #data = (emailadd,)
            #cursor.execute(update_stmt, data)
            if(len(compaignid) > 0):
                update_stmt = "UPDATE email_stats SET delivered = 1 WHERE emailadd = %s and batch_name = %s"
                data2 = (emailadd, compaignid)
                cursor.execute(update_stmt, data2)
                db.commit()
                return 'ok'
        suppressedreason = ''
        if(message['eventType'] == 'Bounce' and message['bounce']['bounceType'] == 'Permanent'):
            suppressedreason = 'BOUNCE'
            #print('Hard bounce......')
            
        if(message['eventType'] == 'Complaint'):
            suppressedreason = 'COMPLAINT'
        if(len(suppressedreason) > 0):
            #print(event)
            # response2 = client.put_suppressed_destination(
            #     EmailAddress=emailadd,
            #     Reason = suppressedreason
            # )
            #cursor.execute("START TRANSACTION")
            #被其它compaign更新的add，更新成2；当前compaign更新的，为1
            #update_stmt = "UPDATE email_stats SET suppressed = 2 WHERE emailadd = %s"
            #data = (emailadd,)
            #cursor.execute(update_stmt, data)
            if(len(compaignid) > 0):
                update_stmt = "UPDATE email_stats SET suppressed = 1 WHERE emailadd = %s and batch_name = %s"
                data2 = (emailadd, compaignid)
                cursor.execute(update_stmt, data2)
                db.commit()
                return 'ok'
        
        if(message['eventType'] == 'Bounce' and message['bounce']['bounceType'] == 'Transient'):
            update_stmt = "UPDATE email_stats SET suppressed = 11 WHERE emailadd = %s and batch_name = %s"
            data2 = (emailadd, compaignid)
            cursor.execute(update_stmt, data2)
            print(cursor.statement)
            db.commit()
        


    except mysql.connector.IntegrityError as e:
        # 处理唯一键冲突或其他完整性错误
        print(f"Error inserting row: {row}")
        print(e)
        db.rollback()
        return 'error'


    except Exception as e:
        # 处理其他异常
        #print(f"Error inserting row: {row}")
        print(e)
        db.rollback()
        return 'error'
