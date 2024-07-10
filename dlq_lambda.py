import json
import mysql.connector
import boto3




db = mysql.connector.connect(
    host="<HOST_RDS>",
    user="<USERNAME>",
    password="<PWD>",
    database="email"
)
cursor = db.cursor()
    # 创建 SQS 客户端
sqs = boto3.client('sqs')
    
def lambda_handler(event, context):
    
    


    # 指定 SQS 队列的 URL
    queue_url = 'https://sqs.us-west-2.amazonaws.com/891377356308/emaildlq'


    # 从 SQS 队列中拉取消息
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # 一次最多拉取 10 条消息
        WaitTimeSeconds=20       # 最长等待时间为 20 秒
    )
    print('fetched data')
    # 处理拉取到的消息
    if 'Messages' in response:
        for message in response['Messages']:
            # 处理消息内容
            bodyjson = message['Body']
            print(bodyjson)
            if ('Records' in bodyjson and 'aws:sns' in bodyjson):
                bodydict = json.loads(bodyjson, strict = False)
                print(bodydict.get('Records')[0]['Sns']['Message'])
                
                flag = processdate(bodydict.get('Records')[0]['Sns']['Message'])  
                
                print(f"flag: {flag}")
                
                if(flag == 'ok'):
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                else:
                    print(f" error processed for recrod {bodyjson}")
                
               
            else:
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            
            
            print('------------')


            # 删除已处理的消息
            


    
    return {
        'statusCode': 200,
        'body': 'SQS messages processed'
    }
    
    
def processdate(message):
    messagejson = json.loads(message, strict = False)
    if(messagejson['mail']['tags'].get('compaignid') is None):
        return
    #message = event['Records'][0]['Sns']['Message']
    emailadd = messagejson['mail']['destination'][0]
    #print('email address.....', message['mail']['destination'][0])
    compaignid = ''
    try:
        if(messagejson['mail']['tags']['compaignid'] != None):
            compaignid = messagejson['mail']['tags']['compaignid'][0]
            #print('tags.......', message['mail']['tags']['compaignid'][0])
        if(messagejson['eventType'] == 'Delivery'):
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
        suppressedreason = ''
        if(messagejson['eventType'] == 'Bounce' and messagejson['bounce']['bounceType'] == 'Permanent'):
            suppressedreason = 'BOUNCE'
            #print('Hard bounce......')
            
        if(messagejson['eventType'] == 'Complaint'):
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
        if(messagejson['eventType'] == 'Bounce' and messagejson['bounce']['bounceType'] == 'Transient'):
            update_stmt = "UPDATE email_stats SET suppressed = 11 WHERE emailadd = %s and batch_name = %s"
            data2 = (emailadd, compaignid)
            cursor.execute(update_stmt, data2)
            db.commit()
        
        return 'ok'
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
