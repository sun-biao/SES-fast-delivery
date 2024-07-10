import argparse
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
import pytz
import hashlib
import json, time
import mysql.connector



greeting = {"EN":"Game mansion Turns 7!",

}
templates = ['FR-high-7th','ES-high-7th','KR-high-7th','JP-high-7th','DE-high-7th','EN-high-7th','CNS-high-7th','FR-other-7th','ES-other-7th','KR-other-7th','JP-other-7th','DE-other-7th','EN-other-7th','CNS-other-7th']

senderemail = "<MM@<SENDER_EMAIL>.com>"
salt = 'saltstring'
unsub_url = 'https://<YOUR UNSUB URL>/prod/unsubscribe'
client = boto3.client('sesv2', region_name='us-west-2')
cloudwatchclient = boto3.client('cloudwatch', region_name='us-west-2')

beijing_tz = pytz.timezone('Asia/Shanghai')

db = None
cursor = None
current_timestamp = None
def initdb():
    # 连接到MySQL数据库
    global db,cursor
    db = mysql.connector.connect(
        host="<RDS URL>",
        user="<USERNAME>",
        password="<PASSWORD>",
        database="email"
    )
    cursor = db.cursor()


def main():
    # 创建一个ArgumentParser对象
    parser = argparse.ArgumentParser()

    #parser.add_argument('test', type=str, help='您的姓名')


    # 添加可选参数
    parser.add_argument('-t','--testemail', type=str)
    parser.add_argument('-m','--testtemplate', type=str)
    parser.add_argument('-v', '--volume', type=int, default=1, help='1到100k的数字')
    parser.add_argument('-c', '--compaignid', type=str, help='1到100k的数字')

    # 解析命令行参数
    args = parser.parse_args()
    initdb()
    global current_timestamp
    current_timestamp = int(time.time())
    volume = 1 if args.volume is  None else int(args.volume)
    # 打印解析后的参数值
    #print(f'testemail: {args.testemail}')
    compaignid = genCompainId() if args.compaignid == None else  args.compaignid
    if (args.testemail == None):
        while True:
            user_input = input('production env..... Please enter Fire to start: ')
            if user_input == 'Fire':
                compaignid = genCompainId() if args.compaignid == None else  args.compaignid
                print(f"compaign id is {compaignid}")
                break
        
        fetchMembers(compaignid,volume)
        print(f"compaign id is {compaignid}")
    else: #测试环境
        if(args.testtemplate == None):
            print('Test template is required in test mode! Please use -m to settle template!')
            return
        else:
            print(f'volume: {args.volume}')
            print(f'compaignid: {args.compaignid}')
            createStatus(compaignid,args.testemail)
            sendemail(compaignid,args.testemail,args.testtemplate) #发送测试邮件
    
    
    
    
    
    cursor.close()
    db.close()
    
def fetchMembers(compaignid, volume):
    try:
        #select_stmt = "select emailadd, templatename, level from email_table et where NOT Exists (select 1 from email_stats es where es.emailadd = et.emailadd) and emailadd not like '%yahoo.com' order by level limit %s"
        select_stmt = "select emailadd, templatename, level from email_table et where NOT Exists (select 1 from email_stats es where es.emailadd = et.emailadd)  order by level limit %s"
        
        data = (volume,)
        cursor.execute(select_stmt, data)
        
        results = cursor.fetchall()
        a = 0
        starttime = datetime.now(timezone.utc)
        send_frequency = 0.5 #sleep time
        for row in results:
            #print(f"a is {a}" )
            time.sleep(send_frequency)
            currenttime = datetime.now(timezone.utc)
            timediff = currenttime - starttime
            if (timediff.total_seconds()/float(60) >= 1): #检查频率
                result = checkPongRate(currenttime)
                starttime = currenttime
                if (result is not None):
                    print(f"The PONG result is #################### {result*100:.2f}% #############################")
                    print(f"The PONG result is #################### {result*100:.2f}% #############################")
                    print(f"The PONG result is #################### {result*100:.2f}% #############################")
                    if(result > 0.96):
                        send_frequency*=0.8
                        print(f"@@@@@@@@@@@@@@@@@@@@@@@@@@@ latency GO UP TO {send_frequency} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
                    if(result < 0.90):
                        if(send_frequency < 0.5):
                            send_frequency = 0.5
                        else:
                            send_frequency*=1.5 #increase 20% latency
                        print(f"|||||||||||||||||||||||| latency DROP DOWN TO {send_frequency} |||||||||||||||||||||||||||||")
                        #time.sleep(5)
                    if(result < 0.80):
                        break
                
            email = row[0]
            tm = row[1]
            level = row[2]
            if tm not in templates:
                print(f"{email} is wrong with template {tm}! ")
                continue
            if(createStatus(compaignid,email) == 1):
                suc = sendemail(compaignid,email,tm)
                if(suc == 1):
                    a+=1
                    print(f"{email.ljust(50,' ')} level {level} has been sent! FQ is {send_frequency:.3f} Total {a}")
            
            #print(email)
        # 获取查询结果
        #result = cursor.fetchone()

        # # 打印结果
        # print(result)
        
    except mysql.connector.IntegrityError as e:
        # 处理唯一键冲突或其他完整性错误
        print(f"error loading emails to send!")
        print(e)
        db.rollback()

    except Exception as e:
        # 处理其他异常
        #print(f"Error inserting row: {row}")
        print(e)
        db.rollback()

    
def createStatus(compaignid,destination):
        try:
            
            insert_stmt = "INSERT INTO email_stats (batch_name, emailadd, delivered, suppressed, batchtime) VALUES (%s, %s, %s, %s, %s)"
            data = (compaignid, destination, 0, 0, current_timestamp)
            cursor.execute(insert_stmt, data)
            db.commit()
            return 1
            
            # 获取查询结果
            #result = cursor.fetchone()

            # # 打印结果
            # print(result)
            
        except mysql.connector.IntegrityError as e:
            # 处理唯一键冲突或其他完整性错误
            print(f"Error inserting destination:{compaignid} {destination}")
            print(e)
            db.rollback()
            return 0

        except Exception as e:
            # 处理其他异常
            #print(f"Error inserting row: {row}")
            print(e)
            db.rollback()
            return 0

   
def genCompainId():
    beijing_now = datetime.now(beijing_tz)
    formatted_time_str = beijing_now.strftime('%Y-%m-%d-%H-%M-%S')
    #print(formatted_time_str)
    return formatted_time_str

def getGreeting(template):
    return greeting[template.split("-")[0]]


def checkPongRate(metrictime):
    print("metric timing is: ", metrictime)
    try:
        starttime = metrictime - timedelta(minutes=2)
        endtime = starttime + timedelta(minutes=1)
        result = checkCWMetric(starttime, endtime)
        if (result is not None):
            return result
        else:
            starttime = metrictime - timedelta(minutes=3)
            endtime = starttime + timedelta(minutes=1)
            return checkCWMetric(starttime, endtime)
    except Exception as e:
        print(e)
        return None

def checkCWMetric(starttime, endtime):
    try:
        send_response = cloudwatchclient.get_metric_statistics(
            Namespace='AWS/SES',
            MetricName='Send',
            Dimensions=[],
            StartTime=starttime,
            EndTime=endtime,
            Period=60,  
            Statistics=['Sum']
        )
        no_send = 0
        no_delivery = 0
        no_bounce = 0
        print(send_response)
        if (len(send_response['Datapoints']) > 0):
            no_send = int(send_response['Datapoints'][0]['Sum'])
        if no_send == 0 :
            return None
        delivery_response = cloudwatchclient.get_metric_statistics(
            Namespace='AWS/SES',
            MetricName='Delivery',
            Dimensions=[],
            StartTime=starttime,
            EndTime=endtime,
            Period=60,  
            Statistics=['Sum']
        )
        bounce_response = cloudwatchclient.get_metric_statistics(
            Namespace='AWS/SES',
            MetricName='Bounce',
            Dimensions=[],
            StartTime=starttime,
            EndTime=endtime,
            Period=60, 
            Statistics=['Sum']
        )
        if (len(delivery_response['Datapoints']) > 0):
            no_delivery = int(delivery_response['Datapoints'][0]['Sum'])
        if (len(bounce_response['Datapoints']) > 0):
            no_bounce = int(bounce_response['Datapoints'][0]['Sum'])
        
        print(f"Time: {starttime}, send: {no_send}, Deliver: {no_delivery}, bounce: {no_bounce}")
        return (no_delivery + no_bounce)/float(no_send)
    except Exception as e:
        print(send_response)
        print('------------')
        print(delivery_response)
        print('------------')
        print(bounce_response)
        print(e)
        return None
    


def sendemail(compaignid,destination,template):
    
    try:
        # 发送模板邮件
        sig = hashlib.sha256((destination+salt).encode()).hexdigest()
        email_link = f"{unsub_url}?email_add={destination}&sig={sig}"
        response = client.send_email(
            FromEmailAddress = getGreeting(template)+senderemail,
            Destination = {'ToAddresses': [destination]},
            Content={
                'Template': {
                    'TemplateName': template,
                    'TemplateData': json.dumps({"MTUnsubscribeLink":email_link}),
                    "Headers": [ 
                        { 
                           "Name": "List-Unsubscribe",
                           "Value": "<" + email_link + ">," + "<mailto: unsubscribe@matchingtonmansion.com>"
                        },
                        {
                          "Name": "List-Unsubscribe-Post",
                          "Value": "List-Unsubscribe=One-Click"            
                        }
                     ]
                }
            },
            ConfigurationSetName="hop",
            EmailTags = [{'Name':'compaignid','Value':compaignid}]
        )
        return 1
        # 打印响应
        #print(response)

    except ClientError as e:
        print(e.response['Error']['Message'])
        return 0
    
    
    
if __name__ == '__main__':
    main()
