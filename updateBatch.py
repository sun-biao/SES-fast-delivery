import csv
import uuid
import mysql.connector
import time
import sys

if (len(sys.argv) != 3):
    print("Error: Insufficient arguments.")
    exit()

filename = sys.argv[1]
delivered = sys.argv[2] == "1"
print("delivered: ",delivered)


# 连接到MySQL数据库
db = mysql.connector.connect(
    host="<HOST>",
    user="<USER_NAME>",
    password="<PWD>",
    database="email"
)
cursor = db.cursor()

with open(filename, 'r', encoding='utf-8', errors='ignore') as csvfile:

    # 创建新的DictReader对象
    csvreader = csv.reader(csvfile,delimiter=',')
    next(csvreader) #跳过标题
    a = 0
    current_timestamp = int(time.time())
    # 遍历每一行
    for row in csvreader:
        try:
            print(a)
            a+=1
            
            
            insert_stmt = "INSERT INTO email_stats (batch_name, emailadd, delivered, suppressed, batchtime) VALUES (%s, %s, %s, %s, %s)"
            if(delivered):
                data = ('pre-delivered', row[0], 1, 0, current_timestamp)
            else:
                data = ('pre-suppressed', row[0], 0, 1, current_timestamp)
            cursor.execute(insert_stmt, data)
            db.commit()
            
            
            # 获取查询结果
            #result = cursor.fetchone()

            # # 打印结果
            # print(result)
            
        except mysql.connector.IntegrityError as e:
            # 处理唯一键冲突或其他完整性错误
            print(f"Error inserting row: {row}")
            print(e)
            db.rollback()

        except Exception as e:
            # 处理其他异常
            #print(f"Error inserting row: {row}")
            print(e)
            db.rollback()
            
# 关闭游标和数据库连接
cursor.close()
db.close()
