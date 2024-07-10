import csv
import uuid
import mysql.connector

# 连接到MySQL数据库
db = mysql.connector.connect(
    host="<RDS_HOST>",
    user="<USERNAME>",
    password="<PASSWORD>",
    database="email"
)
cursor = db.cursor()

with open('emaillist.csv', 'r', encoding='utf-8', errors='ignore') as csvfile:
    csvreader = csv.DictReader(csvfile)
    origin_fieldnames = next(csvreader)  # 获取原始列标题
    
    new_fieldnames=[]
    for field in origin_fieldnames:
        try:
            new_field = field.strip()
            new_fieldnames.append(new_field)
        except UnicodeDecodeError:
            new_fieldnames.append('invalid_field')
    # 修改特定列标题
    new_fieldnames[0] = 'emailadd'
    new_fieldnames[1] = 'lan'
    new_fieldnames[2] = 'level'
    new_fieldnames[3] = 'templatename'
   
        
    
    # 重置文件指针位置
    csvfile.seek(0)

    # 创建新的DictReader对象
    csvreader = csv.DictReader(csvfile, fieldnames=new_fieldnames,strict=False, restkey=None)
    next(csvreader) #跳过标题
    a = 0
    # 遍历每一行
    for row in csvreader:
        try:
            print(a)
            a+=1

            row_id = str(uuid.uuid4())
            emailadd = row['emailadd']
            lan = row['lan']
            level = row['level']
            templatename = row['templatename']
            
            insert_stmt = "INSERT INTO email_table (id, emailadd, lan, level, templatename) VALUES (%s, %s, %s, %s, %s)"
            data = (row_id, emailadd, lan, level, templatename)
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
