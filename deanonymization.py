import pymysql
connection = pymysql.connect(host='localhost',
                         user='root',
                         password='Siddalinga@029',
                         db='my_db')    
cursor=connection.cursor()   
cursor.execute("SELECT * from reference ")
rows=cursor.fetchall()
for row in rows:
       print(row)