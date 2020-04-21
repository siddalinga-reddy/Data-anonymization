from pymongo import MongoClient
import random, string,time,xlrd
import pandas as pd
import uuid,sys,pymysql
from sqlalchemy import create_engine

serial_count=0 
start_time = time.time()

#Details of columns name to be anonymized
wb = xlrd.open_workbook(r'C:\Users\reddysi\Pictures\anon.xlsx') 
sheet = wb.sheet_by_index(0)

#load copy of data from source 
try:
    connect = MongoClient('mongodb://localhost:27017/')                     
    mydb = connect.sheet.cell_value(1,0)                                                #database_name
    collection = mydb.sheet.cell_value(1,1)                                           #collection_name
    df = pd.DataFrame(collection.find())
    df.drop(df.columns[0], axis=1,inplace = True)
except Exception as e:
    try:
        connection=pymysql.connect('localhost','root','Siddalinga@029',sheet.cell_value(1,0))       #database_name
        query ='SELECT * FROM '+  sheet.cell_value(1,1)                                             #table_name
        df= pd.read_sql_query(query, connection)
    except Exception as e:
        sys.exit('failed to access data')
print("before anonymization")
print(df)

#claimToken File is default
df1=pd.DataFrame()
df1['original_claimToken']=df['claimToken']
i=0
for j in df['claimToken']:
    h=str(j[:14])+str(uuid.uuid4())
    df['claimToken']=df['claimToken'].replace([j],h)

#reference  for claimToken
df1['anon_claimToken']=df['claimToken']
db_data = 'mysql+pymysql://' + 'root' + ':' + 'Siddalinga@029' + '@' + 'localhost' + ':3306/' \
       + 'my_db' 
engine = create_engine(db_data)
df1.to_sql('reference', engine, if_exists='append', index=False)
#functions for each column
def serial(a):
    global serial_count
    if serial_count==0:
        for i in df[a]:
            x=''.join(random.choices(string.ascii_uppercase,k=3))
            y=''.join(random.choices(string.digits,k=3))
            z=''.join(random.choices(string.ascii_uppercase + string.digits,k=4))
            h=x+y+z
            df[a]=df[a].replace([i],h)
        serial_count+=1
        
def name(k):
    serial('serial')
    for p,j in zip(df['serial'],df[k]):
        h=j[:4]+p+j[14:]
        df[k]=df[k].replace([j],[h])

def systemid(k):
    serial('serial')
    for p,j in zip(df['serial'],df[k]):
        h=j[:11]+p
        df[k]=df[k].replace([j],[h])

def enclosureId(k):
    serial('serial')
    for p,j in zip(df['serial'],df[k]):
        h=j[:11]+p
        df[k]=df[k].replace([j],[h])
        
#default function for all columns
def default(w):
    i=0
    while i<len(df[w]):
        j=0
        while j < len(df[w][i]):
            if df[w][i][j].islower() :
                a = ''.join(random.choices(string.ascii_lowercase, k=1))
                df[w][i] = df[w][i].replace(df[w][i][j],a)
            elif df[w][i][j].isupper():
                b = ''.join(random.choices(string.ascii_uppercase, k=1))
                df[w][i] = df[w][i].replace(df[w][i][j],b)
            elif df[w][i][j].isnumeric():
                c = ''.join(random.choices( string.digits, k=1))
                df[w][i] = df[w][i].replace(df[w][i][j],c)
            j+=1
        i+=1
 
for  j in range(sheet.nrows):
    v=sheet.cell_value(j,2)
    if j==0:
        continue
    if v.lower()=='serial':
       serial(v)
    elif v.lower()=='name':
        name(v)
    elif v.lower()=='systemid':
        systemid(v)
    elif v.lower()=='enclosureid':
        enclosureId(v)
    elif len(v)>=1:
        default(v)
  
print("after anonymization")
print(df)
print("--- %s seconds ---" % ((time.time() - start_time)))
