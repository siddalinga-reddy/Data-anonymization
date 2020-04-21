from pymongo import MongoClient
import random, string,xlrd,time
import pandas as pd
import uuid,sys,pymysql
serial_count=0 
start_time = time.time()

#Details of columns name to be anonymized
wb = xlrd.open_workbook('anon.xlsx') 
sheet = wb.sheet_by_index(0)

#load copy of data from source 
try:
    connect = MongoClient('mongodb://localhost:27017/')                     
    mydb = connect.sheet.cell_value(1,0)                                                #database_name
    collection = mydb.sheet.cell_value(1,1)                                           #collection_name
    mongo_df = pd.DataFrame(collection.find())
    mongo_df.drop(mongo_df.columns[0], axis=1,inplace = True)
except Exception as e:
    try :
        mongo_df=pd.read_excel(sheet.cell_value(1,0)+'.'+sheet.cell_value(1,1))                 #file name with type
    except Exception as e:
        try:
            connection=pymysql.connect('localhost','root','Siddalinga@029',sheet.cell_value(1,0))       #database_name
            query ='SELECT * FROM '+  sheet.cell_value(1,1)                                             #table_name
            mongo_df= pd.read_sql_query(query, connection)
        except Exception as e:
            sys.exit('failed to access data')
print("before anonymization")
print(mongo_df)
#claimToken File is default
df=pd.DataFrame()
df['original_claimToken']=mongo_df['claimToken']
i=0
for j in mongo_df['claimToken']:
    h=str(j[:14])+str(uuid.uuid4())
    mongo_df['claimToken']=mongo_df['claimToken'].replace([j],h)
df['anon_claimToken']=mongo_df['claimToken']
df.to_excel('reference1.xlsx')              #reference file of claimToken

#functions for each column
def serial(a):
    global serial_count
    if serial_count==0:
        for i in mongo_df[a]:
            x=''.join(random.choices(string.ascii_uppercase,k=3))
            y=''.join(random.choices(string.digits,k=3))
            z=''.join(random.choices(string.ascii_uppercase + string.digits,k=4))
            h=x+y+z
            mongo_df[a]=mongo_df[a].replace([i],h)
        serial_count+=1
        
def name(k):
    serial('serial')
    for p,j in zip(mongo_df['serial'],mongo_df[k]):
        h=j[:4]+p+j[14:]
        mongo_df[k]=mongo_df[k].replace([j],[h])

def systemid(k):
    serial('serial')
    for p,j in zip(mongo_df['serial'],mongo_df[k]):
        h=j[:11]+p
        mongo_df[k]=mongo_df[k].replace([j],[h])

def enclosureId(k):
    serial('serial')
    for p,j in zip(mongo_df['serial'],mongo_df[k]):
        h=j[:11]+p
        mongo_df[k]=mongo_df[k].replace([j],[h])
        
#default function for all columns
def default(w):
    i=0
    while i<len(mongo_df[w]):
        j=0
        while j < len(mongo_df[w][i]):
            if mongo_df[w][i][j].islower() :
                a = ''.join(random.choices(string.ascii_lowercase, k=1))
                mongo_df[w][i] = mongo_df[w][i].replace(mongo_df[w][i][j],a)
            elif mongo_df[w][i][j].isupper():
                b = ''.join(random.choices(string.ascii_uppercase, k=1))
                mongo_df[w][i] = mongo_df[w][i].replace(mongo_df[w][i][j],b)
            elif mongo_df[w][i][j].isnumeric():
                c = ''.join(random.choices( string.digits, k=1))
                mongo_df[w][i] = mongo_df[w][i].replace(mongo_df[w][i][j],c)
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
print(mongo_df)
print("--- %s seconds ---" % ((time.time() - start_time)))
