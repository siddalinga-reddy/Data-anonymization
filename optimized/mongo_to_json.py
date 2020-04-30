from pymongo import MongoClient
import pandas as pd
connect = MongoClient('mongodb://localhost:27017/')                     
mydb = connect.database_name                                      #database_name
collection = mydb.collection_name                                           #collection_name
df = pd.DataFrame(collection.find())
df.to_json('file_name.json')
