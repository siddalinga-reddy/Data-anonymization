import pandas as pd
import binascii,pandas,random,string,time
import hashlib 
start_time = time.time()
df=pd.read_json(r'file_name.json')
print('Before anonymization')
print(df)
for i in range(len(df)):
    message=str(df['claimToken'][i][14:])
    h = hashlib.shake_256(binascii.unhexlify('5821844346F50E78E73F17B1F312F0C86643FD4C60475538D714FE7AA9E04669'))
    h.update(str(message).encode('utf-8'))
    f=df['claimToken'][i][:14]+h.hexdigest(len(message)//2).lower()[:8]+'-'+h.hexdigest(len(message)//2).lower()[8:12]+'-'+h.hexdigest(len(message)//2).lower()[12:16]+'-'+h.hexdigest(len(message)//2).lower()[16:20]+'-'+h.hexdigest(len(message)//2).lower()[20:]
    df['claimToken']=df['claimToken'].replace([df['claimToken'][i]],[f])
    
    message=str(df['serial'][i])
    h = hashlib.shake_256(binascii.unhexlify('5821844346F50E78E73F17B1F312F0C86643FD4C60475538D714FE7AA9E04669'))
    h.update(str(message).encode('utf-8'))
    f=h.hexdigest(len(message)//2).upper()
    
    df['serial']=df['serial'].replace([df['serial'][i]],[f])
    c=df['systemId'][i][:11]+f
    df['systemId']=df['systemId'].replace([df['systemId'][i]],[c])
    c=df['enclosureId'][i][:11]+f
    df['enclosureId']=df['enclosureId'].replace([df['enclosureId'][i]],[c])
    c=df['name'][i][:4]+f+df['name'][i][14:]
    df['name']=df['name'].replace([df['name'][i]],[c])
print('After anonymization')
print(df)
df.to_json('anonymized_data.json')
print("--- %s seconds ---" % ((time.time() - start_time)))
