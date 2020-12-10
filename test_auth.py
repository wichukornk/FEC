import pandas as pd
import os
from google.oauth2 import service_account
thisdir="C:/Users/wichu/Documents"
path_list=[]
for r, d, f in os.walk(thisdir):
    for file in f:
        if file.endswith(".csv"):
            #print(os.path.join(r, file))
            path_list.append(os.path.join(r, file))
df=pd.DataFrame(path_list,columns=['Path'])
#print(df)

credentials = service_account.Credentials.from_service_account_file(
   "C:/Users/wichu/Documents/GitHub/FEC/IntroCloud01-7b6a00779302.json",
)

#pandas_gbq.context.credentials = credentials


df.to_gbq(destination_table='FEC.pathList_sim',project_id='introcloud01',if_exists='replace',credentials=credentials)