import urllib.request
import json
import pandas as pd

# Factory name
df_factoryID=pd.read_csv("https://raw.githubusercontent.com/wichukornk/FEC/main/factoryID.txt",header=None)
df_factoryID[0]=df_factoryID[0].str.replace("<option value=\"","")
df_factoryID[0]=df_factoryID[0].str.replace("</option>","")
df_factoryID[0]=df_factoryID[0].str.replace("\">",",")
df_factoryID=df_factoryID[0].str.split(pat=",",expand=True)

# Electric equipment
def getElecData(facID,sysType):
  url_elec="http://54.254.220.213/besten/backend/web/index.php?r=factories%2Freport&rid="+str(sysType)+"&fid="+facID+"&tid=2&user=admin@diw.com&pass=123456"
  response = urllib.request.urlopen(url_elec)
  data = json.loads(response.read())
  df1=pd.DataFrame(data)
  col_list=['feID', 'feName', 'elec_price', 'feType', 'feType2', 'feSize', 'feUnit','feHours', 'nameplate', 'cal_nameplate', 'nameplate_unit', 'np_d','pot_mj', 'savebaht']
  df=pd.DataFrame(columns=col_list)
  for i in range(len(df1)):
    df2=df1.iloc[i,0]
    df2=pd.DataFrame(df2,index=[i])
    df=df.append(df2)
  df['facID']=facID
  df['equipType']=sysType
  col_list=['facID','equipType']+col_list
  return df.loc[:,col_list]

df_elec_main=pd.DataFrame()
df_elec_sup=pd.DataFrame()

for j in df_factoryID.index: 
  df_elec_main_tod=getElecData(facID=df_factoryID.loc[j,0],sysType=3)
  df_elec_main=df_elec_main.append(df_elec_main_tod)
  df_elec_sup_tod=getElecData(facID=df_factoryID.loc[j,0],sysType=4)
  df_elec_sup=df_elec_sup.append(df_elec_sup_tod)

df_elec=df_elec_main.append(df_elec_sup)
df_elec['facName']=df_elec['facID'].map(dict(zip(df_factoryID[0],df_factoryID[1])))
df_elec['sysCate']=df_elec['equipType'].astype('str').map({"3":"Main electric equipment","4":"Support electric equipment"})


# Heat equipment
def getThermData(facID,sysType):
  url_therm="http://54.254.220.213/besten/backend/web/index.php?r=factories%2Freport&rid="+str(sysType)+"&fid="+facID+"&tid=2&user=admin@diw.com&pass=123456"
  response = urllib.request.urlopen(url_therm)
  data = json.loads(response.read())
  df1=pd.DataFrame(data)
  col_list=['feID', 'feName', 'feType', 'feType2', 'feSize', 'feUnit', 'feHours','fuelType', 'fuelTypeUnit', 'feFuelY', 'feFuelPrice', 'nameplate','cal_nameplate', 'nameplate_unit', 'np_d', 'pot_mj', 'savefuel','savebaht']
  df=pd.DataFrame(columns=col_list)
  for i in range(len(df1)):
    df2=df1.iloc[i,0]
    df2=pd.DataFrame(df2,index=[i])
    df=df.append(df2)
  df['facID']=facID
  df['equipType']=sysType
  col_list=['facID','equipType']+col_list
  return df.loc[:,col_list]

df_therm_main=pd.DataFrame()
df_therm_sup=pd.DataFrame()

for j in df_factoryID.index :
  df_therm_main_tod=getThermData(facID=df_factoryID.loc[j,0],sysType=5)
  df_therm_main=df_therm_main.append(df_therm_main_tod)
  df_therm_sup_tod=getThermData(facID=df_factoryID.loc[j,0],sysType=6)
  df_therm_sup=df_therm_sup.append(df_therm_sup_tod)

df_therm=df_therm_main.append(df_therm_sup)
df_therm['facName']=df_therm['facID'].map(dict(zip(df_factoryID[0],df_factoryID[1])))
df_therm['sysCate']=df_therm['equipType'].astype('str').map({"5":"Main thermal equipment","6":"Support thermal equipment"})

# Upload to BigQuery


df_elec.to_gbq(destination_table='FEC.electric_equipment',project_id='introcloud01',if_exists='replace')
df_therm.to_gbq(destination_table='FEC.thermal_equipment',project_id='introcloud01',if_exists='replace')
