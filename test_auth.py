import mysql.connector
import os

def list_file(pth,files):
    thisdir=pth
    path_list=[]
    for r, d, f in os.walk(thisdir):
        for file in f:
            if file.endswith(files):
                path_list.append(os.path.join(r, file))
    return path_list

def create_pthTurple(path_list):
    path_turple=[]
    for i in range(len(path_list)):
        t=(str(i),path_list[i])
        path_turple.append(t)
    return path_turple

path_list=list_file("C:/Users/wichu/Documents/GitHub",".csv")
val=create_pthTurple(path_list)

#print(val)

mydb = mysql.connector.connect(
  host="163.44.196.192",
  user="ararize",
  password="isylzjko$3rv3r",
  database="tp"
)

#print(mydb)

mycursor = mydb.cursor()

mycursor.execute("TRUNCATE TABLE vm_path")

sql="INSERT INTO vm_path (pthID, pthName) VALUES (%s, %s)"

mycursor.executemany(sql,val)
mydb.commit()
print(mycursor.rowcount)