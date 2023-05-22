# -*- coding: utf-8 -*-
"""
Code to pass from SQL server to sqlite

Created on Fri Mar 24 09:46:59 2023

@author: Rogger.Correa
"""

import sqlite3
import pyodbc
import pandas as pd 
from datetime import datetime
from datetime import timezone

######################## SQL server connection ########################

server="yourserver"
sqldb="yourdatabase"
user="youruser"
password="yourpasswordhere"
sql_connection_str="""Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;UID=%s;PWD=%s;"""
sql_connection = pyodbc.connect(sql_connection_str%(server,sqldb,user,password))


######################### sqlite  conection  #########################

SQlitedb=r'P:\MapComponents\Projects\MobileDB\sql_export\FCNSW.sqlite'
SQlike=''
# create a sqlite db
sqliteConnection = sqlite3.connect(SQlitedb)
sqliteCursor = sqliteConnection.cursor()
# ref: http://hakanu.net/sql/2015/08/25/sqlite-unicode-string-problem/
#sqliteConnection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')

####################### read SQL tables ###############################
print("********************************************************************")
print(" Writing: "+sqldb+"@"+server)
print("onto SQliteDB: "+SQlitedb)

print("********************************************************************")

#this reads a list of tables
qry="SELECT table_name FROM information_schema.tables;"
tables = list(pd.read_sql(qry, sql_connection)['table_name'])
tables.remove("sqlite_sequence")

# this reads one table
count=0
for table in tables:
    count+=1
    qry="SELECT * FROM %s"%table
    tableData = pd.read_sql(qry, sql_connection)
    tableData.to_sql(table,sqliteConnection,if_exists='replace',index=False)
    print(str(count)+" out of  "+str(len(tables))+" "+table+" has been written on sqlite DB")
    
#adds Date of SQlite generation    
now=datetime.now(timezone.utc)
now_str=now.strftime("%Y-%m-%d %H:%M:%S")
sqliteCursor.execute("""UPDATE Configure 
                                 SET 
                                 ConfigValue = '%s'
                                 WHERE
                                 ConfigKey = 'DateOfSqliteGeneration';"""%now_str)
sqliteConnection.commit()        
print("********************************************************************")
print("******** All tables written on sqlite*******************")
print("********************************************************************")
sql_connection.close()
sqliteConnection.close()
#######################################################################