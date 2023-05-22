# -*- coding: utf-8 -*-
"""
Sqlite to SQL

Created on Tue Mar 28 11:07:06 2023

@author: Rogger.Correa

"""

import sqlite3
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
######################## SQL server connection ########################

server="yourserver"
sqldb="yourdatabase"
user="youruser"
password="yourpassword"

ALCHEMY_URL = URL.create(
"mssql+pyodbc",
username=user,
password=password,
host=server,
database=sqldb,
query={
    "driver": "ODBC Driver 17 for SQL Server",
},)
engine = create_engine(ALCHEMY_URL)


######################### sqlite  conection  #########################

SQlitedb=r'P:\MapComponents\Projects\MobileDB\FCNSW.sqlite'
SQlike=''
# create a sqlite db
sqlite_Connection = sqlite3.connect(SQlitedb)
#sqliteCursor = sqliteConnection.cursor()
# ref: http://hakanu.net/sql/2015/08/25/sqlite-unicode-string-problem/
#sqliteConnection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')

####################### read and wirte tables ###############################
print("********************************************************************")
print("Writing SQliteDB: "+SQlitedb)
print(" onto SQLserver: "+sqldb+"@"+server)
print("********************************************************************")
############################################################################
###########################FUNCTIONS########################################




############################################################################

#this reads a list of tables
qry="SELECT name FROM sqlite_master WHERE type='table';"
tables = list( pd.read_sql(qry, sqlite_Connection)['name'])
tables.remove("sqlite_sequence")

count=0
for table in tables:
    count+=1
    sqlite_Connection = sqlite3.connect(SQlitedb)
    qry="SELECT * FROM %s"%table
    df = pd.read_sql(qry, sqlite_Connection)
    

    with engine.connect() as cnn:
          df.to_sql(table,con=cnn, if_exists='replace', index=False)

    ############    Constrains   and defaults    #########################

    #read defauls and not nulls
    qry="pragma table_info('%s')"%table
    table_info = pd.read_sql(qry, sqlite_Connection)
    #write defaults and not nulls
    if not table_info.empty:
       
        for index,column in table_info.iterrows():
            
            if column['notnull'] == 1:
                with engine.connect() as con:
                    coltype=column['type'].lower()
                    if "varchar" in coltype :
                        coltype ="varchar(max)"

                    try:
                        qry="ALTER TABLE %s ALTER COLUMN [%s] %s NOT NULL;"%(table,column['name'],coltype)
                        rs = con.execute(qry)
                    except sqlalchemy.exc.IntegrityError:
                        pass
            if column['dflt_value'] is not None:
                #ignoring false NAD CURRETN EXTENTbecause it doens exist on SQL...need to find a better way to deal with it
                with engine.connect() as con:
                    try:
                        qry="ALTER TABLE %s ADD CONSTRAINT df_%s DEFAULT '%s' FOR [%s];"%(table,column['name'],str(column['dflt_value']),column['name'])
                        rs = con.execute(qry)
                    except sqlalchemy.exc.IntegrityError:
                        pass
                    except sqlalchemy.exc.ProgrammingError as e:
                        #print(e)
                        if "There is already an object named" in str(e):
                            pass
            if column['pk'] == 1:
                
                    coltype=column['type'].lower()
                    if "varchar" in coltype :
                        coltype ="nvarchar(450)"
                    with engine.connect() as con:
                        try:
                            qry="ALTER TABLE %s ALTER COLUMN [%s] %s NOT NULL;"%(table,column['name'],coltype)
                            rs = con.execute(qry)
                        except sqlalchemy.exc.IntegrityError:
                            pass
                    with engine.connect() as con:
                        try:
                            qry="ALTER TABLE %s ADD PRIMARY KEY ([%s]);"%(table,column['name'])
                            rs = con.execute(qry)
                        except sqlalchemy.exc.IntegrityError:
                            pass
                    
    ########################### reads constrains ########################################
    qry="pragma index_list('%s')"%table
    constrains = pd.read_sql(qry, sqlite_Connection)

    
    #write unique constrains    
    if not constrains.empty:
        print('********')
        for constrain in constrains.iterrows():
            if constrain[1]['origin'] != 'pk':
                constrainName=str(constrain[1]['name'])
                qry="pragma index_info('%s')"%constrainName
                constrainDetails = pd.read_sql(qry, sqlite_Connection)
                columnConstrained=constrainDetails['name'][0]
                coltype=table_info[table_info['name']==columnConstrained]['type'].iloc[0].lower()
                
                if "varchar" in coltype :
                    coltype ="nvarchar(450)"
                    with engine.connect() as con:
                        try:
                            qry="ALTER TABLE %s ALTER COLUMN [%s] %s NOT NULL;"%(table,columnConstrained,coltype)
                            rs = con.execute(qry)
                        except sqlalchemy.exc.IntegrityError:
                            pass
                
                with engine.connect() as con:
                    try:
                        qry="ALTER TABLE %s ADD UNIQUE([%s]);"%(table,columnConstrained)
                        rs = con.execute(qry)
                    except sqlalchemy.exc.IntegrityError:
                        pass           
            
        
    





    print(str(count)+" out of  "+str(len(tables))+" "+table+" has been written on SQL server")



print("********************************************************************")
print("******** All tables written on SQL server *******************")
print("********************************************************************")
#sql_connection.close()
sqlite_Connection.close()