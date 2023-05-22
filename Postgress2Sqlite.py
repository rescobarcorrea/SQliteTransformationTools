# -*- coding: utf-8 -*-
"""


Created on Tue Aug 30 09:35:48 2022

@author: Rogger.Correa

POSTGRESS to sqlite

"""

import psycopg2, sqlite3

from datetime import datetime
from datetime import timezone
import decimal
# Create conections
#Change these values as needed
 
#SQdb=r'P:\MapComponents\Projects\MobileDB\FCNSW.sqlite'
SQdb=r'D:\SQliteSync\FCNSW.sqlite'
SQlike=''

PGdb='yourdatabase'
PGuser='youruser'
PGpswd='yourpassword'
PGhost='yourhost.ap-southeast-2.rds.amazonaws.com'
PGport='5432'
PGschema='public'

# create a sqlite db
sqliteConnection = sqlite3.connect(SQdb)
sqliteCursor = sqliteConnection.cursor()
# ref: http://hakanu.net/sql/2015/08/25/sqlite-unicode-string-problem/
sqliteConnection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')

# connect to postgresql

pgConnection = psycopg2.connect(database=PGdb, user=PGuser, password=PGpswd,
                               host=PGhost, port=PGport)

pgCursor = pgConnection.cursor()

# Get the list of tables
pgCursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
       
tables = [i[0] for i in pgCursor.fetchall()]
# get code and types from the pgDB
pgCursor.execute("select oid,typname from pg_type;")
pgTypesCodes = [i[0] for i in pgCursor.fetchall()]
#pgTypesCodes=np.array(pgTypesCodes)
pgCursor.execute("select oid,typname from pg_type;")
pgTypes = [i[1] for i in pgCursor.fetchall()]
#pgTypes=np.array(pgTypes)
# replace things on the pgTypes

pgTypes[pgTypes.index('bpchar')]='varchar'
pgTypes[pgTypes.index('float8')]='float'
pgTypes[pgTypes.index('int8')]='integer'
pgTypes[pgTypes.index('int2')]='integer'
pgTypes[pgTypes.index('int4')]='integer'
pgTypes[pgTypes.index('numeric')]='float'


def get_constrains(schema,table):
    sql_text="""
    select pgc.conname as constraint_name,
       ccu.column_name,
       contype,
        pg_get_constraintdef(pgc.oid)
from pg_constraint pgc
         join pg_namespace nsp on nsp.oid = pgc.connamespace
         join pg_class  cls on pgc.conrelid = cls.oid
         left join information_schema.constraint_column_usage ccu
                   on pgc.conname = ccu.constraint_name
                       and nsp.nspname = ccu.constraint_schema
where
    ccu.table_schema= '%s' and ccu.table_name = '%s'
    
;
                      """
    pgCursor.execute(sql_text %(schema,table))
    constrains=pgCursor.fetchall()
    return constrains


def get_defaults(schema,table):
    sql_text="""
    SELECT column_name, column_default
FROM information_schema.columns
WHERE (table_schema, table_name) = ('%s', '%s')

    
;
                      """
    pgCursor.execute(sql_text %(schema,table))
    defaults=pgCursor.fetchall()
    return defaults



for table in tables:
    
    
    print("---creating table: ")
    print(table)
    #pgCursor.execute(""" SELECT * FROM """ +'"'+ table +'"')
    #table='"'+table+'"'
    
    pgCursor.execute("""SELECT * FROM "%s";""" %table)
    rows=pgCursor.fetchall()
    cols=pgCursor.description
    table_dll=''
    defaults=get_defaults('public',table)
    for column in  cols:
        default=defaults[cols.index(column)][1]
        name=column.name
        #print(name)
        precision=column.precision
        scale=column.scale
       # print(precision,scale)
        type_code=column.type_code
        typePg=pgTypes[pgTypesCodes.index(type_code)]
        if typePg == "varchar":
            typePg ="varchar (0,0)"
        
        if cols.index(column)==0:
            #starts writing the SQlite sintax of the table (table dll)
            if bool (default):
                table_dll="[%s] %s DEFAULT %s"% (name,typePg.upper(),default)
            else:
                table_dll="[%s] %s"% (name,typePg.upper())
        else:
            if bool (default):
                column_str2="[%s] %s DEFAULT %s"% (name,typePg.upper(),default)
                table_dll+=' , '+column_str2
            else:
                #add the commas and enxt elements
                column_str2="[%s] %s"% (name,typePg.upper())
                table_dll+=' , '+column_str2

    
    
        
        
    #Get contrains
    constrains=get_constrains(PGschema, table)
    # set contrains as text
    
    #con_type.replace('"')
    
    for constrain in constrains:
        con_name=constrain[0]
        con_type=constrain[3]
        cons_str="""
        CONSTRAINT %s %s
        """
        
        table_dll+=' , '+cons_str %(con_name,con_type)
    #print(table_dll) 
    sqliteCursor.execute("DROP TABLE IF EXISTS %s;" %table)
    sqliteConnection.commit()
    
    sqliteCursor.execute("CREATE TABLE IF NOT EXISTS [%s](%s);" % (table,table_dll))
    sqliteConnection.commit()
    #format stuff, cna be done better
    try:
        colcount=len(rows[0])
    except IndexError:
        colcount=0
    pholder='?,'*colcount
    newholder=pholder[:-1]
    if bool(rows):
        
       #here, it checks each value and make sure it's not gonna cause issues
        
        newrows=[]
        for row in rows:
            newcols=[]
            for col in row:
                #decimal types causes issues on sql when writing, so have to change it
                if isinstance(col,str):
                    col=col.strip()
                if isinstance (col,decimal.Decimal):
                    col=float(col)
                newcols.append(col)
            #newcols=tuple(newcols)
            newrows.append(newcols)
        try:
            
            sqliteCursor.executemany("INSERT INTO %s VALUES (%s);" % (table, newholder),newrows)
            sqliteConnection.commit()
            print('****** Created', table)
        except:
            print('************************-some error or warning-',table)
            continue
        
        
        
            
#adds Date of SQlite generation    
now=datetime.now(timezone.utc)
now_str=now.strftime("%Y-%m-%d %H:%M:%S")
sqliteCursor.execute("""UPDATE Configure 
                                 SET 
                                 ConfigValue = '%s'
                                 WHERE
                                 ConfigKey = 'DateOfSqliteGeneration';"""%now_str)
sqliteConnection.commit()        
        
print("***************FINISHED**************")

# close all connections
sqliteConnection.close()
pgConnection.close()