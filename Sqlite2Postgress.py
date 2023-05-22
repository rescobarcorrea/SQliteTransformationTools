# -*- coding: utf-8 -*-
"""
Created on Wed Jun 29 10:14:17 2022

@author: Rogger.Correa
Modifyed from 

#Frank Donnelly, Geospatial Data Librarian
#May 22, 2017
#Copies tables and data from a SQLite database and recreates them
#in a PostgreSQL database


"""

 
import psycopg2, sqlite3, sys,re
 
#Change these values as needed
 
SQdb=r'P:\MapComponents\Projects\MobileDB\FCNSW.sqlite'
SQlike=''
PGdb='yourdatabase'
PGuser='yoursuser'
PGpswd='yourpassword'
PGhost='yourhost.ap-southeast-2.rds.amazonaws.com'
PGport='5432'
PGschema='public'
 
conSQ=sqlite3.connect(SQdb)
curSQ=conSQ.cursor()
 
tabnames=[]
 
#curSQ.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%s'" % sqlike)
curSQ.execute("SELECT name FROM sqlite_master WHERE type='table'")
SqliteUniqueWords=[]
tabgrab = curSQ.fetchall()
for item in tabgrab:
    tabnames.append(item[0])
    #print(item[0])
if 'sqlite_sequence' in tabnames:
    tabnames.remove('sqlite_sequence')
j=0
for table in tabnames:
    #table="BankSpacingReq"
    print ('... working on table '+ table+ ' ' + str(j) +' of ' +str(len(tabnames)))
    curSQ.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?;", (table,))
    create = curSQ.fetchone()[0]
    #remove double spaces
    splits = create.split()
    create=' '.join(splits)
    #create=create.replace('(0, 0)','')
    create=create.replace('[','"')
    create=create.replace(']','"')
    create=create.replace('INT64','NUMERIC')
    create=create.replace('INTEGER PRIMARY KEY AUTOINCREMENT','SERIAL PRIMARY KEY')
    create=create.replace('INTEGER PRIMARY KEY ASC AUTOINCREMENT','SERIAL PRIMARY KEY')  
    create=create.replace('TEXT','CHAR')
    create=create.replace('DOUBLE','DOUBLE PRECISION')
    #remove lenght limitations to columns in format (number, number)
    str_format="\(\d{1,3}\,\s\d{1,3}\)"
    create=re.sub(str_format,'',create)

    
    
        
    create=create.replace('(','( ')
    create=create.replace(')',' )')
    create=create.replace(',',' , ')
    splits = create.split()
    
    pkey=True
    if "PRIMARY KEY" not in create:
       print(table,"****************no primary key here")
       pkey=False
      
    
    #for loop to iterate over words and check for quotes on words
    i=0
    commacount=0# to add pkey just before the first comma
    for split in splits:
        #add primary key to table if there is not primary key on it
        if not pkey and (split == ',') and commacount==0:
            splits[i]=' PRIMARY KEY'+split
            commacount+=1
        #add sigle quotes fro words after default
        if "DEFAULT" in split:
                targetString=splits[i+1]
                if  'null' not in targetString and 'NULL' not in targetString and 'false' not in targetString and 'true' not in targetString and '('not in targetString:
                
                    targetString="'" + targetString+ "'"
                    splits[i+1]=targetString
        #ADDS QUTOES ON WORDS qihtout quotes that needs quotes for postgress
        if 'null' not in split and 'false' not in split and 'true' not in split and not split.isupper() and split.isalnum() and not split.isnumeric():
                
                splits[i]='"'+ splits[i] +'"'
                #print('*****',splits[i])

        i+=1
    create=' '.join(splits)
    create=create.replace(' , ',',')
    create=create.replace('( ','(')
    create=create.replace(' )',')')

              
    # if 'DEFAULT' in create:
        
    #     create=create.replace('(','( ')
    #     create=create.replace(')',' )')
    #     create=create.replace(',',' ,')
    #     splits = create.split()
    #     #for loop to iterate over words array and add quotes to words after DEFAULT
    #     i=0
    #     for split in splits:
            
    #         if "DEFAULT" in split:
    #             targetString=splits[i+1]
    #             if  ('null' and 'NULL' and 'false' and 'true' and '(') not in targetString:
                
    #                 targetString="'" + targetString+ "'"
    #                 splits[i+1]=targetString
    #         i+=1
    #     create=' '.join(splits)
    #     create=create.replace('( ','(')
    #     create=create.replace(' )',')')
    
    curSQ.execute("SELECT * FROM %s;" %table)
    rows=curSQ.fetchall()
    
    try:
        colcount=len(rows[0])
    except IndexError:
        colcount=0
    pholder='%s,'*colcount
    newholder=pholder[:-1]
    #here I replace single wuotes to doublequoets
    table='"'+table+'"'
 
    try:
 
        conPG = psycopg2.connect(database=PGdb, user=PGuser, password=PGpswd,
                               host=PGhost, port=PGport)
        curPG = conPG.cursor()
        curPG.execute("SET search_path TO %s;" %PGschema)
        curPG.execute("DROP TABLE IF EXISTS %s;" %table)
        conPG.commit()
        curPG.execute(create)
        conPG.commit()
        try:
            print("Inserting values")
            curPG.executemany("INSERT INTO %s VALUES (%s);" % (table, newholder),rows)
            conPG.commit()
        except psycopg2.errors.UniqueViolation as e:
            print(table,"@@@@@@@@@@@@ this table has no PKEY")
            print(e)
            pass
        print('Created', table)
 
    except psycopg2.DatabaseError as e:
        print ('Error %s' % e) 
        sys.exit(1)
 
    finally:
 
        if conPG:
            conPG.close()
    j+=1
conSQ.close()
print ("*************    Finished  *******************")

    # if '"' not in create:
        
    #     create=create.replace('(','( ')
    #     create=create.replace(')',' )')
    #     splits = create.split()
        
    #     #for loop to iterate over words array and add quotes to words after DEFAULT
    #     i=0
    #     for split in splits:
            
    #         if not split.isupper() and split.isalnum() and not split.isnumeric():
                    
    #                 splits[i]='"'+splits[i]+'"'
    #                 print(splits[i])
    #         i+=1
    #     create=' '.join(splits)
    #     create=create.replace('( ','(')
    #     create=create.replace(' )',')')
    #     print (table, 'here')