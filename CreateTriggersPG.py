# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 09:22:53 2022

@author: Rogger.Correa
POSTGRESS LOGGER

Code to automatically create triggers on all the tables of a postgress DB, 

the triggers should track, insterts, updates or deltes, and write down the details on a time stamped DB

"""



import psycopg2, sqlite3, sys,re
 
#PG conection details
PGdb='yourdatabase'
PGuser='yoursuser'
PGpswd='yourpassword'
PGhost='yourhost.ap-southeast-2.rds.amazonaws.com'
PGport='5432'
PGschema='public'


try:
 
    pgConnection = psycopg2.connect(database=PGdb, user=PGuser, password=PGpswd,
                               host=PGhost, port=PGport)
    pgCursor = pgConnection.cursor()
    # Get the list of tables
    pgCursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = '%s'"""%PGschema)
       
    tables = [i[0] for i in pgCursor.fetchall()]
    #pgCursor.execute("SET search_path TO %s;" %PGschema)
    print ("Conected to PG DB")
except:
    print("error conecting to pg db")

#creates audit table

createAudit= """

CREATE SCHEMA IF NOT EXISTS logging
AUTHORIZATION mapappdatabase;
CREATE TABLE IF NOT EXISTS logging.t_history (
         id SERIAL PRIMARY KEY,
         tstamp timestamp DEFAULT now(),
         schemaname text,
         tabname text,
         operation text,
         who text DEFAULT current_user,
         new_val json,
         old_val json,
         comments text
 );

"""
try:
     # pgCursor.execute("DROP TABLE IF EXISTS logging.t_history;")
     # pgConnection.commit()
     print("table dropped")
     pgCursor.execute(createAudit)
     pgConnection.commit()
     print("Audit table created")
except psycopg2.errors.InsufficientPrivilege:
    
    print(""""PERMISION DENIED A mortal like you shouldn't be trying to do this,
          beg the developer gods to grant you a touch of their power.""")
except:
    print ("Audit table not created for other reason")
    
createFuntion ="""
CREATE FUNCTION change_trigger() RETURNS trigger AS $$
         BEGIN
                IF      TG_OP = 'INSERT'
                THEN
                         INSERT INTO logging.t_history (tabname, schemaname, operation, new_val)
                                 VALUES (TG_RELNAME, TG_TABLE_SCHEMA, TG_OP, row_to_json(NEW));
                         RETURN NEW;
                 ELSIF   TG_OP = 'UPDATE'
                 THEN
                         INSERT INTO logging.t_history (tabname, schemaname, operation, new_val, old_val)
                                 VALUES (TG_RELNAME, TG_TABLE_SCHEMA, TG_OP,
                                         row_to_json(NEW), row_to_json(OLD));
                         RETURN NEW;
                 ELSIF   TG_OP = 'DELETE'
                 THEN
                         INSERT INTO logging.t_history (tabname, schemaname, operation, old_val)
                                 VALUES (TG_RELNAME, TG_TABLE_SCHEMA, TG_OP, row_to_json(OLD));
                         RETURN OLD;
                 END IF;
         END; 
$$ LANGUAGE 'plpgsql' SECURITY DEFINER;
"""
try:
     pgCursor.execute(createFuntion)
     pgConnection.commit()
     print("Function added")
except psycopg2.errors.InsufficientPrivilege:
    
    print(""""PERMISION DENIED A mortal like you shouldn't be trying this,
          beg the developer gods to grant you a touch of their power.""")
except psycopg2.errors.DuplicateFunction:
    print('No worries, function already created')
    pass          
except:
    print ("Audit table not created for other reason")
    
# CREATE TRIGGER ON ALL TABLES OF THE PUBLIC SCHEMA

#REFRESH CONECTION

try:
        pgConnection = psycopg2.connect(database=PGdb, user=PGuser, password=PGpswd,
                                   host=PGhost, port=PGport)
        pgCursor = pgConnection.cursor()
except:
    print("conection not refreshed")

trigger="""
CREATE TRIGGER t_%s
    BEFORE INSERT OR DELETE OR UPDATE 
    ON public."%s"
    FOR EACH ROW
    EXECUTE PROCEDURE %s.change_trigger();
"""
droptrigger="""
DROP TRIGGER IF EXISTs  t_%s
    
    ON public."%s"
    ;
"""


for table in tables:
    print("CREATING TRIGGER ON TABLE: ")
    print(table)
    trigg=trigger%(table,table,PGschema)
    try:
        pgCursor.execute(droptrigger%(table,table))
        pgConnection.commit()
    except:
        print("triggerNotDropped")
    try:
        # pgConnection = psycopg2.connect(database=PGdb, user=PGuser, password=PGpswd,
        #                            host=PGhost, port=PGport)
        # pgCursor = pgConnection.cursor()
        pgCursor.execute(trigger%(table,table,PGschema))
        pgConnection.commit()
    except:
        print("error CREATNG TRIGGER")
    

    
pgConnection.close()
print("*************finished successfully")