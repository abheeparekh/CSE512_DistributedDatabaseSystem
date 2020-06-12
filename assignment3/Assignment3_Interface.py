#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    # to get the output in a file make the testMode true else false
    testMode = True
    try:
        cursor = openconnection.cursor()
        rangeName = 'range_part'
        #get schema of the InputTable
        cursor.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}'".format(InputTable))
        schema = cursor.fetchall()
        
        # get the maximum value of a given column
        cursor.execute('SELECT MAX({0}) FROM {1}'.format(SortingColumnName, InputTable))
        maxValue = cursor.fetchone()[0]
        
        # get the minimum value of a given column
        cursor.execute('SELECT MIN({0}) FROM {1}'.format(SortingColumnName, InputTable))
        minValue = cursor.fetchone()[0]
        
        # set the number of threads which are to be created
        numberOfThreads = 5
        
        delta = (maxValue - minValue)/numberOfThreads
        
        # create table for range partitioning
        for i in range (numberOfThreads):
            iMin = minValue + i*delta
            iMax = iMin + delta
            cursor.execute('DROP TABLE IF EXISTS {0}{1}' .format(rangeName,i))
            cursor.execute('CREATE TABLE {0}{1} ({2} {3})'.format(rangeName, i, schema[0][0], schema[0][1]))

            for j in range (1, len(schema)):
               cursor.execute('ALTER TABLE {0}{1} ADD COLUMN {2} {3}'.format(rangeName, i, schema[j][0], schema[j][1]))

        # start the operation
        thread = [0]*numberOfThreads
        for i in range (numberOfThreads):
            iMin = minValue + i*delta
            iMax = iMin + delta
            thread[i] = threading.Thread(target = ThreadSorting, 
                        args= (InputTable, SortingColumnName, rangeName, i, iMin, iMax, openconnection))
            thread[i].start()
        
        # wait for all threads to join after they complete their operation
        for i in range (numberOfThreads):
            thread[i].join()

        #create the output table
        cursor.execute('DROP TABLE IF EXISTS {0}'.format(OutputTable))
        cursor.execute('CREATE TABLE {0} ({1} {2})'.format(OutputTable, schema[0][0], schema[0][1]))
        for j in range (1, len(schema)):
           cursor.execute('ALTER TABLE {0} ADD COLUMN {1} {2}'.format(OutputTable, schema[j][0], schema[j][1]))
        
        # insert the sorted range partitioned table serially into output table
        # the output table generated will be sorted
        for i in range (numberOfThreads):
            cursor.execute('INSERT INTO {0} SELECT * FROM {1}{2}'.format(OutputTable, rangeName, i))

        # if testMode is selected the output will be written in the file
        if (testMode):
            cursor.execute('SELECT * FROM {0}'.format(OutputTable))
            f = open('sortOutput.txt','w')
            while True:
                output = cursor.fetchone()
                if output == None:
                    break
                f.write(str(output) + '\n')
            print('Check sortOutput.txt file')
            f.close()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        openconnection.commit()

def ThreadSorting (InputTable, SortingColumnName, rangeName, i, iMin, iMax, openconnection):
    # sorting function which each thread will execute independently 
    # this function will insert data into the range partitioned table in ascending order
    try:
        cursor = openconnection.cursor()       
        if(i ==0):
            cursor.execute('INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} >= {4} AND {3} <= {5} ORDER BY {3} ASC' \
            .format(rangeName, i, InputTable, SortingColumnName, iMin, iMax))
        else:
            cursor.execute('INSERT INTO {0}{1} SELECT * FROM {2} WHERE {3} > {4} AND {3} <= {5} ORDER BY {3} ASC' \
            .format(rangeName, i, InputTable, SortingColumnName, iMin, iMax))

    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        openconnection.commit()
    
#pass #Remove this once you are done with implementation

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation
    testMode = True
    try:
        cursor = openconnection.cursor()
        rangeName1 = 'range_table1_part'
        rangeName2 = 'range_table2_part'
        outputName = 'output_table'
        
        #get schema of the InputTable1
        cursor.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}'".format(InputTable1))
        schema1 = cursor.fetchall()
        #get schema of the InputTable2
        cursor.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}'".format(InputTable2))
        schema2 = cursor.fetchall()
        
        # get the maximum value of the join column from InputTable1
        cursor.execute('SELECT MAX({0}) FROM {1}'.format(Table1JoinColumn, InputTable1))
        maxValue1 = cursor.fetchone()[0]
        # get the minimum value of the join column from InputTable1
        cursor.execute('SELECT MIN({0}) FROM {1}'.format(Table1JoinColumn, InputTable1))
        minValue1 = cursor.fetchone()[0]
        
        # get the maximum value of the join column from InputTable2
        cursor.execute('SELECT MAX({0}) FROM {1}'.format(Table2JoinColumn, InputTable2))
        maxValue2 = cursor.fetchone()[0]
        # get the minimum value of the join column from InputTable2
        cursor.execute('SELECT MIN({0}) FROM {1}'.format(Table2JoinColumn, InputTable2))
        minValue2 = cursor.fetchone()[0]

        # get maximum and minimum value of the join column
        if maxValue1 < maxValue2:
            maxColumnValue = maxValue2
        else:
            maxColumnValue = maxValue1

        if minValue1 < minValue2:
            minColumnValue = minValue1
        else: 
            minColumnValue = minValue2
        
        # set number of threads as 5
        numberOfThreads = 5

        delta = (maxColumnValue - minColumnValue)/numberOfThreads
        
        for i in range (numberOfThreads):
            iMin = minColumnValue + i*delta
            iMax = iMin + delta
            
            cursor.execute('DROP TABLE IF EXISTS {0}{1}' .format(rangeName1, i))
            cursor.execute('DROP TABLE IF EXISTS {0}{1}' .format(rangeName2, i))
            # create range partition table 
            if i == 0:    
                cursor.execute('CREATE TABLE {0}{1} AS SELECT * FROM {2} WHERE {3} >= {4} AND {3} <= {5}'
                .format(rangeName1, i, InputTable1, Table1JoinColumn, iMin, iMax))
                
                cursor.execute('CREATE TABLE {0}{1} AS SELECT * FROM {2} WHERE {3} >= {4} AND {3} <= {5}'
                .format(rangeName2, i, InputTable2, Table2JoinColumn, iMin, iMax))

            else:
                cursor.execute('CREATE TABLE {0}{1} AS SELECT * FROM {2} WHERE {3} > {4} AND {3} <= {5}'
                .format(rangeName1, i, InputTable1, Table1JoinColumn, iMin, iMax))
                
                cursor.execute('CREATE TABLE {0}{1} AS SELECT * FROM {2} WHERE {3} > {4} AND {3} <= {5}'
                .format(rangeName2, i, InputTable2, Table2JoinColumn, iMin, iMax))

        for i in range(numberOfThreads):
            cursor.execute('DROP TABLE IF EXISTS {0}' .format(outputName))
            
            # create tables to store the join results of each thread
            cursor.execute('CREATE TABLE {0}{1} ({2} {3})'.format(outputName, i, schema1[0][0], schema1[0][1]))
            for j in range (1, len(schema1)):
                cursor.execute('ALTER TABLE {0}{1} ADD COLUMN {2} {3}'.format(outputName, i, schema1[j][0], schema1[j][1]))
            for j in range (len(schema2)):
                cursor.execute('ALTER TABLE {0}{1} ADD COLUMN {2} {3}'.format(outputName, i, schema2[j][0], schema2[j][1]))

        thread = [0]*numberOfThreads
        for i in range (numberOfThreads):
            iMin = minColumnValue + i*delta
            iMax = iMin + delta
            thread[i] = threading.Thread(target = ThreadJoin, 
                        args= (Table1JoinColumn, Table2JoinColumn, i, rangeName1, rangeName2, outputName, openconnection))
            thread[i].start()
        
        for i in range (numberOfThreads):
            thread[i].join()

        # create output table to store the join result
        cursor.execute('DROP TABLE IF EXISTS {0}' .format(OutputTable))
        cursor.execute('CREATE TABLE {0} ({1} {2})'.format(OutputTable, schema1[0][0], schema1[0][1]))
        
        for i in range (1, len(schema1)):
            cursor.execute('ALTER TABLE {0} ADD COLUMN {1} {2}'.format(OutputTable, schema1[i][0], schema1[i][1]))
        for i in range (len(schema2)):
            cursor.execute('ALTER TABLE {0} ADD COLUMN {1} {2}'.format(OutputTable, schema2[i][0], schema2[i][1]))
        
        # insert the join results computed by each thread into the output table
        for i in range (numberOfThreads):
            cursor.execute('INSERT INTO {0} SELECT * FROM {1}{2}'.format(OutputTable, outputName, i))
        
        # if testMode is selected the output will be written in the file
        if (testMode):
            cursor.execute('Select * from ratings, movies where movies.movieid1 = ratings.movieid')
            f = open('outputJoin.txt','w')
            while True:
                output = cursor.fetchone()
                if output == None:
                    break
                f.write(str(output) + '\n')
            print('Check outputJoin.txt file')
            f.close()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        openconnection.commit()

def ThreadJoin(Table1JoinColumn, Table2JoinColumn, i, rangeName1, rangeName2, outputName, openconnection):
    # perform inner join on partitioned table
    # each thread will perform this operation independently on the assigned partitioned table
    try:
        cursor = openconnection.cursor()
        cursor.execute('INSERT INTO {0}{1} SELECT * FROM {2}{1} INNER JOIN {3}{1} ON {2}{1}.{4} = {3}{1}.{5}'
        .format(outputName, i, rangeName1, rangeName2, Table1JoinColumn, Table2JoinColumn))

    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
        openconnection.commit()
################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


