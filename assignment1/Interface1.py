import psycopg2


RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath , openconnection):
    try:
        with openconnection.cursor() as cur:
            ratingTable = open(ratingsfilepath,'r')
            
            #create ratings table
            cur.execute('CREATE TABLE {0} (userid int,movieid int,rating float);'
            .format(ratingstablename))

            #read input file and transfer the data to the ratings table
            for row in ratingTable:
                [userid, movieid, rating, utp] = row.split('::')
                #handle null userid case
                if userid == '':
                    userid = 0
                #handle null movieid case
                if movieid == '':
                    movieid = 0
                cur.execute('INSERT INTO {0} VALUES({1}, {2}, {3});'
                .format(ratingstablename, userid, movieid, rating))
            cur.close()
            openconnection.commit()
    except Exception as e:
        traceback.print_exc()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    # Throw an error if number of partitions is less than 1
    # or if number of partitions is not integer
    if(numberofpartitions < 1 or not isinstance(numberofpartitions, int) ):
        print('Invalid number of partitions')
        return
    try:
        with openconnection.cursor() as cur:
            #create number of range tables equal to number of partitions
            interval = 5.0 / numberofpartitions
            
            for i in range (numberofpartitions):
                minval = i*interval
                maxval = minval + interval
                #print(minval, maxval)
                cur.execute('CREATE TABLE {0}{1} (userid int,movieid int,rating float);'
                .format(RANGE_TABLE_PREFIX,i))
                
                # For i=0 table contains [0, interval]
                # For i>0 table contains (interval, i*interval]
                if (i == 0):
                    cur.execute('''
                    INSERT INTO {0}{1} 
                    SELECT * FROM {2}
                    WHERE rating >= {3} AND rating <= {4};
                    '''.format(RANGE_TABLE_PREFIX,i,ratingstablename,minval,maxval))
                else:
                    cur.execute('''
                    INSERT INTO {0}{1} 
                    SELECT * FROM {2}
                    WHERE rating > {3} AND rating <= {4};
                    '''.format(RANGE_TABLE_PREFIX,i,ratingstablename,minval,maxval))

                openconnection.commit()

            cur.close()
            openconnection.commit()
    except Exception as e:
        traceback.print_exc()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    # Throw an error if number of partitions is less than 1
    # or if number of partitions is not integer
    if(numberofpartitions < 1 or not isinstance(numberofpartitions, int) ):
        print('Invalid number of partitions')
        return
    try:
        with openconnection.cursor() as cur:
            #create round robin tables equal to number of partitions
            for i in range (numberofpartitions):
                cur.execute('CREATE TABLE {0}{1} (userid int,movieid int,rating float);'
                .format(RROBIN_TABLE_PREFIX,i))
                
                #Take modulo of row number with number of partitions and get partition number
                # One is subtracted from row numbersince postgres enrtry starts from 1
                #  and round robin tables are from 0
                cur.execute('''
                INSERT INTO {0}{1} 
                SELECT * FROM
                (SELECT ROW_NUMBER() OVER(ORDER BY userid) FROM {2}) AS foo
                WHERE(row_number-1)%{3} = {4};
                '''.format(RROBIN_TABLE_PREFIX,i, ratingstablename,numberofpartitions,i))
        

                openconnection.commit()
            
            cur.close()
            openconnection.commit()
    except Exception as e:
        traceback.print_exc()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    #Create a set for valid ratings 
    ratingSet = {0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0}
    if(rating not in ratingSet):
        print('Invalid rating')
        return
    try:
        with openconnection.cursor() as cur:
            #Insert entry into ratings table
            cur.execute('''
            INSERT INTO {0}
            VALUES({1},{2},{3});
            '''.format(ratingstablename,userid,itemid,rating))
            
            openconnection.commit()

            #Get count of the data in ratingtable to get rownumber of inserted data 
            cur.execute('''
            SELECT COUNT(*)
            FROM {0}
            '''.format(ratingstablename))

            rowNumber = int(cur.fetchone()[0])
            
            #Get number of partitions by counting number of round robin tables
            cur.execute('''
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name LIKE '{0}%';
            '''.format(RROBIN_TABLE_PREFIX) )
            
            numberofpartitions = int(cur.fetchone()[0]) 

            # One is subtracted from row numbersince postgres enrtry starts from 1
            #  and round robin tables are from 0
            partitionNumber = (rowNumber-1)%numberofpartitions

            cur.execute('''
            INSERT INTO {0}{1}
            VALUES({2}, {3}, {4});
            '''.format(RROBIN_TABLE_PREFIX,partitionNumber,userid,itemid,rating))
            
            cur.close()
            openconnection.commit()
    
    except Exception as e:
        traceback.print_exc()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    #Create a set for valid ratings 
    ratingSet = {0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0}
    if(rating not in ratingSet):
        print('Invalid rating')
        return
    try:
        with openconnection.cursor() as cur:
            #Insert entry into ratings table
            cur.execute('''
            INSERT INTO {0}
            VALUES({1},{2},{3});
            '''.format(ratingstablename,userid,itemid,rating))
        
            openconnection.commit()

            #Get number of partitions by counting number of range tables
            cur.execute('''
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name LIKE '{0}%';
            '''.format(RANGE_TABLE_PREFIX))
            
            numberofpartitions = int(cur.fetchone()[0]) 


            minval = 0.0
            interval = 5.0 / numberofpartitions
            partitionNumber = 0

            #If rating is less than interval insert it into partition 0 else count the partition number
            if(rating <= interval):
                cur.execute('''
                INSERT INTO {0}{1}
                VALUES({2}, {3}, {4});
                '''.format(RANGE_TABLE_PREFIX,partitionNumber,userid,itemid,rating))
            
            else:
                while True:
                    #print(partitionNumber)
                    if(minval >= rating):
                        partitionNumber = partitionNumber - 1
                        break
                    else:
                        partitionNumber = partitionNumber + 1
                        minval = minval + interval
                cur.execute('''
                INSERT INTO {0}{1}
                VALUES({2}, {3}, {4});
                '''.format(RANGE_TABLE_PREFIX,partitionNumber,userid,itemid,rating))

            cur.close()
            openconnection.commit()
    
    except Exception as e:
        traceback.print_exc()


def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
