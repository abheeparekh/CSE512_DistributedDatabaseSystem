
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    rangeName   = "RangeRatingsPart"
    rrName      = "RoundRobinRatingsPart"
    f = open(outputPath, 'w')
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from RangeRatingsMetadata")
        rangeMetaData = cursor.fetchall()
        #print(rangeMetaData)
        #print(rangeMetaData[3][1])
        for i in range (len(rangeMetaData)):
            minValue = rangeMetaData[i][1]
            maxValue = rangeMetaData[i][2]
            if  (minValue <= ratingMaxValue and maxValue >= ratingMinValue):
                #print(minValue, maxValue, i)
                cursor.execute("select * from {0}{1} WHERE Rating <= {2} AND Rating >= {3}"
                .format(rangeName,i, ratingMaxValue, ratingMinValue))
                queryResult  = cursor.fetchall()
                for row in queryResult:
                    f.write('{0}{1},{2},{3},{4} \n'.format(rangeName,i,row[0],row[1],row[2]))   

        cursor.execute("select * from RoundRobinRatingsMetadata")
        rrmetadata = cursor.fetchall()
        numberOfPartitions = rrmetadata[0][0]
        # print(numberOfPartitions)
        for i in range (numberOfPartitions):
            cursor.execute("select * from {0}{1} WHERE Rating <= {2} AND Rating >= {3}"
            .format(rrName,i, ratingMaxValue, ratingMinValue))
            queryResult  = cursor.fetchall()
            for row in queryResult:
                f.write('{0}{1},{2},{3},{4} \n'.format(rrName,i,row[0],row[1],row[2])) 
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
    #pass #Remove this once you are done with implementation

def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    rangeName   = "RangeRatingsPart"
    rrName      = "RoundRobinRatingsPart"
    f = open(outputPath, 'w')
    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from RangeRatingsMetadata")
        rangeMetaData = cursor.fetchall()
        #print(rangeMetaData)
        #print(rangeMetaData[3][1])
        for i in range (len(rangeMetaData)):
            minValue = rangeMetaData[i][1]
            maxValue = rangeMetaData[i][2]
            if  (minValue <= ratingValue and maxValue >= ratingValue):
                #print(minValue, maxValue, i)
                cursor.execute("select * from {0}{1} WHERE Rating = {2}"
                .format(rangeName,i, ratingValue))
                queryResult  = cursor.fetchall()
                for row in queryResult:
                    f.write('{0}{1},{2},{3},{4} \n'.format(rangeName,i,row[0],row[1],row[2]))   

        cursor.execute("select * from RoundRobinRatingsMetadata")
        rrmetadata = cursor.fetchall()
        numberOfPartitions = rrmetadata[0][0]
        # print(numberOfPartitions)
        for i in range (numberOfPartitions):
            cursor.execute("select * from {0}{1} WHERE Rating = {2}"
            .format(rrName,i, ratingValue))
            queryResult  = cursor.fetchall()
            for row in queryResult:
                f.write('{0}{1},{2},{3},{4} \n'.format(rrName,i,row[0],row[1],row[2])) 
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
    #pass # Remove this once you are done with implementation
