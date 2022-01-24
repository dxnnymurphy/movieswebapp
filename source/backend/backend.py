from datetime import datetime

from flask import Flask
from sqlalchemy import Boolean
from flask_sqlalchemy import SQLAlchemy

import pyspark
import pandas as pd

from pyspark.sql import SparkSession

from pyspark.ml.feature import StringIndexer

from pyspark.sql.types import *
from pyspark.sql.functions import *

import os
import shutil

from enum import Enum

DATABASE_NAME = "TestDB"
PASSWORD = "Queensmead11"

DATABASE_URL = 'mysql+mysqlconnector://root:' + PASSWORD + '@localhost:3306/' + DATABASE_NAME


#Create flask app
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
db = SQLAlchemy(app)


#Establish a spark session
spark = SparkSession.builder.appName("SDP").getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

#read from the parquet file
#df = spark.read.parquet("../../parquetFiles/cleaned_films.parquet")

#write to the mysql database
#df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/TestDB") \
#    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Films") \
#    .option("user", "root").option("password", PASSWORD).mode('append').save()

"""Define the schema of the database"""

class Films(db.Model):
    title = db.Column("title", db.String(100), primary_key = True)
    director = db.Column("director", db.String(100))
    airing_date = db.Column("airing_date", db.Date, default=datetime.utcnow)
    country = db.Column("country", db.String(100))
    adult = db.Column("adult", db.Boolean, default=False, nullable=False)
    american = db.Column("american", db.Boolean)
    streaming_service = db.Column("streaming_service", db.String(100),
                                  primary_key = True)
    runtime = db.Column("runtime", db.Integer)

    def __init__(self, title, director, airing_date, country, adult,
                     american, streaming_service, runtime):
        self.title = title
        self.director = director
        self.airing_date = airing_date
        self.country = country
        self.adult = adult
        self.american = american
        self.streaming_service = streaming_service
        self.runtime = runtime


class StreamingService(str, Enum):
    netflix = "Netflix"
    amazon = "Amazon Prime"
    disney = "Disney+"

def createDatabase():
    """
        Create the database
    """
    db.create_all()
    db.session.commit()

def getDBSession():
    """
        Return the db session so the data can be queried
        Type is an SQLAlchemy session
    """
    return db


def addFilm(filmIn):
    """
        Add the supplied film to the database
        Return an error string, or None if successful
    """
    try:
        db.session.add(filmIn)
        db.session.commit()
    except Exception as e:
        return str(e)
    return None

def deleteAllFilms():
    """
        Delete all film data
    """
    output = Films.query.delete()
    db.session.commit()
    return output

def readParquet(fileName):
    """
        read a parquet file from the given filename
        Return a pyspark dataframe
    """
    return spark.read.parquet(fileName)

def addFilms(df):
    """
        Take a pyspark dataframe and save to the database
        Return a string of errors
    """
    try:
        df.write.format("jdbc").option("url", "jdbc:" + DATABASE_URL) \
            .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Films") \
            .option("user", "root").option("password", PASSWORD).mode('append').save()
        print("Successfully stored the dataframe in the database")
    except Exception as e:
        return "Failed to write dataframe to the database" + str(e)
    return None

def getFilmsAsDataframe():
    """
        Read the films database and return a pyspark dataframe
    """
    df = pd.read_sql_table("film", DATABASE_URL)
    return spark.createDataFrame(df)

def queryFilms(query):
    """
        Run the supplied query on the database, and return a pyspark dataframe,
        or None if no data was returned. Bad queries result in None being
        returned
    """
    if(not isinstance(query, str)):
        raise ValueError("Query must be str, not " + type(query))
    try:
        df = pd.read_sql_query(query, DATABASE_URL)
        if(df.empty):
            return None
        return spark.createDataFrame(df)
    except Exception as e:
        print("Failed to execute query: " + str(e))

def filterFilms(streamingService = None, adult = None, american = None,
                minRuntime = None, maxRuntime = None):
    """
        Run an sql query to fetch films with the specified parameters
        Leaving a parameter as None means it will be ignored in the query
        streamingService must be a streamingservice enum
        adult is of type boolean
        american is of type boolean
        minRuntime is a number which is casted to int
        maxRuntime is a number which is casted to int
    """
    if(not (isinstance(streamingService, StreamingService) or streamingService == None)):
        raise ValueError("streamingService must be of type StreamingService, not " + str(type(streamingService)))
    if(not (isinstance(adult, bool) or adult == None)):
        raise ValueError("adult must be of type boolean or None, not " + str(type(adult)))
    if(not (isinstance(american, bool) or american == None)):
        raise ValueError("american must be of type boolean or None, not " + str(type(adult)))

    if(minRuntime != None):
        try:
            minRuntime = int(minRuntime)
        except:
            raise ValueError("minRuntime must be a number")

    if(maxRuntime != None):
        try:
            maxRuntime = int(maxRuntime)
        except:
            raise ValueError("maxRuntime must be a number")

    if(minRuntime != None and maxRuntime != None):
        if(minRuntime >= maxRuntime):
            raise ValueError("minRuntime must be less than maxruntime")

    query = "'1' "

    if(streamingService != None):
        query = query + "AND streaming_service = '" + streamingService + "' "

    if(adult != None):
        adultNumber = 0
        if(adult):
            adultNumber = 1
        query = query + "AND adult = " + str(adultNumber) + " "

    if(american != None):
        americanNumber = 0
        if(american):
            americanNumber = 1
        query = query + "AND american = " + str(americanNumber)

    if(minRuntime != None):
        query = query + "AND runtime >= " + str(minRuntime) + " "

    if(maxRuntime != None):
        query = query + "AND runtime <= " + str(maxRuntime) + " "


    query = "SELECT * FROM films WHERE " + query + ";"

    print("constructed query is: " + query + "\n")
    return queryFilms(query)

#print(deleteAllFilms())

#Testing
#films = filterFilms(minRuntime=10, maxRuntime=20)
#films.show()
#print(films.schema)

#df = readParquet("../../parquetFiles/cleaned_films.parquet")
#df.show()
#addFilms(df)

#createDatabase()

#Testing
#pq = readParquet("../../parquetFiles/cleaned_films.parquet")
#addFilms(pq)


#Add a film to the database
#date = datetime(2022, 3, 3, 10, 10, 10)
#newFilm = Film("rots", "George", date, "far far away", False, False, "Netflix", 180)
#addFilm(newFilm)

#Show the contents of the Film table
#cur.execute("""SELECT * FROM Film""")
#print(cur.fetchall())
