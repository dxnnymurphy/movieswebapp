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

#change as required
DATABASE_NAME = "TestDB"
PASSWORD = "password"

DATABASE_URL = 'mysql+mysqlconnector://root:' + PASSWORD + '@localhost:3306/' + DATABASE_NAME


#Create flask app
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
db = SQLAlchemy(app)


#Establish a spark session
spark = SparkSession.builder.appName("SDP").getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


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


def createDatabase():
    """
        Create the database
    """
    db.create_all()
    db.session.commit()


