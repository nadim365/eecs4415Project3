"""
    This Spark app connects to the data source script running in another Docker container on port 9999, which feeds the
    Spark app with a stream of data from the get requests made to the GitHub Search API.
    The application receives the data stream, divides it into batches at an interval of 60 seconds (batch duration = 60 seconds),
    and performs:
        1. Compute the total number of collected repos since the start of the streaming application for each of the 3 programming languages (Pyhton, Java, C). Each repo should only be counted once.
        2. Compute the number of collected repos with changes pushed during the last 60 seconds for all the repositories. Each repo should only be counted once.
        3. compute the average number of stars of all the collected repos since the start of the streaming application for each of the 3 programming languages.
        4. Find the top 10 most frequent words in the description of all the collected repos since the start of the streaming application for each of the 3 programming languages.
        5. Print the analysis results of each batch to the console.
"""
import sys
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']


