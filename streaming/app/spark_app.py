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
import json
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def process_rdd(time, rdd):
    print("---------------- %s ----------------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda repo: Row(lang=repo[0], count=repo[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("repos")
        new_results_df = sql_context.sql("select lang, count from repos")
        new_results_df.show()
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]

if __name__ == '__main__':
    DATA_SOURCE_IP = 'data-source'
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName='GitHubSearch')
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint('checkpoint_GitHubSearch')
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    repos = data.flatMap(lambda repo: [repo.split('\t')])
    counts = repos.map(lambda repo: ('python' if repo[4].lower() == 'python' else ( 'java' if repo[4].lower() == 'java' else 'c'), 1)).reduceByKey(lambda a, b: a+b)
    #computing the number of collected repos with changes pushed during the last 60 seconds for all the repositories
    recent = repos.map(lambda repo: ("recent" if datetime.datetime.now() - datetime.timedelta(seconds=60) < repo[1] <= datetime.datetime.now() else "old", 1)).reduceByKey(lambda a, b: a+b)
    windowedCounts = repos.map(lambda repo: ('python' if repo[4].lower() == 'python' else ( 'java' if repo[4].lower() == 'java' else 'c'), 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 60, 60)
    aggregate_counts = counts.updateStateByKey(aggregate_count)
    aggregate_counts.foreachRDD(process_rdd)
    aggregate_counts.pprint()
    windowedCounts.pprint()
    ssc.start()
    ssc.awaitTermination()