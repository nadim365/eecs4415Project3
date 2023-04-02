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
import datetime
import sys
import requests
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession


def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def aggregate_avg(new_values, total_avg):
    return (float(sum(new_values) + (total_avg or 0))) / 2.0


def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)


def process_rdd_avg(time, rdd):
    print('---------------- AVG STARS collected since start at time: %s ----------------' % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda repo: Row(
            Language=repo[0], Average_Stars=repo[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("stars")
        results_df.show()
        # new_results_df = sql_context.sql('select Language, AVG(Average_Stars) from stars group by Language')
        # new_results_df.show()
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]


def process_rdd_sum(time, rdd):
    print("---------------- AGG COUNT of repos collected since start at time: %s ----------------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda repo: Row(lang=repo[0], count=repo[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("counts")
        results_df.show()
        # new_results_df = sql_context.sql("select lang, count from counts")
        # new_results_df.show()
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]


def process_rdd_time(time, rdd):
    print('---------------- AGE OF REPOS for current batch(60s) at: %s ----------------' % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda repo: Row(Age=repo[0], Count=repo[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("Age")
        results_df.show()
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
    repos = data.map(lambda repo: json.loads(repo))

    # requirement 1 : total number of collected repos since start for each language (Python, Java, C)
    counts = repos.map(
        lambda repo: (
            repo['language'], 1
        )
    ).reduceByKey(lambda x, y: x + y).updateStateByKey(aggregate_count)

    # requirement 2 : number of collected repos with changes pushed during the last 60 seconds for all repos
    age = repos.map(lambda repo: ("Old" if (datetime.datetime.utcnow() - datetime.datetime.strptime(
        repo['pushed_at'], "%Y-%m-%dT%H:%M:%SZ")).total_seconds() > 60 else "Recent", 1)).reduceByKey(lambda a, b: a+b)

    # requirement 3 : average number of stars of all the collected repos since the start for each language
    stars = repos.map(lambda repo: (repo['language'], int(repo['stargazers_count']))).reduceByKey(
        lambda x, y: x + y).updateStateByKey(aggregate_avg)

    # requirement 4 : top 10 most frequent words in the desc of all the collected repos since the start of the streaming app for each language
    # words = repos.map(lambda repo: (repo['language'], repo['description'].split())).flatMapValues(lambda x: x).map(lambda x: (x[0], x[1].lower())).filter(lambda x: x[1].isalpha()).map(lambda x: (x[0], (x[1], 1))).reduceByKey(lambda x, y: (x[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0], x[1][1])).map(lambda x: (x[0], (x[1], x[2]))).groupByKey().mapValues(lambda x: sorted(x, key=lambda y: y[1], reverse=True)).mapValues(lambda x: x[:10]).flatMapValues(lambda x: x).map(lambda x: (x[0], x[1][0], x[1][1]))
    #words = repos.filter(lambda repo: repo['description'] is not None).map(
    #    lambda repo: (repo['language'], repo['description'])
    #).flatMapValues(lambda repo: repo)
    # printing the analysis results to the console and sending the results to the dashboard
    counts.foreachRDD(process_rdd_sum)
    stars.foreachRDD(process_rdd_avg)
    age.pprint()
    age.foreachRDD(process_rdd_time)
    #words.pprint()
    ssc.start()
    ssc.awaitTermination()
