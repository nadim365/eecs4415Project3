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
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession, Window
from pyspark.sql.functions import col, row_number
from operator import add


def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def aggregate_avg(new_values, total_avg):
    return (float(sum(new_values) + (total_avg or 0))) / 2.0


def aggregate_top10(new_values, prev_top10):
    count = 0
    counts = [repo[1] for repo in new_values]
    word = new_values[0]
    if prev_top10:
        count = prev_top10[1]
    else:
        new_word = word
    return new_word, sum(counts) + count


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
        send_df_to_dashboard(results_df)
        # new_results_df = sql_context.sql('select Language, AVG(Average_Stars) from stars group by Language')
        # new_results_df.show()
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def process_rdd_sum(time, rdd):
    print("---------------- AGG COUNT of repos collected since start at time: %s ----------------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda repo: Row(lang=repo[0], count=repo[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("counts")
        results_df.show()
        send_df_to_dashboard(results_df)
        # new_results_df = sql_context.sql("select lang, count from counts")
        # new_results_df.show()
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def process_rdd_time(time, rdd):
    print('----------------REPOS with PUSHES IN LAST 60S for current batch(60s) at: %s ----------------' % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda repo: Row(Language=repo[0], Recents=repo[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("Age")
        results_df.show()
        send_df_to_dashboard(results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def process_rdd_freq(time, rdd):
    print('---------------- TOP 10 WORDS in DESCRIPTIONS of REPOS collected since start at time: %s ----------------' % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda repo: Row(
            Language=repo[0][0], Word=repo[0][1], Freq=repo[1]))
        results_df = sql_context.createDataFrame(
            row_rdd, ['Language', 'Word', 'Freq'])
        results_df.createOrReplaceTempView('Freq')
        # creating a window function to group the words by language and order them by their frequency
        window_freq = Window.partitionBy(
            "Language").orderBy(col("Freq").desc())
        # applying the window to the dataframe by adding a column with ranks for each word
        # and filtering the top 10 words for each language
        # and dropping the rank column at the end
        results_df.withColumn('rank', row_number().over(window_freq)).filter(
            col('rank') <= 10).drop("rank").show()
        send_df_to_dashboard(results_df)
    except ValueError:
        print(f'Waiting for data...')
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


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
    # input for map function is repo from data stream
    # output of map function is (language, 1)
    # input for reduceByKey is (language, 1)
    # output of reduceByKey is (language, sum of 1)
    # using updateStateByKey to keep track of the total count of repos for each language since start
    counts = repos.map(lambda repo: (repo['language'], 1)) \
        .reduceByKey(add) \
        .updateStateByKey(aggregate_count)

    # requirement 2 : number of collected repos with changes pushed during the last 60 seconds for all repos of each language
    # input for map function is repo from data stream
    # output of map function is (language, 1 if pushed_at is within last 60s else 0)
    # input for reduceByKey is (language, 1 if pushed_at is within last 60s else 0)
    # output of reduceByKey is (language, sum of 1 if pushed_at is within last 60s else 0)
    age = repos.map(lambda repo: (repo['language'], 1 if (datetime.datetime.utcnow(
    ) - datetime.datetime.strptime(repo['pushed_at'], "%Y-%m-%dT%H:%M:%SZ")).total_seconds() <= 60 else 0)).reduceByKey(add)

    # requirement 3 : average number of stars of all the collected repos since the start for each language
    # input for the map function is repo from data stream
    # output of map function is (language, stars)
    # input for reduceByKey is (language, stars)
    # output of reduceByKey is (language, sum of stars)
    # input for updateStateByKey is (language, sum of stars)
    # output of updateStateByKey is (language, average of stars as float)
    stars = repos.map(lambda repo: (repo['language'], int(repo['stargazers_count']))) \
        .reduceByKey(lambda x, y: x + y) \
        .updateStateByKey(aggregate_avg)

    # requirement 4 : top 10 most frequent words in the description of all the collected repos since the start of the streaming app for each language
    # flatMapValues output: (language, word)
    # map output: ((language, word), 1)
    # reduceByKey output: ((language, word), count)
    # input for updateStateByKey is ((language, word), count)
    # want the top 10 of each language
    words = repos.filter(lambda repo: repo['description'] is not None) \
        .map(lambda repo: (repo['language'], repo['description'])) \
        .flatMapValues(lambda repo: repo) \
        .map(lambda repo: ((repo[0], repo[1]), 1)) \
        .reduceByKey(add) \
        .updateStateByKey(aggregate_count)

    # printing the analysis results to the console and sending the results to the dashboard
    counts.foreachRDD(process_rdd_sum)
    stars.foreachRDD(process_rdd_avg)
    age.foreachRDD(process_rdd_time)
    words.foreachRDD(process_rdd_freq)
    ssc.start()
    ssc.awaitTermination()
