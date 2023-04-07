"""
Flask app to receive the data from the spark streaming app and add it to a Redis Datbase and display it on the webpage.
The webapp is listening on port 5000.
The redis database is listening on port 6379.
    1. Display the number of collected repositories for each language since the start of the streaming application in real-time.
        Numbers are updated every 60 seconds.
    2. A real-time line chart that shows the number of collected repositories with changes pushed during the last 60 seconds for each batch interval (60 seconds) for each of the 3 languages. 
        X-axis is the time and the count is on the Y-axis. The chart is updated every 60 seconds.
    3. A real-time bar plot that shows the average number of stars of all the collected repos since the start of the streaming application for each of the
        3 programming languages. The bar plot is updated every 60 seconds.
    4. 3 Lists that contain the top 10 most frequent words in the description of all the collected repos and their occurrences since the start of the streaming applicaiton,
        sorted from most frequent to least frequent in real-time. The lists are updated every 60 seconds.
"""

from itertools import count
import time
from flask import Flask, render_template, request, jsonify
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)


@app.route('/updateCountsData', methods=['POST'])
def update_counts_data():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('counts', json.dumps(data))
    return jsonify({'msg': 'success'})


@app.route('/updateRecentsData', methods=['POST'])
def update_recents_data():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    # push the recents data of the 3 languages to a list in redis for each language
    for language, count in zip(data['Language'], data['Recents']):
        # curr_time = datetime.datetime.now().strftime('%H:%M:%S')
        r.lpush(language, int(count))
        # keep the list size to 10
        if r.llen(language) > 10:
            r.rpop(language)
    return jsonify({'msg': 'success'})


@app.route('/updateStarsData', methods=['POST'])
def update_stars_data():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('stars', json.dumps(data))
    return jsonify({'msg': 'success'})


@app.route('/updateTop10Data', methods=['POST'])
def update_top10_data():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    # push the top 10 words and their occurrences to a hash in redis for each language
    r.delete('top10')
    r.set("top10", json.dumps(data))
    return jsonify({'msg': 'success'})


@app.route('/', methods=['GET'])
def index():
    # process the data from redis, create the charts and display them on the webpage
    r = Redis(host='redis', port=6379)
    counts = r.get('counts')
    stars = r.get('stars')
    recents_python = r.lrange('Python', 0, -1)
    recents_java = r.lrange('Java', 0, -1)
    recents_c = r.lrange('C', 0, -1)
    top10 = r.get('top10')
    try:
        counts = json.loads(counts)
        stars = json.loads(stars)
        recents_python = [int(i) for i in recents_python]
        recents_java = [int(i) for i in recents_java]
        recents_c = [int(i) for i in recents_c]
        top10 = json.loads(top10)
    except TypeError:
        print('Waiting for data from spark streaming app....')

    try:
        py_index = counts['lang'].index('Python')
        py_Counts = counts['count'][py_index]
    except ValueError:
        py_Counts = 0
    try:
        java_index = counts['lang'].index('Java')
        java_Counts = counts['count'][java_index]
    except ValueError:
        java_Counts = 0
    try:
        c_index = counts['lang'].index('C')
        c_Counts = counts['count'][c_index]
    except ValueError:
        c_Counts = 0

    # create the charts
    # stars chart
    stars_lang = stars['Language']
    stars_avg = stars['Average_Stars']
    plt.bar(stars_lang, stars_avg, color=[
            'tab:red', 'tab:green', 'tab:blue'], width=0.5)
    plt.xlabel('Language')
    plt.ylabel('Average Stars')
    plt.title(
        f'Average Stars of Repositories at time {time.strftime("%H:%M:%S")}')
    plt.savefig('/app/webapp/static/images/stars.png')
    plt.clf()
    plt.cla()

    # recents time series chart
    # plot the recents data of the 3 languages
    plt.plot(recents_python, color='tab:blue', label='Python')
    plt.plot(recents_java, color='tab:green', label='Java')
    plt.plot(recents_c, color='tab:orange', label='C')
    plt.xlabel('Time')
    plt.ylabel('Count')
    # display current time and the time of the last 10 minutes in descending order more efficiently and clearly with space between the labels without rotation
    plt.xticks(range(10), [time.strftime("%H:%M:%S", time.localtime(
        time.time() - i * 60)) for i in range(10)], rotation=45)
    plt.legend(loc='upper right')
    plt.title(
        f'Repositories with pushes in last 60s from time {time.strftime("%H:%M:%S")} in last 10 minutes:')
    plt.savefig('/app/webapp/static/images/recents.png')
    plt.clf()
    plt.cla()

    # top 10 words dictionary
    words_counts = {}
    for lang, word, count in zip(top10['Language'], top10['Word'], top10['Freq']):
        if lang not in words_counts:
            words_counts[lang] = {}
        words_counts[lang][word] = count

    return render_template('index.html', url_recents='/static/images/recents.png', url_stars='/static/images/stars.png', py_Counts=py_Counts, java_Counts=java_Counts, c_Counts=c_Counts, stars_avg=stars_avg, stars_lang=stars_lang, words_py=words_counts['Python'], words_java=words_counts['Java'], words_c=words_counts['C'])


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
