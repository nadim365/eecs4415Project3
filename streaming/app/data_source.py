"""
 Data source for the streaming application that reads from the github api information about the most recently pushed repos that use python,
 java, and C. The data is sent to the spar application at an interval of 15 seconds.
 This search API returns a json file that contains a list (items), each element is a dictionary that is comopsed of many key-value pairs.
 the fields full_name, pushed_at, stargazers_count , and the description are processed using the json library.
"""
import os
import re
import requests
import socket
import json
import time
# check for duplicates from the repos collected
languages = ['Python', 'Java', 'C']
url_python = "https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50"
url_java = "https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50"
url_c = "https://api.github.com/search/repositories?q=+language:C&sort=updated&order=desc&per_page=50"

token = os.getenv("TOKEN")
TCP_IP = '0.0.0.0'
TCP_PORT = 9999
CONN = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")
# create a set to store the repo ids and check for duplicates
repo_id = set()


def send_calls(conn, res):
    if res['items'] is not None:
            for item in res['items']:
                if item['id'] in repo_id:
                    print(f'duplicate: {item["id"]} ')
                    continue
                else:
                    repo_id.add(item['id'])
                    if item['description'] is not None:
                        desc: str = item['description']
                        strip_desc = re.sub('[^a-zA-Z ]', '', desc)
                        strip_desc = strip_desc.split(' ')
                    else:
                        strip_desc = None
                    data = {
                        "full_name": item["full_name"],
                        "pushed_at": item["pushed_at"],
                        "stargazers_count": item["stargazers_count"],
                        "description": strip_desc,
                        "language": item["language"]
                    }
                    conn.send((json.dumps(data) + '\n').encode())
                    print(
                        f'full_name:{item["full_name"]}\tpushed_at:{item["pushed_at"]}\tstargazers_count:{item["stargazers_count"]}\tdescription: {item["description"]}\tlanguage:{item["language"]}'
                    )


while True:
    try:
        # get request for python repos and print required fields
        print("-----------------------Python---------------------------")
        res_py = requests.get(
            url_python, headers={
                "Authorization": "token " + str(token)
            }
        ).json()
        send_calls(conn, res_py)

        # get request for Java repos and print required fields
        print("------------------------Java--------------------------")
        res_java = requests.get(
            url_java, headers={
                "Authorization": "token " + str(token)
            }
        ).json()
        send_calls(conn, res_java)

        # create a new request for C repos and print required fields
        print("------------------------C--------------------------")
        res_c = requests.get(
            url_c, headers={
                "Authorization": "token " + str(token)
            }
        ).json()
        send_calls(conn, res_c)

        time.sleep(15)
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        s.shutdown(socket.SHUT_RD)
