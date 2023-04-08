import json
import requests
import time
import socket
import os
import re
import errno

token = os.getenv("TOKEN")
TCP_IP = '0.0.0.0'
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")
# create a set to store the repo ids and check for duplicates
repo_ids = set()
# set of languages to iterate over
languages = ['Python', 'Java', 'C']


def data_stream(token, conn, repo_ids, languages):
    for lang in languages:
        url = f'https://api.github.com/search/repositories?q=+language:{lang}&sort=updated&order=desc&per_page=50'
        try:
            req = requests.get(url, headers={
                "Authorization": "token" + str(token)
            })
            if req.ok:
                print(
                    f'-----------------------{lang}\tSTATUS CODE:{req.status_code}---------------------------')
                res = req.json()
                if res['items'] is not None:
                    for item in res['items']:
                        if item['id'] in repo_ids:
                            print(f'DUPLICATE: {item["id"]}')
                            continue
                        else:
                            repo_ids.add(item['id'])
                            if item['description'] is not None:
                                desc: str = item['description']
                                strip_desc = re.sub('[^a-zA-Z ]', '', desc)
                                strip_desc = strip_desc.split(' ')
                            else:
                                strip_desc = None

                            data = {
                                "id": item["id"],
                                "pushed_at": item["pushed_at"],
                                "stargazers_count": item["stargazers_count"],
                                "description": strip_desc,
                                "language": item["language"]
                            }
                            conn.send((json.dumps(data) + '\n').encode())
                            print(
                                f'full_name:{item["full_name"]}\tpushed_at:{item["pushed_at"]}\tstargazers_count:{item["stargazers_count"]}\tdescription: {item["description"]}\tlanguage:{item["language"]}'
                            )
            else:
                print(
                    f'-----------------------{lang}\tSTATUS CODE:{req.status_code}---------------------------')
        except IOError as e:
            if e.errno == errno.EPIPE:
                print("EPIPE error")
                pass


while True:
    try:
        data_stream(token, conn, repo_ids, languages)

        time.sleep(15.0)

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        s.shutdown(socket.SHUT_RD)
