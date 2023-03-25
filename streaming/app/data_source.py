"""
 Data source for the streaming application that reads from the github api information about the most recently pushed repos that use python,
 java, and C. The data is sent to the spar application at an interval of 15 seconds.
 This search API returns a json file that contains a list (items), each element is a dictionary that is comopsed of many key-value pairs.
 the fields full_name, pushed_at, stargazers_count , and the description are processed using the json library.
"""


import os
import sys
from wsgiref import headers
import requests
import socket
import json
import time

url_python = "https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50"
url_java = "https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50"
url_c = "https://api.github.com/search/repositories?q=+language:C&sort=updated&order=desc&per_page=50"

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
while True:
    try:
        # get request for python repos and print required fields
        res_py = requests.get(
            url_python, headers={
                "Authorization": "token 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"
            }
        ).json()
        print("-----------------------Python---------------------------")
        for item in res_py['items']:
            data = f'{item["full_name"]}\t{item["pushed_at"]}\t{item["stargazers_count"]}\t{item["description"]}'
            conn.send(data.encode())
            print(
                f'full_name: {item["full_name"]}\tpushed_at: {item["pushed_at"]}\tstargazers_count: {item["stargazers_count"]}\tdescription: {item["description"]}'
            )
        time.sleep(15)

        # get request for Java repos and print required fields
        res_java = requests.get(
            url_java, headers={
                'Authorization': 'token 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'
            }
        ).json()
        print("------------------------Java--------------------------")
        for item in res_java['items']:
            data = f'{item["full_name"]}\t{item["pushed_at"]}\t{item["stargazers_count"]}\t{item["description"]}'
            conn.send(data.encode())
            print(
                f'full_name: {item["full_name"]}\tpushed_at: {item["pushed_at"]}\tstargazers_count: {item["stargazers_count"]}\tdescription: {item["description"]}'
            )
        time.sleep(15)

        # create a new request for C repos and print required fields
        res_c = requests.get(
            url_c, headers={
                'Authorizaton': 'token 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'}
        ).json()
        print("-------------------------C---------------------------")
        for item in res_c['items']:
            data = f'{item["full_name"]}\t{item["pushed_at"]}\t{item["stargazers_count"]}\t{item["description"]}'
            conn.send(data.encode())
            print(
                f'full_name: {item["full_name"]}\tpushed_at: {item["pushed_at"]}\tstargazers_count: {item["stargazers_count"]}\tdescription: {item["description"]}'
            )
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        s.shutdown(socket.SHUT_RD)
