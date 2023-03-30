"""
 Data source for the streaming application that reads from the github api information about the most recently pushed repos that use python,
 java, and C. The data is sent to the spar application at an interval of 15 seconds.
 This search API returns a json file that contains a list (items), each element is a dictionary that is comopsed of many key-value pairs.
 the fields full_name, pushed_at, stargazers_count , and the description are processed using the json library.
"""

import requests
import time

url_python = "https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50"
url_java = "https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50"
url_c = "https://api.github.com/search/repositories?q=+language:C&sort=updated&order=desc&per_page=50"


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
            print(
                f'{item["full_name"]},{item["pushed_at"]},{item["stargazers_count"]},{item["description"]},{item["language"]}'
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
            print(
                f'{item["full_name"]},{item["pushed_at"]},{item["stargazers_count"]},{item["description"]},{item["language"]}'
            )
        time.sleep(15)

        # create a new request for C repos and print required fields
        res_c = requests.get(
            url_c, headers={
                'Authorizaton': 'token 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'}
        ).json()
        print("-------------------------C---------------------------")
        for item in res_c['items']:
            print(
                f'{item["full_name"]},{item["pushed_at"]},{item["stargazers_count"]},{item["description"]},{item["language"]}'
            )
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
