# Q&As on the Project Description

## When displaying the result of the description word counts on the webapp dashboard, is it okay if we assume that each repository count towards the result only once since the start of the streaming like in 3iv)?
You can't assume that. You need to check if the repo has been processed before.

## I tried to run docker-compose up in the streaming folder and this is the error I got... Yet the next line is the output of data_source.py:
Please note that the docker-compose.yaml file in the project repo is copied from the Lab7 repo, and it is just a placeholder to show the correct directory structure. You need to write your own docker-compose.yaml for your solution. Please make sure your whole system can be up and running by executing the two commands specified in Requirement 5.
Also, when evaluating your solution, we do not put the value of the environment variable (e.g., PAT) in quotes.

## In the following scenario for requirement 3(iv), Python: ('for',5), ('at',5),... Does the order matter when the count of each word is the same?
The order doesn't matter.

## Does the report need to be single-spaced or double-spaced?
There are no strict formatting requirements, but your report must cover: (1) description of the system architecture; (2) how the services interact with each other; (3) how the Spark application processes the streaming data.

## For the time value on the x-axis for requirement 4 section ii, what time does the starting point of the line-chart have to start at? Is this time supposed to be the exact moment when the batch interval starts, or can we be more lenient and just set the starting time to anything around when the batch interval starts, such as when the flask app receives the results data?
You need to visualize the streaming analysis results of each batch interval. So, the answer is yes.

## Is there any specific formatting we have to follow for Requirement 3 Section v? Do we have to convert the rdd to a pyspark.DataFrame and then print it out using DataFrame.show() (like in Lab 7), or can we just print out key value pairs? Also, are we required to convert the rdd to a pyspark.DataFrame before we send it to the Flask app, or can we use whatever data structure we see fit? 
The output must be able to demonstrate that your service has satisfied all the requirements.
Also, please make sure your output is human-readable and properly labeled so that TAs can easily see your service is running without errors.
There is no specific format on how to print out the output. You can use any method you see fit.

## For project 3, is the repository description word count (requirement 3 (iv)) case sensitive? e.g. A != a or Python != python
Either way is okay.

## For requirement 3 section ii, should the project description say that we have to count the number of repositories pushed during the last 60 seconds by programming language? I'm asking because the web service requirement section that is associated with requirement 3 section ii says that we have to show three lines, one for each programming language, so it wouldn't make sense to not split by programming language in requirement 3 but split by programming language in requirement 4.
For requirement 3(ii), you should compute the number of the collected repositories with changes pushed during the last 60 seconds for each programming language. We actually mentioned it in requirement 4(ii).

## Are we suppose to take only 50 repositories per 15 seconds(per_page=50), or are we taking the entire list of repositories which is around 3 million repositories per 15 seconds?
I don't see the need to retrieve the list of 3 million repositories. Please read the project description again carefully.

## In requirement 3, section iii said: "Compute the number of the collected repositories with changes pushed during the last 60 seconds. Each repository should be counted only once during a batch interval (60 seconds)." What should I compute here? The number of repos I received in the last 60 seconds, or the number of repos that its 'pushed_at' is within the last 60 seconds?
The number of repos that its 'pushed_at' is within the last 60 seconds

## In the instruction a sample endpoint is provided that fetch data of most recently-pushed repositories that use C# language as the primary coding language as follow:
```
'https://api.github.com/search/repositories?q=+language:CSharp&sort=updated&order=desc&per_page=50'
or we can use an endpoint that fetch data from repositories of the 3 programming languages at once. for example:

https://api.github.com/search/repositories?q=+language:Javascript+language:java+language:C&per_page=50

if we use the above endpoint it returns the repositories data that uses either of the Javascript, java or C as their primary language.  I wonder if we should use 3 different endpoints to fetch data for each of the 3 programming languages from github separately or  we can use an endpoint that fetch data from repositories of the 3 programming languages at once?
```
Please use the URL presented in the project description. That is to say, your data source service queries one PL in a request.

## From my understanding of instructions I assume for running the application we need to replace our PAT with dummyvalue in docker-compose.yaml and then using the os module we need to get 'TOKEN' variable and store it in a variable and then use that variable instead of our actual path in front of the Authorization in the following code instead of putting our actual PAT:
```
res = requests.get(url, headers={"Authorization": "token ghp_REPLACE_THIS_DUMMY_VALUE_WITH_YOUR_PAT"})

Is that the case here or I misunderstood it? 
```
Yes, it should be correct.

## I used three separate queries corresponding to each programming language. Would this be alright? I understand this assignment will be marked automatically, and the two different approaches will produce different data. Will this be accounted for in the grading?
It would be fine as long as the data source service can get the required data.

## For requirement 3, sections i and iii say that "Each repository should be counted only once". To clarify, this means that we are not supposed to count repositories if we've already seen them in a previous iteration? 
This is true for requirements 3(i), 3(iii), and 3(iv). For requirement 3(ii), each repository should be counted only once during a batch interval.

## For requirements 3i, iii, and iv, can we assume that once we see a repository, its data will never change (i.e. language, number of stars, description), e.g. if a repository has 1000 stars, then it will always stay at 1000 stars?
For requirements 3(i), 3(iii), and 3(iv), you need to skip over the repository if it has been processed during any batch interval.

## I have a question regarding the PAT(personal access token) generated from Github. in the instructions of the project it is stated that to send http get requests we need PAT and we can send requests for a specific programming language. However, it is also stated that the scripts running in the data source service container need to get PAT from the environment variable TOKEN. Do not hard-code your PAT in your solution. In your submission, you can define a dummy value in docker-compose.yaml, and we will replace it with our PAT when evaluating your solution. and there is a code snippet for getting the environment variable 'TOKEN'. I was a bit confused so my question is in our script for data source is it just enough to make requests as above or do we need to set the TOKEN variable value in the  docker-compose.yaml file as well? I really appreciate it if you shed lights on this issue.

The code snippet in the appendix is just an example to show how to use the requests module to interact with GitHub API. It is not a requirement that you must use the exact same code snippet in your solution.

However, "the data source service should read the GitHub personal access token (PAT) from the environment variable TOKEN" is one of the requirements you need to follow precisely.

Therefore, when developing your solution, you should define your PAT using the environment attribute in docker-compose.yaml, and make your scripts read the PAT from the environment variable "TOKEN". When submitting your solution, you need to replace your PAT in docker-compose.yaml with a dummy value (to avoid token leaks).

## I submitted my project, but the docker_compose.yaml, has my github token as the value for TOKEN instead of dummyvalue.
Again, replacing your PAT with a dummy value is just to avoid token leaks because your PAT is the personal credential for your GitHub account. If your data source service manages to get the PAT from the environment variable, then you should be fine. I recommend that you deactivate your PAT for security reasons.
