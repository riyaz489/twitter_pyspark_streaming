import socket
import sys
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = '#'
ACCESS_SECRET = '#'
CONSUMER_KEY = '#'
CONSUMER_SECRET = '#'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    #this is twitter live-streaming url so here we get data continuesly and it will never end
    #in case of normal streaming (like getting image from server) it will end when whole object came

    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '='+str(t[1]) for t in query_data])
    #getting data from request streams
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

#fecthoing tweets from above response
def send_tweets_to_spark(http_resp, tcp_connection):
    # getting data line by line from server( this is happening because request stream is true so we can get
    # chunk of data from server one by one until we get all data of response )
    for line in http_resp.iter_lines():
        try:
            #loading data from json as python object
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("-----------------------------------")

            ''' sending data, coming line by line from twitter response stream to spark using tcp socket (tcp socket is
             used for spark streaming ). here spark creates a batches (a batch is in form of rdd internally) on the 
             basis of time interval defined by us.
            for eg if we set time interval 2 sec then it will create a batch for data which came in 2 sec
            and create a another batch for data which came in next 2 sec to spark and then that batches goes to 
            spark engine one by one'''
            ''' Spark Streaming provides a way to consume a continuous data stream'''
            '''We can only have one StreamingContext per JVM.'''
            tcp_connection.send(bytes(((tweet_text)+ '\n'),'utf-8'))

        except Exception as e:
            print("Error %s" %e)


# configuring a spark stream on localhost:9009 using TCP socket
TCP_IP = "localhost"
TCP_PORT = 9019
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for tcp connection... ")
conn, addr = s.accept()
print("this is addr", addr)
print("Connected ... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)

