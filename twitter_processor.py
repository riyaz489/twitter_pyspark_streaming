from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests

# there are also window operations in pyspark streaming
# here we can work on batches which came inside given window time
# for example Reduce last 30 seconds of data, every 10 seconds
# windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)


# to perform operations on a rdd with rdds of given stream we use transform operations
# to join to streams we use join() and to join a stream with single rdd we use join() inside transform()

# creating spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# creating spark context with above config
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the streaming context from the above spark with interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
# this checkpoint is used to save generated rdds into reliable storage(this one is called dataCheckPointing )
# (another way is to metadata-checkPointing where we store
# stream Configuration,DStream operations and Incomplete batches)
# also when we have to operate on different batches of same stream then we will use data checkPointing and this
# operation is called stateful transformations
# checkpoint_TwitterApp is checkPointing directory
ssc.checkpoint("checkpoint_TwitterApp")
# read data form port 9009 (basically creating discretized stream (DStream) to read continues data)
# Internally, a DStream is represented as a sequence of RDDs.
# Any operation applied on a DStream translates to operations on the underlying RDDs.
# so this spark stream create batches in every 2 sec for data, which came through our tcp socket at localhost 9019 port
dataStream = ssc.socketTextStream('localhost', 9019)

# split each tweet into words
# creating a new DStream of words
words = dataStream.flatMap(lambda line: line.split(" "))
# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))


# creating a function for updateStateByKey() function
# here new_value is value coming from new batch and running_total_sum is a total_sum which came from previous batch
# result (so basically running_total_sum is state here
# here runnning_total_sum is state which is None for first batch (state is kind of object which is same for all
# the batches)
# the value returned by this function will be updated to state(running_total_sum) of particular key
def aggregate_tags_count(new_values, running_total_sum):
    # if total_sum is None then 'or' will return 0
    return sum(new_values) + (running_total_sum or 0)


# adding the count of each hashtag to its last count
# updateStateByKey function will take a function as argument and modify stream according to that
# in the argument function of updateStateByKey we get two arguments first one is current batch and second one is state(a
# object which is same to all batches)
# it will create a new state for every key in given stream (i.e each unique key has their own state (running_total))
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# so tags_total have data like  (tagName,count) where tagName is key and count is value
# and always count value is updated for each tagName
# so for every batch it will check its keys and update the tag count


# creating spark sql context
def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("---------%s-------------" %str(time))
    try:
        # get spark sql singleton context from the current context
        sql_context = get_sql_context_instance(rdd.context)
        # convert rdd to row rdd
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        # creating a df from the row rdd
        hashtags_df = sql_context.createDataFrame(row_rdd)
        # reginstering the df as table
        hashtags_df.registerTempTable('hashtags')
        # get the top 10 hashtags from the table using sql as df and print them
        hashtag_counts_df = sql_context.sql('select hashtag, hashtag_count from hashtags order by hashtag_count'
                                            ' desc limit 10')
        hashtag_counts_df.show()
        # call this method to prepare top 10 hashtags DF and send them
        send_df_to_dashboard(hashtag_counts_df)
    except Exception as e:

        print("Error: %s" %e)

def  send_df_to_dashboard(df):
    # extract the hashtags from the dataframe and convert them into array
    top_tags = [str(t.hashtag) for t in df.select('hashtag').collect()]
    # extract the count from df and convert the into array
    tags_count = [p.hashtag_count for p in df.select('hashtag_count').collect()]
    # initialize and send the data through REST API to show it to dashboard
    url = 'http://localhost:5001/updateData'
    reqeust_data = {'label':str(top_tags), 'data':str(tags_count)}
    response =requests.post(url, data=reqeust_data)




# do processing for each RDD generated in each interval
# the function we pass inside foreach will get two arguments batch interval time and current batch as rdd
tags_totals.foreachRDD(process_rdd)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

