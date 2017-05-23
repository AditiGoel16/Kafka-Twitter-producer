from __future__ import print_function
import os

import pandas as pd
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
#   Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
import time
from pyspark.sql import SQLContext,Row
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']
def mapCompany(word,searchTerms):
    for search in searchTerms:
      if search.lower() in word.lower():
        return search.lower()
    return ''.join(e for e in word if e.isalnum())
def filterCompany(word,searchTerms):
    for search in searchTerms:
      if search in word.lower():
        return True
    return False
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SQLContext
        sqlContext = getSqlContextInstance(rdd.context)

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda (w,c): Row(count=c,company=w))
        wordsDataFrame = sqlContext.createDataFrame(rowRdd)
        wordsDataFrame.show()
        #wordsDataFrameWithTime = wordsDataFrame.withColumn("time",time.strftime('%d%m%y %I%M%p'))
        #wordsDataFrameWithTime.show()
        wordsDataFrame.toPandas().to_csv('../output/'+str(time.strftime('%y-%m-%dT%H:%M:%SZ'))+'.csv')
    except:
        pass
nasdaq100 = pd.read_csv('nasdaq100.csv')
searchTerms = nasdaq100['Search']
searchList = []
sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 60)
sqlContext = SQLContext(sc)
kafkaStream = KafkaUtils.createStream(ssc, '34.201.10.83:2181', 'spark-streaming', {'twitterstream':1})

parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

tweets = parsed.map(lambda tweet: tweet['text'])
words = tweets.flatMap(lambda line: line.split(' '))
companyMap = words.map(lambda word: mapCompany(word,searchTerms))
#companyMap.pprint()
companyCounts = companyMap.filter(lambda word: filterCompany(word,searchTerms))
#companyCounts.pprint()
wordCounts = companyCounts.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
#wordCounts.pprint()
wordCounts.foreachRDD(process)

#countsDf = wordCounts.toDf(['company','counts'])
#wordCounts.saveAsTextFile('file:///Users/Nikhil/Documents/Tweety/output/'+time.strftime('%d%m%y %I%M%p'))
ssc.start()
ssc.awaitTermination()
