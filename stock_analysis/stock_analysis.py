import pymongo_spark
import calendar, time, math
from pyspark import SparkContext
import sys

pymongo_spark.activate()

sc = SparkContext("local", "Stock analysis")

dateFormatString='%Y-%m-%d %H:%M'

minBarRawRDD = sc.mongoRDD('mongodb://10.1.15.169:27017/marketdata.minbars')
groupedBars = minBarRawRDD.sortBy(lambda doc: str(doc["Timestamp"])).groupBy(lambda doc :
        (doc["Symbol"], math.floor(calendar.timegm(time.strptime(doc["Timestamp"],
            dateFormatString)) / (5*60))))

# looking at each group and pulling out OHLC
def ohlc(grouping):
    low = sys.maxint
    high = -sys.maxint
    i = 0
    groupKey = grouping[0]
    group = grouping[1]

    for doc in group:
        #  take time and open from first bar
        if i == 0:
            openTime = doc["Timestamp"]
            openPrice = doc["Open"]

        # assign min and max from the bar if appropriate
        if doc["Low"] < low:
            low = doc["Low"]
        if doc["High"] > high:
            high = doc["High"]
        i = i + 1
        # take close of last bar
        if i == len(group):
            closePrice = doc["Close"]
            outputDoc = {"Symbol": groupKey[0],
                         "Timestamp": openTime,
                         "Open" : openPrice,
                         "High" : high,
                         "Low" : low,
                         "Close" : closePrice}

    return (None, outputDoc)

resultRDD = groupedBars.map(ohlc)
#resultRDD.saveToBSON('minBar_result')
resultRDD.saveToMongoDB('mongodb://10.1.15.169:27017/marketdata.minbars_result')

