import sys
from pyspark import SparkConf,SparkContext
 
conf = SparkConf().setMaster("local").setAppName("WeatherYear")
sc = SparkContext(conf = conf)
 
input = sc.textFile("weather_data_set.csv")
 
templocationmapping = input.map(lambda x : (x.split(",")[2],x.split(",")[4]))
maxtemplocation = templocationmapping.reduceByKey(lambda x,y:max(x,y))
mintemplocation = templocationmapping.reduceByKey(lambda x,y:min(x,y))
 
maxtemplocationresults = maxtemplocation.collect();
mintemplocationresults = mintemplocation.collect();
 
print("Maximum temperature location wise")
for results in maxtemplocationresults:
    print(results)
print("Minimum temperature location wise")
for results in maxtemplocationresults:
    print(results)
