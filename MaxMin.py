import sys
from pyspark import SparkConf,SparkContext
 
conf = SparkConf().setMaster("local").setAppName("WeatherYear")
sc = SparkContext(conf = conf)
 
input = sc.textFile("weather_data_set.csv")
 
tempyearmapping = input.map(lambda x : (x.split(",")[1],x.split(",")[4]))
maxtempyear = tempyearmapping.reduceByKey(lambda x,y:max(x,y))
mintempyear = tempyearmapping.reduceByKey(lambda x,y:min(x,y))
 
maxtempyearresults = maxtempyear.collect();
mintempyearresults = mintempyear.collect();
 
print("Maximum temperature yearwise")
for results in maxtempyearresults:
    print(results)
print("Minimum temperature yearwise")
for results in mintempyearresults:
    print(results)
 


