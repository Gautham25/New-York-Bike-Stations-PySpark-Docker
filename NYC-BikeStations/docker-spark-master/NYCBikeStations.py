from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_timestamp, col, udf, when, regexp_replace
from pyspark.sql.types import *
import json, urllib.request

class NYCBikes:
	def __init__(self):
		pass
	def cleanData(self):
		sc = SparkContext("local","count app")
		spark = SparkSession.builder.getOrCreate()
		@udf('string')
		def normalizeAddress_udf(row):
			res = ""
			for word in row.split(' '):
				if word.lower() == "street":
					res+="St. "
				elif word.lower() == "avenue":
					res+="Ave. "
				elif word.lower() == "boulevard":
					res+="BLVD "
				elif word.lower() == "north":
					res+="N "
				elif word.lower() == "south":
					res+="S "
				elif word.lower() == "east":
					res+="E "
				elif word.lower() == "west":
					res+="W "
				else:
					res+=word+" "
			return res.strip()
		response = urllib.request.urlopen("https://feeds.citibikenyc.com/stations/stations.json")
		data = response.read().decode('utf-8')
		data = json.loads(data)
		res = [json.dumps(data['stationBeanList'])]
		rddjson = sc.parallelize(res)
		df = spark.read.option("multiline",True).option("mode","PERMISSIVE").json(rddjson)

		df5=df.select(col("id").alias('df5id'),col("stAddress1").alias('oldstAdd'),normalizeAddress_udf("stAddress1").alias('stAddress1'),normalizeAddress_udf("stAddress1").alias('stationName')).drop('oldstAdd')
		df = df.withColumn('lastCommunicationTime',to_timestamp('lastCommunicationTime',"yyyy-MM-dd hh:mm:ss aa"))
		df3 = df.drop('df2id','stationName','stAddress1')
		df3 = df3.join(df5, df3.id == df5.df5id).drop('df5id')
		for name,type in df3.dtypes:
			if type == 'string':
				df3 = df3.withColumn(name,when(col(name)!='',col(name)).otherwise(None))
		colNames = sorted(df3.columns)
		colNames.remove('id')
		colNames.insert(0,'id')
		df = df3.select(colNames)
		df.coalesce(1).write.option("header", "true").csv("output")
		print(df)
		df.show(10)
		spark.stop()
		sc.stop()
if __name__ == "__main__":
	NYCBikes().cleanData()
