from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('Babys-Process').\
    getOrCreate()

def checkColsWithNullValues( df ):
    for row in df.columns:
        #print(row)
        isNull = df.filter( col(row).isNull() ).count()
        print( row , str(isNull))
    
df = spark.read.\
    csv('/home/virtual/Downloads/US_Baby_Names_right.csv')
#dfPandas = df.toPandas()
#df.show(5)
# dfName = df.groupBy('Name').count()
# dfName.show(10)
df.groupBy('Gender').count().show(2)

