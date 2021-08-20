from pyspark.sql import *
from pyspark.sql.functions import *
import connection
import pandas

spark = SparkSession.builder.appName('Babys').getOrCreate()

dfBabys = spark.read.csv('/home/virtual/Downloads/US_Baby_Names_right.csv', header=None)

dfBabys.createOrReplaceTempView('Titanic')
dfBabys.show(5)



connSql = connection.getConnMySQL()
cursorBabys = connSql.cursor()

for row in dfBabys.iterrows():
    id_1 = row[1][0]
    id = row[1][1]
    Name =   str(row[1][2])
    Ano =    row[1][3]
    Gender =    str(row[1][4])
    State =  str(row[1][5])    
    Count_ano =  row[1][6]
    Insert = """INSERT INTO karina_figueredo.Babys
    (id_1, id,Name, Gender, State, Count_ano)
    VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(id_1, id, Name,Ano, Gender, State, Count_ano)
    # print(Insert)
    cursorBabys.execute(Insert)
    connSql.commit()

    