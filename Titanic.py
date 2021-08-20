from pyspark.sql import *
from pyspark.sql.functions import *
import connection

spark = SparkSession.builder.appName('titanic').getOrCreate()

dfTitanic = spark.read.csv('/home/virtual/Downloads/titanic_train.csv', inferSchema=True, header=True)

dfTitanic.createOrReplaceTempView('Titanic')

#spark.sql("SELECT * FROM Titanic where Cabin is null").show(10)
dfTitanic = dfTitanic.na.fill('N/A', subset=['Cabin', 'Embarked'])
dfTitanic = dfTitanic.withColumn("Name", regexp_replace('Name', '''['"]''', '')) # Replace caracter somente uma coluna

dfCsvTitanic = dfTitanic.toPandas()

connSql = connection.getConnMySQL()
cursorTitanic = connSql.cursor()


for t in dfCsvTitanic.iterrows():
    PassengerID =    t[1][0]
    Survived =       t[1][1]
    Pclass =         t[1][2]
    Name =        str(t[1][3])
    Sex =         str(t[1][4])
    Age =         float(t[1][5])
    SibSp =          t[1][6]    
    Parch =          t[1][7]
    Ticket =      str(t[1][8])
    Fare =        str(t[1][9])
    Cabin =       str(t[1][10])
    Embarked =    str(t[1][11])
    include = """INSERT INTO karina_figueredo.Titanic
    (PassengerID, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
    VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(PassengerID, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
    print(include)
    cursorTitanic.execute(include)
    connSql.commit()