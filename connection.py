from pymongo import MongoClient
import MySQLdb
# from cassandra.cluster import Cluster

def getConnMySQL():
    print(__name__)
    connMySQL = MySQLdb.connect(
        host='ffborelli.ddns.net',
        user='root',
        passwd='root',
        db='karina_figueredo'
    )

    return connMySQL

# def getConnCassandra():
#     cluster = Cluster(['ffborelli.ddns.net'])
#     session = cluster.connect()

#     return session

def getConnMongo():
    client = MongoClient('ffborelli.ddns.net', 27017)
    db = client.big_data
    collection = db.karina_figueredo
    
    return collection

