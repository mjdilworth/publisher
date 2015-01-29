#!/usr/bin/python
import pymongo
import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(
    'localhost'))
channel = connection.channel()

channel.queue_declare(queue='test_queue')

# Connection to Mongo DB
try:
    conn=pymongo.MongoClient()
    print ("Connected successfully!!!")
except (pymongo.errors.ConnectionFailure, e):
    print ("Could not connect to MongoDB: %s" % e )

    
# connect to the students database and the ctec121 collection
db = conn.test.test_data

# create a dictionary to hold student documents

# find all documents
results = db.find()
print('Starting')

iRet = results.count()


# display documents from collection

print ('found ' + str(iRet) + ' records')
iKounter = 0
iTotal = 0
for record in results:
    #if i > 10: break

    id = record['_id']
    dt = record['DT']
    
    Mytime = time.mktime(dt.timetuple())
    #print time.mktime(dt.timetuple())

    #mytiem = json.dumps(dt, default=json_util.default)
    val = record['Value']
    
    myMessage = '{ "Value":"' + str(val) + '" , "ID":"' + str(id) + '", "DT":"' + str(Mytime) + '"}' 

    channel.basic_publish(exchange='', routing_key='test_queue', body=myMessage, 
                      properties=pika.BasicProperties(message_id = str(id), timestamp = time.time()))

    if iKounter < 10 :
        iKounter += 1
        iTotal += 1
    else:
        iKounter = 1
        print ('processed ' + str(iTotal) + ' of a ' + str(iRet))
        iTotal += 1

    time.sleep(1)

# close the connection to MongoDB
conn.close()
connection.close()
print('finished')
