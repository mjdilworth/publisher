# publisher
publisher in python

publisher_test.py: a python script to read all records form the mongodb test_data collection. It then loops through the returned collection and every second it will put one of these onto a rabbitMQ queue called test_queue.

uses a simple time.sleep(1) call

