import nsq
import tornado.ioloop
import time

def pub_message():
    writer.pub('test', time.strftime('%H:%M:%S').encode('utf-8'), finish_pub)

def finish_pub(conn, data):
    print(data)

writer = nsq.Writer(['47.102.220.1:4150'])
tornado.ioloop.PeriodicCallback(pub_message, 5000).start()
nsq.run()