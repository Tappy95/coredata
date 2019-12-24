import nsq

def handler(message):
    print(message.body)
    return True

r = nsq.Reader(message_handler=handler,
        nsqd_tcp_addresses=['47.102.220.1:4150'],
        topic='test', channel='test', lookupd_poll_interval=15)
nsq.run()