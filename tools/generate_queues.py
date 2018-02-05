import pika

parameters = pika.ConnectionParameters(
    host="localhost",
    virtual_host="/",
    credentials=pika.PlainCredentials("guest", "guest")
)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

for i in range(9000):
    channel.queue_declare(queue="channel{}".format(i), durable=True)

connection.close()