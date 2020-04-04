import pika
import json
import get_stream
import sys
def recive_mes(ch, method, properties, body):
   print(" [x] [x] [x] ")
   body = json.loads(body.decode('utf8'))
   print(body)
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
#connection = pika.BlockingConnection(
#    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='mes')
channel.basic_consume(queue='mes', on_message_callback=recive_mes, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming() 
