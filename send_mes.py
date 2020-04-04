import pika
import json
import get_stream
import sys
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#channel.queue_declare(queue='mes')

scr_list = ['/home/philong/Desktop/FTI_wfh/multipro/video1.mp4',
            '/home/philong/Desktop/FTI_wfh/multipro/mapping.mp4'
                ]
message = json.dumps({"urls":scr_list[int(sys.argv[1])],"opcode": sys.argv[2]})

channel.basic_publish(exchange='', routing_key='mes', body=message)
print(" [x] Sent 'Hello World!'")
connection.close()