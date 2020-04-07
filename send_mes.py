import pika
import json
import get_stream
import sys
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='check_status', exchange_type='fanout')
channel.queue_declare(queue='mes')
src_list = get_stream.GetStream()
# scr_list = ['/home/philong/Desktop/motion_detect/backend_demo/video0.mp4',
#             '/home/philong/Desktop/motion_detect/backend_demo/video1.mp4',
#             '/home/philong/Desktop/motion_detect/backend_demo/video2.mp4',
#             '/home/philong/Desktop/motion_detect/backend_demo/video3.mp4',
#             '/home/philong/Desktop/motion_detect/backend_demo/video4.mp4',
#             '/home/philong/Desktop/motion_detect/backend_demo/video5.mp4',
#             '/home/philong/Desktop/motion_detect/backend_demo/video6.mp4'
#                 ]


for i in range(100,120):
    #message = json.dumps({"urls":src_list[int(sys.argv[1])],"opcode": sys.argv[2]})
    queue_name = 'mes'
    #queue_name = src_list[int(sys.argv[1])].split('/')[4]
    message = json.dumps({"urls":src_list[i],"opcode": sys.argv[1]})
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)

#message = json.dumps({"opcode":'check'})
#channel.basic_publish(exchange='check_status',routing_key='', body=message)

print(" [x] Sent 'Hello World!'")
connection.close()