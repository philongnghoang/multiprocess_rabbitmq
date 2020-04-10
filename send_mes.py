import pika
import json
import get_stream
import sys
message = {}
settime = {}
model = {}

# src_list = get_stream.GetStream()
src_list = [
            'https://5a2f17f8a961a.streamlock.net:41938/live/ftihn-lobby1.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:41938/live/ftihn-lobby.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:51945/live/epz2-p-sanhthang3treta-39-201.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:61950/live/cto_chinhanh_tang_3.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:61950/live/epz2-p-sanhchinhtret-39-200.stream/playlist.m3u8',
            'https://5a3505075bba2.streamlock.net:2952/live/cayqueo_cam03.stream/playlist.m3u8'
                ]

settime['starttime']='7:00'
settime['stoptime'] ='8:00'

model['od']='off'
model['motion']='off'

message['urls']=src_list[int(sys.argv[1])]

message['opcode'] = sys.argv[2]
#message['opcode'] = {'settime':settime}
#message['opcode'] = {'display':'off'}
#message['opcode'] = {'model':model}

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='check_status', exchange_type='fanout')
channel.queue_declare(queue='mes')



#for i in range(0,6):
#message = json.dumps({"urls":src_list[int(sys.argv[1])],"opcode": sys.argv[2]})
queue_name = 'mes'
#queue_name = src_list[int(sys.argv[1])].split('/')[4]
#message = json.dumps({"urls":src_list[i],"opcode": sys.argv[1]})
channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(message))

#message = json.dumps({"opcode":'check'})
#channel.basic_publish(exchange='check_status',routing_key='', body=message)

print(" [x] Sent 'Hello World!'")
connection.close()
