import pika
import json
import get_stream
import sys
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='check_status', exchange_type='fanout')
channel.queue_declare(queue='mes')
queue_name = 'mes'

message = {}
settime = {}
model = {}

src_list = get_stream.GetStream()
src_list = [
            'https://5a2f17f8a961a.streamlock.net:41938/live/ftihn-lobby1.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:41938/live/ftihn-lobby.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:51945/live/epz2-p-sanhthang3treta-39-201.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:61950/live/cto_chinhanh_tang_3.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:61950/live/epz2-p-sanhchinhtret-39-200.stream/playlist.m3u8',
            'https://5a3505075bba2.streamlock.net:2952/live/cayqueo_cam03.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:41938/live/ftilau3cam3.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:41938/live/ftilau3cam7.stream/playlist.m3u8',
            'https://5a2f17f8a961a.streamlock.net:41938/live/fti-lau1-cam09.stream/playlist.m3u8',
            'https://5a3505075bba2.streamlock.net:1952/live/dth_cs_cam21.stream/playlist.m3u8'              
   ]

settime['starttime']='2020:4:20:22:00'
settime['stoptime'] ='2020:4:21:6:00'
settime['repeat'] = 'off'
model['od']='on'
model['motion']='on'

# for i in range(90,116):
#     message['urls']=src_list[i]
#     message['opcode'] = sys.argv[1]
#     #message['settime'] = settime
#     channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(message))
message['urls']=src_list[int(sys.argv[1])]
message['settime'] = settime
message['opcode'] = sys.argv[2]
message['update']={'display':'on'}
channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(message))

print(" [x] Sent 'Hello World!'")
connection.close()
