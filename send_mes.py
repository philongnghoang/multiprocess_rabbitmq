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
# src_list = [
#             'https://5a2f17f8a961a.streamlock.net:51945/live/epz2-p-sanhthang3treta-39-201.stream/playlist.m3u8',
#             'https://5a3505075bba2.streamlock.net:2952/live/epz2-p-thang3l5-39-216.stream/playlist.m3u8',
#             'https://5a2f17f8a961a.streamlock.net:51945/live/epz2-p-thang1l5-39-217.stream/playlist.m3u8',
#             'https://5a2f17f8a961a.streamlock.net:61950/live/epz2-p-sanhchinhtret-39-200.stream/playlist.m3u8',
#             'https://5a3505075bba2.streamlock.net:2952/live/ftel-pvi-cuaphu.stream/playlist.m3u8',
#             'https://5a3505075bba2.streamlock.net:1952/live/dth_cs_cam21.stream/playlist.m3u8',
#             'https://5a2f17f8a961a.streamlock.net:61950/live/cto_chinhanh_tang_3.stream/playlist.m3u8',
#             'https://5a3505075bba2.streamlock.net:1952/live/behind_office_sisophon.stream/playlist.m3u8',
#             'https://5a2f17f8a961a.streamlock.net:41938/live/ftilau3cam3.stream/playlist.m3u8',
#             'https://5a2f17f8a961a.streamlock.net:61950/live/cto_chinhanh_tang_3.stream/playlist.m3u8',
#              'https://5a2f17f8a961a.streamlock.net:41938/live/ftihn-lobby.stream/playlist.m3u8',
#                     'https://5a2f17f8a961a.streamlock.net:41938/live/ftihn-lobby1.stream/playlist.m3u8',
#                     'rtsp://admin:AI_team123@192.168.1.64:554/Streaming/Channels/101/'
#                 ]


#for i in range(90,120):
message = json.dumps({"urls":src_list[int(sys.argv[1])],"opcode": sys.argv[2]})
queue_name = 'mes'
#queue_name = src_list[int(sys.argv[1])].split('/')[4]
#message = json.dumps({"urls":src_list[i],"opcode": sys.argv[1]})
channel.basic_publish(exchange='', routing_key=queue_name, body=message)

#message = json.dumps({"opcode":'check'})
#channel.basic_publish(exchange='check_status',routing_key='', body=message)

print(" [x] Sent 'Hello World!'")
connection.close()