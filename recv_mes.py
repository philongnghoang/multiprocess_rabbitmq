import pika
import json
import get_stream
import sys
import logging

logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s %(name)s - %(levelname)s - %(message)s',level=logging.INFO)
def recive_mes(ch, method, properties, body):
      #print(" [x] [x] [x] ")
      mess_recive = json.loads(body.decode('utf8'))
      
      if 'opcode' in mess_recive.keys():
            logging.info(mess_recive['urls'] +'===='+ mess_recive['opcode'])
      if 'update' in mess_recive.keys():
            logging.info(mess_recive['urls'] +' ==== Update === '+json.dumps(mess_recive['update']))
      if 'settime' in mess_recive.keys():
            logging.info(mess_recive['urls'] +' ==== Settime === '+ json.dumps(mess_recive['settime']))
      if 'result' in mess_recive.keys():
            logging.info(mess_recive['urls'] +' ==== ALARM ==='+ mess_recive['result'])
credentials = pika.PlainCredentials('user', 'user')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
channel = connection.channel()
channel.basic_consume(queue='logging', on_message_callback=recive_mes, auto_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming() 
