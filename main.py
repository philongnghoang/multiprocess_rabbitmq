import cv2,time
from multiprocessing import Process, Queue, Pipe
from datetime import datetime
from threading import Thread
# import grpc_infer_api
# import socket
import sys
import pickle
# import struct ### new code
# import zmq
# import base64
import argparse
import requests
import json
import pika
import numpy as np
#from collections import deque
import get_stream
import csv
class Streaming(Process):
    
    def __init__(self, name, URL, start_date, stop_date, FPS, **kwargs):
        super(Streaming, self).__init__()
        self.name = name
        self.kwargs = kwargs
        self.URL = URL 
        self.Run = True
        self.start_date = start_date
        self.stop_date  = stop_date
        self.FPS = FPS
        
        credentials = pika.PlainCredentials('admin', 'admin')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
        #connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.URL)
        self.channel.queue_declare(queue='result')
        self.channel.exchange_declare(exchange='check_status', exchange_type='fanout')
        self.channel.queue_bind(exchange='check_status', queue=self.URL)
        self.rabbit_status = {}
 
        #################################################
        

    def run(self):
        cap = cv2.VideoCapture(self.URL)
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
        cnt = 0
        while(self.Run):
            self.Comunicate_master()
            if  cap.isOpened(): 
                ret, frame = cap.read()
                frame_predict = cv2.resize(frame, (640,640))

                if cnt%50==0:
                    cnt = 0
                cnt+=1
                #break
                cv2.imshow('CAM'+self.name,frame_predict)
                if cv2.waitKey(10)=='q':
                    break
        cap.release()
        cv2.destroyAllWindows()
            
          
    def Comunicate_master(self):
        queue_empty = self.channel.queue_declare(queue=self.URL).method.message_count
        if queue_empty!=0:
            method, properties, body = self.channel.basic_get(queue=self.URL, auto_ack=True)
            self.callback_child_process(self.channel, method, properties, body)
    def callback_child_process(self,ch, method, properties, body):
        new_recv = json.loads(body.decode('utf8'))
        print('=============================')
        print('NEW RECIVE: ',new_recv) 
        print(self.channel.queue_declare(queue=self.URL).method.queue)
        if new_recv['opcode'] == 'whoareu':
            self.child_conn.send({'stream_name':self.name})

        elif new_recv['opcode'] == 'timesetup':
            self.child_conn.send({'start': self.start_date, 'stop': self.stop_date})

        elif new_recv['opcode'] == 'settime':
            if self.update_time(   new_recv['start'] , new_recv['stop']  ) :
                self.child_conn.send({'opcode':'settime','state':'done'})
            else:
                self.child_conn.send({'opcode':'settime','state':'error'})
        elif new_recv['opcode'] == 'stop':
            print('Stopping process : ',self.name) 
            self.Run = False 
        elif new_recv['opcode'] == 'check':    
            channel.basic_publish(exchange='', routing_key='send_main', body=json.dumps({"urls":self.URL}))

class Check_alive(Process):
    def __init__(self, **kwargs):
        super(Check_alive, self).__init__()
        credentials = pika.PlainCredentials('admin', 'admin')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
        #connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='check_status', exchange_type='fanout')
    def run(self):
        while True:
            message = json.dumps({"opcode":'check'})
            self.channel.basic_publish(exchange='check_status',routing_key='', body=message)
            print('[x][x][x] Fanout message')
            time.sleep(30)

def Start_newstreaming(name,URL, start_date, stop_date , FPS):
    newstreaming =  Streaming(name,URL, start_date, stop_date, FPS)
    return newstreaming

def callback(ch, method, properties, body):
    print(" [x] Received ")
    message_rabbitmq = json.loads(body.decode('utf8'))

    FPS = 1
    start_date = datetime(2020, 1, 3, 0, 0, 0, 0)
    stop_date  = datetime(2021, 10, 25, 6, 0, 0, 0)

    name_process = message_rabbitmq['urls']
    if message_rabbitmq['opcode'] == 'start' and name_process not in processes:
        print('Start process: ',name_process)
        Stream_= Start_newstreaming(name_process,name_process,start_date,stop_date,FPS)
        processes[name_process]         = Stream_
        processes[name_process].start()

    print('Publish a message to ', name_process,' queue')
    channel.basic_publish(exchange='', routing_key=name_process, body=body)

def recive_mes_process(ch, method, properties, body):
    print(" [x] [x] [x] ")
    status_process = json.loads(body.decode('utf8'))
    if status_process['urls'] not in running_process:
        running_process.append(status_process['urls'])
    print("Running process")
    print(running_process)
def restart_queue():
    list_queue = ['/home/philong/Desktop/FTI_wfh/multipro/mapping.mp4','/home/philong/Desktop/FTI_wfh/multipro/video1.mp4']
    for i in list_queue:
        channel.queue_delete(queue=i)
if __name__ == "__main__":
    
    processes       =   {}
    running_process = []
    credentials = pika.PlainCredentials('admin', 'admin')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
    #connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    #restart_queue()
    channel.queue_declare(queue='mes')
    channel.basic_consume(queue='mes', on_message_callback=callback, auto_ack=True)
    channel.queue_declare(queue='send_main')
    channel.basic_consume(queue='send_main', on_message_callback=recive_mes_process, auto_ack=True)
    #################################################################################
    p = Check_alive()
    p.start()
    print("Running process")
    print(running_process)
    #################################################################################
    
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming() 