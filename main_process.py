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
    
    def __init__(self, child_conn, name, URL, start_date, stop_date, FPS, **kwargs):
        super(Streaming, self).__init__()
        self.child_conn = child_conn
        self.name = name
        self.kwargs = kwargs
        self.URL = URL 
        self.Run = True
        self.start_date = start_date
        self.stop_date  = stop_date
        self.FPS = FPS
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()
        #self.channel.queue_declare(queue='main',exclusive=True)
        self.channel.queue_declare(queue='result')
        self.rabbit_status = {}
        # context = zmq.Context()
        # self.footage_socket = context.socket(zmq.PUB)
        # # 'tcp://localhost:5555'
        # ip = "127.0.0.1"
        # port = 8881
        # target_address = "tcp://{}:{}".format(ip, port) 
        # print("Publish Video to ", target_address)
        # self.footage_socket.connect(target_address)
        # self.API_ENDPOINT = '192.168.1.45:50'

    def run(self):
        cap = cv2.VideoCapture(self.URL)
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
        self.rabbit_status['urls']= self.URL
        self.rabbit_status['status'] = 'START'
        self.channel.basic_publish(exchange='', routing_key='main', body=json.dumps(self.rabbit_status))
        cnt = 0
        while(self.Run):
            self.Comunicate_master()
            if  cap.isOpened(): 
                ret, frame = cap.read()
                #frame_predict = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                frame_predict = cv2.resize(frame, (640,640))
                # save_name = './save_image/image.jpg'
                # cv2.imwrite(save_name,frame_predict)
               # print('Running ... ', self.name)
                
                if cnt%50==0:
                    self.channel.basic_publish(exchange='', routing_key='main', body=json.dumps({'urls':self.URL}))
                    cnt = 0
                cnt+=1
                #break
                cv2.imshow('CAM'+self.name,frame_predict)
                if cv2.waitKey(10)=='q':
                    break
        cap.release()
        cv2.destroyAllWindows()
        self.child_conn.send({'opcode':'stopped '+self.name}) 
        self.rabbit_status['status'] = 'STOP'
        self.channel.basic_publish(exchange='', routing_key='main', body=json.dumps(self.rabbit_status))
    def Comunicate_master(self):
        if self.child_conn.poll() :
            new_recv = self.child_conn.recv()
            print('NEW RECIVE: ',new_recv) 

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
def Start_newstreaming(name,URL, start_date, stop_date , FPS):
    parent_conn, child_conn = Pipe()
    newstreaming =  Streaming(child_conn,name,URL, start_date, stop_date, FPS)
    return newstreaming, parent_conn


def callback(ch, method, properties, body):
    print(" [x] Received ")
    body = json.loads(body.decode('utf8'))
    print(body['urls'])
    FPS = 1
    start_date = datetime(2020, 1, 3, 0, 0, 0, 0)
    stop_date  = datetime(2021, 10, 25, 6, 0, 0, 0)

    name_process = body['urls']
    if body['status'] == 'START' and name_process not in processes:
        print('Name process: ',name_process)
        Stream_, parent_conn_ = Start_newstreaming(name_process,name_process,start_date,stop_date,FPS)
        processes[name_process]         = Stream_
        pipe_parent[name_process]       = parent_conn_
        processes[name_process].start()

    elif body['status'] == 'STOP' and name_process in processes :  
        pipe_parent[name_process].send({'opcode':'stop'})
    else:
        print('ALREADY EXISTS',name_process,'!!!')
        

def recive_mes_process(ch, method, properties, body):
    print(" [x] [x] [x] ")
    status_process = json.loads(body.decode('utf8'))
    print(status_process)
    #print('STATUS IN PROCESS',status_process['status'])
    #if status_process['status']=='STOP':
    #    del processes[status_process['urls']]
    #    del pipe_parent[status_process['urls']]
    #print("ALL PROCESS",processes)
    #print("ALL PIPE",pipe_parent)
    #print(pipe_parent[status_process['urls']])
if __name__ == "__main__":
    processes       =   {}
    pipe_parent     =   {}
    credentials = pika.PlainCredentials('admin', 'admin')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
    #connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    channel.queue_declare(queue='mes')
    channel.basic_consume(queue='mes', on_message_callback=callback, auto_ack=True)
    channel.queue_declare(queue='main')
    channel.basic_consume(queue='main', on_message_callback=recive_mes_process, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming() 