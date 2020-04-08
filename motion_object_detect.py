import cv2,time
from multiprocessing import Process, Queue, Pipe
from datetime import datetime
from threading import Thread
import grpc_infer_api
import socket
import sys
import pickle
import struct ### new code
import zmq
import base64
import argparse
import requests
import json
import pika
import numpy as np
from collections import deque
class Load_balencer():
    def __init__(self,server_list,test_image):
        self.server_list = server_list.copy()
        self.test_image = test_image
        self.online_server_list = server_list.copy()
        self.time_exec = {}
        self.run_var_server = 0
        self.CheckServer()                       
        print("Online Servers are: ", self.online_server_list)
        time.sleep(2)   
        thread = Thread(target=self.CheckServerPerTime)
        thread.start()

    def CheckServer(self):
        print("========================= ONLINE SERVER CHECKING ==================================")
        for idx,server in enumerate(self.server_list):
            print("Start testing server: ",server)
            start_time = time.time()
            OD_model =  grpc_infer_api.GRPC_inference_OD(hostport=server, model_name='OD', signature_name='serving_default', image_shape=(640 , 640),\
		                                  graph_input_name='input', graph_score_name='score', graph_numbox_name='num', graph_classes_name='classes',\
		                                  graph_boxes_name='boxes')
            try:
                status, num_box, classes, score, boxes = OD_model.do_inference_sync(self.test_image,1)
            except: 
                status = -1
            stop_time = time.time()
            print(num_box, classes, score, boxes)
            print("Statusssssss: ", status)
            if status is None:
                try:
                    self.time_exec[server] = (stop_time-start_time + self.time_exec[server])/2
                except:
                    self.time_exec[server] = stop_time-start_time
                print('Tested '+server+': UP !')
                if server not in self.online_server_list:
                    self.online_server_list.append(server)
            elif server in self.online_server_list:
                self.online_server_list.remove(server)
                print('Tested '+server+': DOWN !')
            else:
                print('Tested '+server+': DOWN !')
            print("----------------------------------------------------------------------------")

    def CheckServerPerTime(self):
        while True:
            try:
                self.CheckServer()
            except:
                print("Check server Failed !")
            time.sleep(10)

    def RemoveServer(self,server):
        try:
            self.online_server_list.remove(server)
            print("Removed server: ",server)
        except:
            print("Server dosen't exist !")

    def TakeServer(self):  
        self.run_var_server = (self.run_var_server+1)%len(self.online_server_list)
        server = self.online_server_list[self.run_var_server]
        return server



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
        context = zmq.Context()
        self.footage_socket = context.socket(zmq.PUB)
        # 'tcp://localhost:5555'
        ip = "127.0.0.1"
        port = 8881
        target_address = "tcp://{}:{}".format(ip, port) 
        print("Publish Video to ", target_address)
        self.footage_socket.connect(target_address)
        self.API_ENDPOINT = '192.168.1.45:50'
        self.nvof = cv2.cuda_FarnebackOpticalFlow.create(5,0.5,False,4,20,5,1.1,0)

    def draw_hsv(self,flow):
        (h, w) = flow.shape[:2]
        (fx, fy) = (flow[:, :, 0], flow[:, :, 1])
        ang = np.arctan2(fy, fx) + np.pi
        v = np.sqrt(fx * fx + fy * fy)
        hsv = np.zeros((h, w, 3), np.uint8)
        hsv[..., 0] = ang * (180 / np.pi / 2)
        hsv[..., 1] = 0xFF
        hsv[..., 2] = np.minimum(v * 4, 0xFF)
        bgr = cv2.cvtColor(hsv, cv2.COLOR_HSV2BGR)
        # cv2.imshow('hsv', bgr) #brg dark
        return bgr

    def movingcheck(self,prevI,curI,n,drawing_flag=False):
        prevgray = cv2.cvtColor(prevI, cv2.COLOR_BGR2GRAY)
        vis = curI.copy()
        gray = cv2.cvtColor(curI, cv2.COLOR_BGR2GRAY)

        

        f1=cv2.cuda_GpuMat(prevgray)
        f2=cv2.cuda_GpuMat(gray)

        f1=f1.convertTo(cv2.CV_32FC1,f1)
        f2=f2.convertTo(cv2.CV_32FC1,f2)
        
        flowUpSampled=self.nvof.calc(f1,f2,None)
        flow=flowUpSampled.download(None)

        print('done')
        ###############
        gray_hsv=self.draw_hsv(flow)
        gray1=cv2.cvtColor(gray_hsv,cv2.COLOR_BGR2GRAY)

        thresh = cv2.threshold(gray1, 25, 0xFF,
                                cv2.THRESH_BINARY)[1]
        # np.ones((5,5), np.uint8)
        thresh = cv2.erode(thresh,None, iterations=5) 
        thresh = cv2.dilate(thresh, None, iterations=25)

        cnts, hierarchy = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE) #gray2, cnts, hierarchy
        
        if (drawing_flag==True):
            for c in cnts:
                (x, y, w, h) = cv2.boundingRect(c)
             
                if w > n and h > n :
                    cv2.rectangle(vis, (x, y), (x + w, y + h), (0,0xFF,0), 4)
                    # cv2.putText(vis,str(time.time()),(x, y),cv2.FONT_HERSHEY_SIMPLEX,1,(0, 0, 0xFF),1)
            #cv2.imshow('vis1', thresh) # video with bbox
            #cv2.imshow('vis', vis) # video with bbox

        if (len(cnts)>n):
            return vis,True 
        else:
            return vis,False

    def run(self):
        
        cap = cv2.VideoCapture(self.URL)
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
        OD_model =  grpc_infer_api.GRPC_inference_OD(hostport=self.API_ENDPOINT, model_name='OD', signature_name='serving_default', image_shape=(640 , 640),\
		                                  graph_input_name='input', graph_score_name='score', graph_numbox_name='num', graph_classes_name='classes',\
		                                  graph_boxes_name='boxes')

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='hello')

        #channel.exchange_declare(exchange='image_exch')
        (ret, frame) = cap.read()
        prev_frame = frame
        while(self.Run and self.comparing_date()):

            self.Comunicate_master()
            if  cap.isOpened(): 
                ret, frame = cap.read()
                ######################
                prev_frame =cv2.resize(prev_frame, (0,0), fx=1, fy=1)
                frame =cv2.resize(frame, (0,0), fx=1, fy=1)
                frame,ans=self.movingcheck(prev_frame,frame,40,True)
                ###################### 
                frame_predict = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                frame_predict = cv2.resize(frame_predict, (640,640))
                try:
                    status, num_box, classes, score, boxes = OD_model.do_inference_sync(frame_predict,1)
                except: 
                    status = -1


                while status != None:
                    time.sleep(0.05)
                    try:
                        status, num_box, classes, score, boxes = OD_model.do_inference_sync(frame_predict,1)
                    except: 
                        status = -1
                

                person_check = False
                if num_box != 0:
                    bnbbox = boxes
                    for index in range(num_box):
                        if score[index] > 0.45 and classes[index] == 1 :
                            person_check = True
                            h, w, _ = frame.shape
                            x1, y1, x2, y2 = int(bnbbox[index][1] * w), int(bnbbox[index][0] * h), int(bnbbox[index][3] * w), int(bnbbox[index][2] * h)
                            #cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 1)
                            cv2.circle(frame, (int((x1+x2)/2),int((y1+y2)/2)),int(max(x2-x1,y2-y1)/2), (0,0,255), thickness=3, lineType=8, shift=0)
                            #cv2.line(frame, start_point, end_point,(0,0,255),8) 
                            if person_check:
                                encoded, buffer_ = cv2.imencode('.jpg', frame[y1:y2,x1:x2],encode_param)
                                data = base64.b64encode(buffer_)
                                data = data.decode("utf-8")
                                channel.basic_publish(exchange='', routing_key='hello', body=json.dumps({"image":data,"id_cam":self.name,"status":"HAVE_PERSON!!!!"}).encode('utf8'))
                        else:
                            channel.basic_publish(exchange='', routing_key='hello', body=json.dumps({"id_cam":self.name,"status":"NO_PERSON"}).encode('utf8'))
                cv2.imshow('CAM'+self.name,frame)
                if cv2.waitKey(1)=='q':
                    break
            
        cap.release()
        cv2.destroyAllWindows()
        self.child_conn.send({'opcode':'stopped '+self.name}) 


    def Comunicate_master(self):
        if self.child_conn.poll() :
            new_recv = self.child_conn.recv()
            print(new_recv) 

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

    def comparing_date(self):
        nowdate = datetime.now().timestamp()
        #   print(nowdate)
        if nowdate <= self.stop_date.timestamp() and nowdate >= self.start_date.timestamp():
            return True
        else:
            return False

    def update_time(self,start,stop):
        nowdate = datetime.now().timestamp()
        if nowdate <= stop.timestamp() and nowdate >= start.timestamp():
            self.start_date = start
            self.stop_date  = stop
            return True
        else:
            return False


def Start_newstreaming(name,URL, start_date, stop_date , FPS):
    parent_conn, child_conn = Pipe()
    newstreaming =  Streaming(child_conn,name,URL, start_date, stop_date, FPS)
    return newstreaming, parent_conn

#LB = Load_balencer(['192.168.1.45:50'],cv2.imread('image.jpg'))

if __name__ == "__main__":
    
    '''
    scr_list        =   ['http://118.69.190.173:1935/live/v1-hn07-nxq-c4-cam01-1.stream/playlist.m3u8'
                        #'http://118.69.190.173:1935/live/v1-hn08-qoi-cam3.stream/playlist.m3u8',
                        #'http://118.69.190.173:1935/live/v7-vlg-ptb-c4-cam01-1.stream/playlist.m3u8',
                        #'http://118.69.190.173:2935/live/v5-sg08-tdc-c2-cam01-1.stream/playlist.m3u8',
                        #'http://118.69.190.173:1935/live/v7-lan-dha-c5-cam01-1.stream/playlist.m3u8',
                        #'http://118.69.190.173:1935/live/v7-cto-cto_cncto.stream/playlist.m3u8',
                        #'http://118.69.190.173:2935/live/v5-sg11-btn-c3-cam03-1.stream/playlist.m3u8'
                       ]
    '''            
    scr_list        =   [
                        #'http://118.69.190.173:1935/live/v1-hn07-nxq-c4-cam01-1.stream/playlist.m3u8',
                        'http://118.69.190.173:2935/live/v5-sg11-btn-c3-cam03-1.stream/playlist.m3u8'
                       ]

    FPS = 1
    start_date = datetime(2020, 1, 3, 0, 0, 0, 0)
    stop_date  = datetime(2020, 2, 25, 6, 0, 0, 0)
    #print("year =", a.year)
    #print("month =", a.month)
    #print("hour =", a.hour)
    #print("minute =", a.minute)
    #print("timestamp =", a.timestamp())
    
    processes       =   {}
    pipe_parent     =   {}
    
    for i,scr in enumerate(scr_list):
        Stream_, parent_conn_ = Start_newstreaming('khang_test_'+str(i),scr,start_date,stop_date,FPS)
        processes['khang_test_'+str(i)]         = Stream_
        pipe_parent['khang_test_'+str(i)]       = parent_conn_
    
    for process_name in processes:  
        print("PROCESS",process_name)
        processes[process_name].start()
    #for process_name in processes:  
    #    processes[process_name].join()
    while True:
        cv2.imshow('IMAGE',cv2.imread('stop.jpg'))
        for pipe_name in pipe_parent:  
            if pipe_parent[pipe_name].poll():
                print(pipe_parent[pipe_name].recv())
        
        if cv2.waitKey(20) & 0xFF == ord('q'):
            for pipe_name in pipe_parent:  
                pipe_parent[pipe_name].send({'opcode':'stop'})
            time.sleep(2)
            break
    