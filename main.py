import cv2,time
from multiprocessing import Process, Queue, Pipe
from datetime import datetime as time_day
import grpc_infer_api
import sys
import os
import psutil
import json
import pika
import numpy as np
import get_stream
import csv
import Motion_pb2
import Motion_pb2_grpc
from numproto import ndarray_to_proto, proto_to_ndarray
import grpc
import datetime
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

        self.credentials = pika.PlainCredentials('user', 'user')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',heartbeat=60,
                                       blocked_connection_timeout=300,credentials=self.credentials))

        self.channel = self.connection.channel()
        self.queue_name = self.name
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.queue_declare(queue='result')
        #self.channel.exchange_declare(exchange='check_status', exchange_type='fanout')
        self.channel.queue_bind(exchange='check_status', queue=self.queue_name)
        self.message_respond = {}
        self.API_ENDPOINT = '192.168.1.45:8101'
        #################################################
        self.OD_model =  grpc_infer_api.GRPC_inference_OD(hostport=self.API_ENDPOINT, model_name='OD', signature_name='serving_default', image_shape=(640 , 640),\
		                                  graph_input_name='input', graph_score_name='score', graph_numbox_name='num', graph_classes_name='classes',\
		                                  graph_boxes_name='boxes')
        #################################################
        self.options = [('grpc.max_send_message_length', 512 * 1024 * 1024), ('grpc.max_receive_message_length', 512 * 1024 * 1024)]
        self.hostname = '192.168.1.45'
        self.port = '8818'
        self.channel_motion = grpc.insecure_channel(self.hostname + ':' + str(self.port), options=self.options)
        self.stub = Motion_pb2_grpc.PredictStub(self.channel_motion)
        self.DATA_DIR = '/hdd/Long/DATA_SAVE_9_4/'
    def moving_visualize(self,status,num_box,boxes,frame):
        h_origin = frame.shape[0]
        w_origin = frame.shape[1]
        for cur_b in boxes:
            xmin_cur, ymin_cur, xmax_cur, ymax_cur = int(cur_b[0]*w_origin), int(cur_b[1]*h_origin),int((cur_b[0]+cur_b[2])*w_origin),int((cur_b[1]+cur_b[3])*h_origin)
            cv2.rectangle(frame, (xmin_cur, ymin_cur), (xmax_cur, ymax_cur), (0, 255, 0), 2)
        return frame
    
    def movingcheck(self,queue_id,image_id, CurFrame,mask_size = 40,threshold_cam =10):
        CurFrame = cv2.cvtColor(CurFrame, cv2.COLOR_BGR2GRAY)

        CurFrame_send = ndarray_to_proto(CurFrame)
        requestPrediction  = Motion_pb2.Features(queue_id = queue_id,image_id=image_id,
                                    CurFrame = CurFrame_send, MaskSize= mask_size,
                                    Threshold = threshold_cam,ImageSize=640)

        responsePrediction = self.stub.DetectMotion(requestPrediction,10)
        status = responsePrediction.Status
        num_box = responsePrediction.NumBoxes
        if status:
            result = np.frombuffer(responsePrediction.Boxes, dtype= 'float')
            result = result.reshape(num_box,4)
        else:
            result = np.array([])

        return status,num_box,result
    
    def overlap_check(self,person_boxes,motion_boxes,frame):
        h = frame.shape[0]
        w = frame.shape[1]
        overlap_check = False
        for pre_b in person_boxes:
            xmin_pre, ymin_pre, xmax_pre, ymax_pre = int(pre_b[1]*w), int(pre_b[0]*h),int(pre_b[3]*w),int(pre_b[2]*h)
            s_pre = (xmax_pre-xmin_pre)*(ymax_pre-ymin_pre)
            for cur_b in motion_boxes:
                xmin_cur, ymin_cur, xmax_cur, ymax_cur = int(cur_b[0]*w), int(cur_b[1]*h),int((cur_b[0]+cur_b[2])*w),int((cur_b[1]+cur_b[3])*h)
                s_cur = (xmax_cur-xmin_cur)*(ymax_cur-ymin_cur)
                x_min,x_max,y_min,y_max = max(xmin_cur,xmin_pre) , min(xmax_cur,xmax_pre), max(ymin_cur,ymin_pre), min(ymax_cur,ymax_pre)
                if (x_max-x_min) > 0 and (y_max-y_min) > 0:
                    Square = abs(x_max-x_min)*abs(y_max-y_min)
                    if Square > 0.4*min(s_pre,s_cur) : 
                        overlap_check = True
                        cv2.rectangle(frame, (x_min, y_min), (x_max, y_max), (0, 0, 255), 1)

        return overlap_check,frame

    def run(self):
        save_dir = self.DATA_DIR + self.name
        if not os.path.isdir(save_dir):
            os.mkdir(save_dir)        # Create target Directory
            print("Directory " , save_dir ,  " Created ")
        else:
            print("Directory " , save_dir ,  " already exists")
      
        cap = cv2.VideoCapture(self.URL)
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]
        frame_id = 0
        pre_frame_id = 0
        
        pre_time = int(round(time.time()))
        pre_time_save = int(round(time.time()))
        self.channel.basic_publish(exchange='', routing_key='send_main', body=json.dumps({"urls":self.name,"opcode":"start"}))
        # self.channel.basic_publish(exchange='', routing_key='result', body=json.dumps({"urls":self.name,"opcode":"start"})) 
        save_check = False
        while(self.Run):
            self.Comunicate_master()
            if  cap.isOpened(): 
                try:
                    ret, frame = cap.read()
                    frame_orginal = frame.copy()
                    frame_predict = cv2.resize(frame, (640,640))
                    # #=============================== MOTION DETECT ===============================
                    image_id = str(frame_id)+ '_' + str(int(round(time.time())))
                    move, numbox, boxes_motion = self.movingcheck(queue_id =self.name,image_id=image_id,CurFrame =frame_predict)
                    frame = self.moving_visualize(move,numbox,boxes_motion,frame)
                    frame_id +=1
                    # #=============================== OD DETECT ===================================
                    frame_predict = cv2.cvtColor(frame_predict, cv2.COLOR_BGR2RGB)
                    try:
                        status, num_box, classes, score, boxes = self.OD_model.do_inference_sync(frame_predict,10)
                    except: 
                        status = -1
                    
                    person_check = False
                    person_boxes = []
                    if num_box != 0:
                        bnbbox = boxes
                        for index in range(num_box):
                            if score[index] > 0.4 and classes[index] == 1 :
                                h, w, _ = frame.shape
                                x1, y1, x2, y2 = int(bnbbox[index][1] * w), int(bnbbox[index][0] * h), int(bnbbox[index][3] * w), int(bnbbox[index][2] * h)
                                #cv2.rectangle(frame, (x1, y1), (x2, y2), (255, 0, 0), 2) 
                                person_boxes.append(bnbbox[index])
                                person_check = True
                    # #=============================== OVERLAP ===========================================
                    if move== True and person_check == True:
                        save_check,frame = self.overlap_check(np.asarray(person_boxes),boxes_motion,frame)
                    else:
                        save_check = False
                    #=============================== UPDATE STATUS ====================================
                    if int(round(time.time())) - pre_time > 5:
                        pre_time = int(round(time.time()))
                        self.channel.basic_publish(exchange='', routing_key='send_main', body=json.dumps({"urls":self.name,"opcode":"start"}))
                    #=============================== SHOW IMAGE =====================================
                    if save_check == True and int(round(time.time())) - pre_time_save > 1:
                        pre_time_save = int(round(time.time()))          
                        cur_time = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
                        name = save_dir + "/" + self.name +'_'+str(cur_time)+'.jpg'
                        cv2.imwrite(name,frame)
                        print("CAPTURED: ",self.name,'_ Time save:',cur_time)

                    cv2.imshow('CAM'+self.name,frame)
                    if cv2.waitKey(30)=='q':
                        self.message_respond['error']='Stop stream'
                        break
                       
                except:
                    self.message_respond['error']='Code error'
                    break
            else:
                self.message_respond['error']='Stream dead'
                break
        cap.release()
        cv2.destroyAllWindows()
        self.message_respond['urls'] = self.name
        self.message_respond['opcode'] = 'stop'
        self.channel.basic_publish(exchange='', routing_key='send_main', body=json.dumps(self.message_respond))   

    def Comunicate_master(self):
        queue_empty = self.channel.queue_declare(queue=self.queue_name).method.message_count
        if queue_empty!=0:
            method, properties, body = self.channel.basic_get(queue=self.queue_name, auto_ack=True)
            self.callback_child_process(self.channel, method, properties, body)
    def callback_child_process(self,ch, method, properties, body):
        new_recv = json.loads(body.decode('utf8'))
        print('=============================')
        print(self.name)
        print('NEW RECIVE: ',new_recv) 
        print('=============================')
        # print(self.channel.queue_declare(queue=self.URL).method.queue)
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
            self.message_respond['error']='Stop stream'
            self.Run = False 
        elif new_recv['opcode'] == 'check':    
            self.channel.basic_publish(exchange='', routing_key='result', body=json.dumps({"urls":self.name,"opcode":"start"}))

class Check_alive(Process):
    def __init__(self, **kwargs):
        super(Check_alive, self).__init__()
        self.credentials = pika.PlainCredentials('user', 'user')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='check_status', exchange_type='fanout')
    def run(self):
        cnt=0
        while True:
            #time.sleep(1)
            message = json.dumps({"opcode":'check'})
            self.channel.basic_publish(exchange='check_status',routing_key='', body=message)
            print('[x][x][x] Fanout message')
            cnt+=1
            if cnt == 1:
                break    
def Start_newstreaming(name,URL, start_date, stop_date , FPS):
    newstreaming =  Streaming(name,URL, start_date, stop_date, FPS)
    return newstreaming

def callback(ch, method, properties, body):
    print(" [x] Received ")
    message_rabbitmq = json.loads(body.decode('utf8'))

    FPS = 1
    start_date = time_day(2020, 1, 3, 0, 0, 0, 0)
    stop_date  = time_day(2021, 10, 25, 6, 0, 0, 0)

    name_process = message_rabbitmq['urls'].split('/')[4]
    if message_rabbitmq['opcode'] == 'start' and name_process not in running_process:
        print('Start process: ',name_process)
        Stream_= Start_newstreaming(name_process,message_rabbitmq['urls'],start_date,stop_date,FPS)
        processes[name_process]         = Stream_
        processes[name_process].start()
        channel.basic_publish(exchange='', routing_key=name_process, body=body)
    elif message_rabbitmq['opcode']=='stop' and name_process in running_process:
        channel.basic_publish(exchange='', routing_key=name_process, body=body)

def recive_mes_process(ch, method, properties, body):
    #print('========================================')
    #print(" [x] [x] [x] ")
    status_process = json.loads(body.decode('utf8'))
    
    if status_process['urls'] not in running_process:
        running_process.append(status_process['urls'])
    elif status_process['urls'] in running_process and status_process['opcode']=='stop':
        print(status_process['urls'],'                    ===> ',status_process['error'])
        running_process.remove(status_process['urls'])
        print("Running process")
        print(len(running_process))


if __name__ == "__main__":
    processes       =   {}
    running_process = []
    credentials = pika.PlainCredentials('user', 'user')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',heartbeat=10,
                                       blocked_connection_timeout=300,credentials=credentials))

    channel = connection.channel()
    channel.queue_declare(queue='mes')
    channel.basic_consume(queue='mes', on_message_callback=callback, auto_ack=True)

    channel_2 = connection.channel()
    channel_2.queue_declare(queue='send_main')
    channel_2.basic_consume(queue='send_main', on_message_callback=recive_mes_process, auto_ack=True)
    # ################################################################################
    # p = Check_alive()
    # p.start()
    # ################################################################################
    
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming() 