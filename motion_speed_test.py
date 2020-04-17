#franerback
import numpy as np 
import cv2
import sys
import os
import threading 
import time
import multiprocessing 
import Motion_pb2
import Motion_pb2_grpc
from numproto import ndarray_to_proto, proto_to_ndarray
import grpc
import DetectMotion

def movingcheck(queue_id, CurFrame,num_img,mask_size = 40,threshold_cam =10):
    Motion_check = DetectMotion.Cuda_Farnebac_OpticalFlow()
    options = [('grpc.max_send_message_length', 1024 * 1024 * 1024), ('grpc.max_receive_message_length', 1024 * 1024 * 1024)]
    hostname = '192.168.1.45'
    port = '8818'
    channel_motion = grpc.insecure_channel(hostname + ':' + str(port), options=options)
    stub = Motion_pb2_grpc.PredictStub(channel_motion)
    for frame_id in range(num_img):
        
        image_id = str(frame_id)+ '_' + str(int(round(time.time())))
        #Motion_check.Movingcheck(CurFrame,CurFrame, mask_size, threshold_cam,640,queue_id,image_id)
        requestPrediction  = Motion_pb2.Features(queue_id = queue_id,image_id=image_id,
                                    CurFrame = CurFrame, MaskSize= mask_size,
                                    Threshold = threshold_cam,ImageSize=640)
        responsePrediction = stub.DetectMotion(requestPrediction,30)
        
        # status = responsePrediction.Status
        # num_box = responsePrediction.NumBoxes
        # if status:
        #     result = np.frombuffer(responsePrediction.Boxes, dtype= 'float')
        #     result = result.reshape(num_box,4)
        # else:
        #     result = np.array([])

        # return status,num_box,result
def main():
    CurFrame = cv2.imread('../backend_demo/image.jpg')
    CurFrame = cv2.cvtColor(CurFrame, cv2.COLOR_BGR2GRAY)
    CurFrame = cv2.resize(CurFrame, (640,640))
    CurFrame = ndarray_to_proto(CurFrame)
    t_start = time.time()
    worker=[]
    worker_num = 6
    num_test = 100
    for i in range(worker_num):
        t = multiprocessing.Process(target=movingcheck,args=(str(i),CurFrame,num_test,))
        t.start()
        worker.append(t)

    for t in worker:
        t.join()
    t_stop = time.time()

    print("Time infer: ", t_stop - t_start )



    total_set_of_pred = worker_num*num_test

    FPS = total_set_of_pred / (t_stop -t_start)
    print ('FPS for ', total_set_of_pred, ' is ', FPS)


if __name__ == '__main__':

    main()

