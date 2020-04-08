from __future__ import print_function
# This is a placeholder for a Google-internal import.
import cv2
import grpc
import tensorflow as tf
from tensorflow_serving.apis import predict_pb2
from tensorflow_serving.apis import prediction_service_pb2_grpc
from tensorflow_serving.apis import model_pb2
from tensorflow_serving.apis import prediction_log_pb2

import numpy as np
import time
from keras.preprocessing import image as imagekeras
import keras.backend as K
#from efficientnet.keras import center_crop_and_resize, preprocess_input

global status
status = {
      1: "UNAVAILABLE - Server connection error",
      2: "INVALID_ARGUMENT - Input name or shape input is wrong",
      3: "FAILED_PRECONDITIO - Signature name is wrong",
      4: "NOT_FOUND - Model name is wrong",
      5: "DEADLINE_EXCEEDED - Time out",
      6: "OUT_NAME - Output name is wrong"
    }
def get_status(status_code):
  global status
  if status_code !=None and status_code <= len(status) and status_code > 0 :
    return status[status_code]
  else:
    return None

class GRPC_inference_OD:
  def __init__(self, hostport, model_name, signature_name, image_shape,
                                          graph_input_name, graph_score_name, graph_numbox_name, graph_classes_name, graph_boxes_name ):
    '''
    Defauult image type for inference is RGB
    '''
    #Intit core para
    self.hostport       = hostport
    self.model_name     = model_name
    self.signature_name = signature_name
    self.image_shape    = image_shape
    #Init GRPC para
    channel    = grpc.insecure_channel(hostport)
    self.stub  = prediction_service_pb2_grpc.PredictionServiceStub(channel)
    request    = predict_pb2.PredictRequest()
    request.model_spec.name           = model_name
    request.model_spec.signature_name = signature_name
    self.request                      = request
    #Init graph para
    self.graph_input_name   = graph_input_name
    self.graph_score_name   = graph_score_name
    self.graph_numbox_name  = graph_numbox_name
    self.graph_classes_name = graph_classes_name 
    self.graph_boxes_name   = graph_boxes_name

  def do_inference_sync(self,image, timeout):
    #Resize
    # if imagetype == 'BGR':
    image = cv2.cvtColor(image,cv2.COLOR_BGR2RGB)
    image = cv2.resize(image, self.image_shape, interpolation=cv2.INTER_NEAREST)
    #Make request and send
    self.request.inputs[self.graph_input_name].CopyFrom(tf.contrib.util.make_tensor_proto(image,shape=[1]+list(image.shape)))
    result_future = self.stub.Predict.future(self.request,timeout)
    return self.post_processing(result_future)
  
  def make_warm_up_file(self,image_path, savepath):
    image = cv2.imread(image_path)
    image = cv2.resize(image, self.image_shape, interpolation=cv2.INTER_NEAREST)
    #Make request and send
    with tf.python_io.TFRecordWriter(savepath) as writer:
      self.request.inputs[self.graph_input_name].CopyFrom(tf.contrib.util.make_tensor_proto(image,shape=[1]+list(image.shape)))
      log = prediction_log_pb2.PredictionLog(
          predict_log=prediction_log_pb2.PredictLog(request=self.request))
      writer.write(log.SerializeToString())

  def post_processing(self,result_future):
    exception = result_future.exception()
    if exception:
      #print the exception below for more details
      #print(exception)
      if exception.code() == grpc.StatusCode.UNAVAILABLE:
        return 1, None, None, None, None
      elif exception.code() == grpc.StatusCode.INVALID_ARGUMENT:
        return 2, None, None, None, None
      elif exception.code() == grpc.StatusCode.FAILED_PRECONDITION:
        return 3, None, None, None, None
      elif exception.code() == grpc.StatusCode.NOT_FOUND:
        return 4, None, None, None, None
      elif exception.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return 5, None, None, None, None
      else:
        return 0, None, None, None, None
    else:
      if   (len(result_future.result().outputs[self.graph_numbox_name].float_val)  == 0):
        print("case 1 ")
        return 6, None, None, None, None
      elif ((len(result_future.result().outputs[self.graph_numbox_name].float_val)  != 0)  and \
              (len(result_future.result().outputs[self.graph_score_name].float_val)   == 0  or \
               len(result_future.result().outputs[self.graph_classes_name].float_val) == 0  or \
               len(result_future.result().outputs[self.graph_boxes_name].float_val)   == 0   )):
        print("case 2 ")
        return 6, None, None, None, None
      else:
        num_box = int(np.array(result_future.result().outputs[self.graph_numbox_name].float_val))
        score   = np.array(result_future.result().outputs[self.graph_score_name].float_val)[0:num_box]
        classes = np.array(result_future.result().outputs[self.graph_classes_name].float_val, dtype=int)[0:num_box]
        boxes   = np.array(result_future.result().outputs[self.graph_boxes_name].float_val)[0:num_box*4].reshape((num_box,4))
        return None, num_box, classes, score, boxes


class GRPC_inference_Classify:
  def __init__(self, hostport, model_name, signature_name, image_shape, 
                                    graph_input_name, graph_predict_name,graph_FE_name=None):
    '''
    Defauult image type for inference is RGB
    Defauult preprocessing_mean,preprocessing_std is RGB
    '''
    #Intit main para
    self.hostport       = hostport
    self.model_name     = model_name
    self.signature_name = signature_name
    self.image_shape    = image_shape
    #Init GRPC para
    channel    = grpc.insecure_channel(hostport)
    self.stub  = prediction_service_pb2_grpc.PredictionServiceStub(channel)
    request    = predict_pb2.PredictRequest()
    request.model_spec.name           = model_name
    request.model_spec.signature_name = signature_name
    self.request                      = request
    #Init graph para
    self.graph_input_name     = graph_input_name
    self.graph_predict_name   = graph_predict_name
    if graph_FE_name is not None:	
        self.graph_FE_name = graph_FE_name
    else:
        self.graph_FE_name = None     
    # preprocessing parameter from the train data set ex: list follow RGB 
 
 
  def do_inference_sync(self, image, timeout):
    #Resize
    # DO NOT process as caller has already processed w/ EfficienNet
    #image = self.pre_processing(image)

    self.request.inputs[self.graph_input_name].CopyFrom(tf.contrib.util.make_tensor_proto(image,shape=[1]+list(image.shape)))
    result_future = self.stub.Predict.future(self.request,timeout)
    return self.post_processing(result_future)

  # def make_warm_up_file(self, image_path, savepath):
  #   img = cv2.imread(image_path)
  #   img_preprocess = center_crop_and_resize(img, image_size=self.image_shape[0])
  #   img_preprocess = preprocess_input(img_preprocess)
  #   img_preprocess = img_preprocess.astype(np.float32)
  #   with tf.python_io.TFRecordWriter(savepath) as writer:
  #     self.request.inputs[self.graph_input_name].CopyFrom(tf.contrib.util.make_tensor_proto(img_preprocess,shape=[1]+list(img_preprocess.shape)))
  #     #result_future = self.stub.Predict.future(self.request,timeout)
  #     log = prediction_log_pb2.PredictionLog(
  #         predict_log=prediction_log_pb2.PredictLog(request=self.request))
  #     writer.write(log.SerializeToString())
  
  def pre_processing(self, image):
    # Chua dung toi ham nay
    image = cv2.resize(image, self.image_shape, interpolation=cv2.INTER_NEAREST)
    image = imagekeras.img_to_array(image)
    image = image[..., ::-1]
    # Zero-center by mean pixel
    # image[..., 0] -= self.preprocessing_mean[0]
    # image[..., 1] -= self.preprocessing_mean[1]
    # image[..., 2] -= self.preprocessing_mean[2]
    # image[..., 0] /= self.preprocessing_mean[0] # scale values
    # image[..., 1] /= self.preprocessing_mean[1] # scale values
    # image[..., 2] /= self.preprocessing_mean[2] # scale values
    return image

  def post_processing(self,result_future):
    exception = result_future.exception()
    if exception:
      #print the exception below for more details
      print(exception)
      if exception.code() == grpc.StatusCode.UNAVAILABLE:
        return 1, None
      elif exception.code() == grpc.StatusCode.INVALID_ARGUMENT:
        return 2, None
      elif exception.code() == grpc.StatusCode.FAILED_PRECONDITION:
        return 3, None
      elif exception.code() == grpc.StatusCode.NOT_FOUND:
        return 4, None
      elif exception.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        return 5, None
      else:
        return 0, None
    else: 
      predict = np.array(result_future.result().outputs[self.graph_predict_name].float_val)
      len_check = len(predict)
      predict = predict.reshape(1,-1)
      if len_check == 0:
        return 6, None
      else:
        if self.graph_FE_name is not None:
            FE = np.array(result_future.result().outputs[self.graph_FE_name].float_val)
            return None, predict, FE
        else:
            return None, predict
