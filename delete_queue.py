import pika
credentials = pika.PlainCredentials('user', 'user')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))

channel = connection.channel()
def restart_queue():
    scr_list = [
    'cto_pgd_thotnot_02_48_138.stream',
    'vung5-cuchi-04.stream'
    ]
    for i in scr_list:
        channel.queue_delete(queue=i)
if __name__ == "__main__":
    restart_queue()