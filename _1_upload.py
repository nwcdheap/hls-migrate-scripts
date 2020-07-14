import threading
import queue
import requests
import boto3
import logging
import time
import os

s3_client = boto3.client('s3')


class download_and_upload(threading.Thread):
    def __init__(self, link, queue, logger, bucket, prefix):
        threading.Thread.__init__(self)
        self.queue = queue
        self.link = link
        self.bucket = bucket
        self.path = prefix + link[link.find('/',10)+1:]
        self.logger = logger

    def run(self):
        try:
            #print('开始下载',self.path)
            file = requests.get(self.link)
            if file.status_code == 200:
                s3_client.put_object(Body=file.content, Bucket=self.bucket, Key=self.path)
        except Exception as e:
            print(e)
            self.logger.error(self.link)
        finally:
            self.queue.get(True,10)
            self.queue.task_done()

def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    #if logger_name == '404logger' and not os.path.isdir('404'):
        #os.mkdir('404')
    fileHandler = logging.FileHandler(log_file, mode='w')
    streamHandler = logging.StreamHandler()

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)    

def get_all_ts_links(new_url):

    all_ts_names = requests.get(new_url).text.split('\n')
    pre = new_url[:new_url.find('index.m3u8')]
 
    all_ts_urls = (pre + i for i in filter(lambda x:len(x) > 0 and x[0] != '#',all_ts_names))
    return all_ts_urls

def multi_thread(url, maxThreads, logger, bucket, prefix):
    q = queue.Queue(maxThreads)
    print('dealing with:', url)
    
    if '/hls/' in url:
        new_url = url
    else:
        download = requests.get(url).text
        start    = download.rfind('\n')+1
        newlink  = download[start:]
        new_url  = url[:url.find('index.m3u8')] + newlink 
    
    t00 = download_and_upload(new_url, q, logger, bucket, prefix)
    t00.start()
    
    t0 = download_and_upload(url, q, logger, bucket, prefix)
    t0.start()
    
    for ts_link in get_all_ts_links(new_url):
        q.put(ts_link)
        t = download_and_upload(ts_link, q, logger, bucket, prefix)
        t.start()
    q.join()
    #print('over')

def multi_thread_playlist(url, maxThreads, logger, bucket, prefix):
    q = queue.Queue(maxThreads)
    print('dealing with:', url)
    
    pre = url[:url.rfind('/',0,url.find('.m3u8'))+1]
    download = requests.get(url).text.split('\n')

    all_ts_links = (pre + i for i in filter(lambda x:len(x) > 0 and x[0] != '#' and x[-3:] == '.ts',download))
    
    t0 = download_and_upload(url, q, logger, bucket, prefix)
    t0.start()

    for ts_link in all_ts_links:
        q.put(ts_link)
        t = download_and_upload(ts_link, q, logger, bucket, prefix)
        t.start()
    q.join()

def check_error(bucket, prefix, max_thread = 300):
    def get_latest_log(log_lists):
        latest_time = None
        for i in log_lists:
            time_ls = time.strptime(i[:-4],"%Y-%m-%d-%H-%M-%S")
            if latest_time == None:
                latest_time = time_ls
            elif time_ls > latest_time:
                latest_time = time_ls
        return time.strftime("%Y-%m-%d-%H-%M-%S", latest_time)+'.log'

    def get_all_links():
        path = os.getcwd()
        #获取所有log文件
        try:
            log_lists = filter(lambda x: '.log' in x,os.listdir(path))
        
            if not log_lists:
                print('no log files')
            log_file = get_latest_log(log_lists)
            print('lastest log file:',log_file)
        
            with open(log_file) as f:
                return [line.strip() for line in f.readlines()]
        except Exception as e:
            print(e)
    
    all_links = get_all_links()
    
    q = queue.Queue(max_thread)
    
    if all_links:
        setup_logger('log',r'%s.log'%time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
        logger = logging.getLogger('log')

        for link in all_links:
            #print(link)
            q.put(link)
            t = download_and_upload(link, q, logger, bucket, prefix)
            t.start()
        q.join()


def delete_1000kb_hls(url):
    if 'kb/hls' in url:
        number = url.find('kb/hls')
        return url[:url.rfind('/',0,number)] + url[number+6:]
    else:
        return url
    
def main():
    setup_logger('log',r'%s.log'%time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
    logger = logging.getLogger('log')

    with open(file_path,'r') as f:
        all_m3u8_lists = f.read().splitlines()

    #logging.info('main task starts')
    for url in all_m3u8_lists:
        if url and 'index.m3u8' in url:
            multi_thread(delete_1000kb_hls(url), maxThreads, logger, bucket, prefix)
        elif url and 'playlist.m3u8' in url:
            multi_thread_playlist(url, maxThreads, logger, bucket, prefix) 

if __name__ == '__main__':  
    #--------------------------------自修改信息区域------------------------------#
    
    file_path = 'xxx.txt'
    maxThreads = 800
    bucket = 'xxx'
    prefix = 'xxx/'   #最前面不要/,最后要/，比如 abc/123/
    
    #--------------------------------------------------------------------------#

    main()
