#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
modified 2020-03-24
1. 增加index.m3u8后，ts文件夹内的s3总数量大1，即 get_s3_numbers = all_ts_numbers + 1
2. 不相等的子文件夹路径会输出到 not_equal_ts.log
3. 执行upload_again()函数即可遍历'not_equal_ts.log'，并完成上传（全部遍历一遍上传）
"""

import boto3
import requests
import logging
import _1_upload
import os
import time
import queue
import threading

class multi_compare(threading.Thread):
    def __init__(self, url, queue, logger, bucket, prefix):
        threading.Thread.__init__(self)
        self.queue = queue
        self.url = url
        self.bucket = bucket
        self.logger = logger
        self.all_ts_numbers, self.prefix, self.path = self.get_information()
        if prefix != '':
            self.prefix = prefix            
    
    def run(self):
        try:
            self.all_s3_ts_numbers = self.get_s3_numbers(self.bucket, self.prefix, self.path)
            if self.all_ts_numbers == 0:
                self.logger.error('502-'+self.url)               
            elif self.all_s3_ts_numbers-1 == self.all_ts_numbers or self.all_s3_ts_numbers-2 == self.all_ts_numbers:
                print('yes,{0},numbers:{1}'.format(self.url, self.all_ts_numbers))
            else:
                print('不相等,{0},s3:{1},should be:{2}'.format(self.url,self.all_s3_ts_numbers, self.all_ts_numbers))
                self.logger.error(self.url)
        except Exception as e:
            self.logger.error(self.url)
        finally:
            self.queue.get(True,10)
            self.queue.task_done()

    def get_s3_numbers(self, bucket, prefix, path):
        return sum(1 for _ in self.bucket.objects.filter(Prefix=self.prefix+self.path))

    def get_information(self):
        if 'index.m3u8' in self.url and '/hls/' not in self.url:
            download = requests.get(self.url).text
            start    = download.rfind('\n')+1
            newlink  = download[start:]
            new_url  = self.url[:self.url.find('index.m3u8')] + newlink
        else:
            new_url = self.url
            
        prefix, path = self.get_prefix(new_url)
        
        all_ts_numbers = sum(1 for item in requests.get(new_url).text.split('\n') if len(item) > 0 and item[0] != '#' and item[-3:]=='.ts')
    
        return all_ts_numbers, prefix, path
    
    def get_prefix(self, new_url):
        '''
        根据url自动获得prefix + path
        '''
        end_1 = new_url.find('/',10)
        end_2 = new_url.find(':',10)
        if end_1 >0 and end_2>0:
            end = min(end_1, end_2)
        else:
            end = max(end_1,end_2)
        prefix = new_url[new_url.find('//')+2:end].replace('.','-')+'/'    
        right = new_url.find('.m3u8')
        start = new_url.rfind('/',0,right)
        path = new_url[new_url.find('/',10)+1:start+1] 
        return prefix, path


def upload_again(bucket, prefix, maxThreads = 300):
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
        path = os.getcwd()+'/tsfile'
        #获取所有log文件
        try:
            log_lists = filter(lambda x: '.log' in x,os.listdir(path))
        
            if not log_lists:
                print('no log files')
            log_file = get_latest_log(log_lists)
            print('lastest log file:',log_file)
        
            return 'tsfile/'+log_file
        except Exception as e:
            print(e)
    
    all_links_file = get_all_links()
    tslogger = set_tslogger()
        
    with open(all_links_file,'r') as f:
        all_m3u8_lists = set()
        all_m3u8 = f.read().splitlines()
        for link in all_m3u8:
            if link and '502-' in link:
                all_m3u8_lists.add(link[4:])
            elif link:
                all_m3u8_lists.add(link)

    for url in all_m3u8_lists:
        _1_upload.multi_thread(url, maxThreads, tslogger, bucket, prefix)


def set_tslogger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    if os.path.isdir('tsfile'):
        ch = logging.FileHandler('tsfile/%s.log'%time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
    else:
        os.mkdir('tsfile')
        ch = logging.FileHandler('tsfile/%s.log'%time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger

def main():
    s3 = boto3.resource('s3')
    tslogger = set_tslogger()
    
    #自修改信息区域--------------------
    my_bucket = s3.Bucket('')
    txt_file = ''
    prefix = ''  # 最后需要带 "/", 如 "video/"
    maxThreads = 100
    #-------------------------------
    
    q = queue.Queue(maxThreads)
    with open(txt_file) as f:
        for url in f.read().splitlines():
            if url:
                q.put(url)
                t = multi_compare(_1_upload.delete_1000kb_hls(url), q, tslogger, my_bucket, prefix)
                t.start()
        q.join()
    #print('over')

if __name__ == '__main__':
    main()
