#!/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
import codecs
from datetime import datetime
import time
import redis
import os
import re
import socket


PASSAGE_VALUES = (
    # 三网 J60010
    ('4', u'C:\\Users\Administrator\Desktop\emp部署-window\SPGATE\sms_spgate\LOGS\LOGS%s\AllLog.txt', '127.0.0.1', 8022),
    # 海交所   
    ('20', u'C:\\Users\Administrator\Desktop\emp部署-window\SPGATE\sms_spgate - 105-海南股权\LOGS\LOGS%s\AllLog.txt', '127.0.0.1', 9001),
    # 验证码 J21770
    ('2', u'C:\\Users\Administrator\Desktop\emp部署-window\通讯网关程序\wangg\SPGATE\sms_spgate\LOGS\LOGS%s\AllLog.txt', '127.0.0.1', 7901),
    # 通知 J21778
    ('3', u'C:\\Users\Administrator\Desktop\emp部署-window\通讯网关程序\wangg\SPGATE\sms_spgate2\LOGS\LOGS%s\AllLog.txt', '127.0.0.1', 7901)
    )

REDIS_IP = '127.0.0.1'
REDIS_PORT = 6379

ISOFORMAT='%Y%m%d'

isContainError = re.compile(u'错误码').search


def contain_error(logline):
    """分析日志是否出现错误码
    如果有错误码    返回True 
    如果没有错误码  返回False
    """
    return isContainError(logline)


def scan_log(logfile, filemark):
    """逐行读文件
    返回文本行和文件偏移量
    """
    lines = []
    if os.path.exists(logfile):
        with codecs.open(logfile, 'r', 'gbk') as f:
            f.seek(filemark)
            lines = f.readlines()
            filemark = f.tell()
    else:
        print 'file ' + logfile + 'not exists'
    return lines, filemark


def send_failure(passage):
    """发送通道错误
    使通道的错误数 +1
    """
    r = redis.StrictRedis(host=REDIS_IP, port=REDIS_PORT, db=0)
    r.incr(passage + ':ERROR')


def refresh_ok(passage):
    """
    """
    r = redis.StrictRedis(host=REDIS_IP, port=REDIS_PORT, db=0)
    r.set(passage + ':TTL', 'OK')
    r.expire(passage + ':TTL', 40)


def get_last_file_offset(passage):
    r = redis.StrictRedis(host=REDIS_IP, port=REDIS_PORT, db=0)
    return int(r.get(passage + ':FILEOFFSET'))


def set_filemark(passage, filemark):
    r = redis.StrictRedis(host=REDIS_IP, port=REDIS_PORT, db=0)
    r.set(passage + ':FILEOFFSET', filemark)


def monitor_remote(ip, port, passage):
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.settimeout(1)
    try:
        sk.connect((ip, port))
    except Exception:
        r = redis.StrictRedis(host=REDIS_IP, port=REDIS_PORT, db=0)
        r.incr(passage + ':ERROR')
        r.incr(passage + ':ERROR')
    finally:
        sk.close()

def task(passage, logfile, ip, port):
    filemark = get_last_file_offset(passage)
    daymark = datetime.now().strftime(ISOFORMAT)

    while True:
        try:
            monitor_remote(ip, port, passage)

            today = datetime.now().strftime(ISOFORMAT)
            if daymark != today:
                daymark = today
                filemark = 0
                
            lines, filemark = scan_log(logfile % today, filemark)
            for line in lines:
                if contain_error(line):
                    send_failure(passage)
            set_filemark(passage, filemark)
            refresh_ok(passage)
            time.sleep(10)
            print 'MONITOR__'+passage+'__OK'
        except Exception, ex:
            print ex
            print 'MONITOR__'+passage+'__ERROR'
            time.sleep(10)

def init_monitor(passage):
    r = redis.StrictRedis(host=REDIS_IP, port=REDIS_PORT, db=0)
    r.sadd('MONITORED_PASSAGE', passage)
    if r.get(passage + ':FILEOFFSET') is None:
        r.set(passage + ':FILEOFFSET', 0)
    if r.get(passage + ':ERROR') is None:
        r.set(passage + ':ERROR', 0)
    r.set(passage + ':TTL', 'OK')

def run_monitor():
    for passage, logfile, ip, port in PASSAGE_VALUES:
        init_monitor(passage)
        monitor = threading.Thread(target=task, args=(passage, logfile, ip, port))
        monitor.start()
    

if __name__ == '__main__':
    run_monitor()
