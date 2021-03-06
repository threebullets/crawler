#!usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Aiming at:

    Get html, extract meta data and title and save them in json.

First version by yifyu@foxmail.com

Input:
    userid-url file in lines, split '\t'
    
Output:
    html files, with hopefully following infomation:
        
        title, 
        keywords, 
        description, 
        meta
        
    and saved in json files

Need improvements:
    1. Better anti-anti-crawler practice
    
"""
import time
import random
import requests
from bs4 import BeautifulSoup
import json
import logging
import os
import threading
import pickle
import sys

# usr-url file path
FILE_PATH = './sample100.csv'
# file path for record of processed users and urls
RECORD_PATH = 'record.pic'


# denote output path
OUTPUT_PATH = './result'

if not os.path.exists(OUTPUT_PATH):
    os.mkdir(OUTPUT_PATH)

# max sleep time(in sec)
# uniform distributed (0,max sleep time)
# set near to zero, get max speed
# But under greater detection risk
MAX_SLEEP_TIME = 1
# max time of waiting connection
TIME_OUT = 60
# max times of reconnecting
MAX_RETRY_TIMES = 5
# num of thread used
THREAD_NUM = 5
# time interval for self check. unit: s. Use lower value for debugging
TIMER_INTER = 60
# self check thread
SELF_CHECK_TIMER = None

# list for user agent candidates
USR_AGENTS = [
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"
]

# disable logger from requests
logging.getLogger("requests").setLevel(logging.WARNING)
# setting logger format
logFormatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", 
                                 datefmt='%a, %d %b %Y %H:%M:%S')
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

# logger for file log output
fileHandler = logging.FileHandler('crawler.log')
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.INFO)
rootLogger.addHandler(fileHandler)

# logger for console log output
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.DEBUG)
# uncomment the next line to set console logger level to INFO
# consoleHandler.setLevel(logging.INFO)
rootLogger.addHandler(consoleHandler)


def getHtml(url):

    # Fake headers:
    # When Blocked by certain website,
    # change this and try again
    # for more kinds of headers, search online
    headers = {'User-Agent': random.choice(USR_AGENTS),
               'Referer': 'https://www.baidu.com'}

    # This function gets a page
    # returns a html file in text
    page = requests.get(url, headers=headers, timeout=TIME_OUT)
    html = page.text
    # html = page.content.decode('utf-8')
    return html


def processUsrUrl(line):
    # get usr id
    uid = line[0]

    # get html file, try again if the connection fails
    url = line[1]
    html = None    
    retryTimes = MAX_RETRY_TIMES
    while retryTimes > 0:
        try:
            html = getHtml(url)
        except requests.exceptions.RequestException:
            logging.warning("Request error for %s, retrying..." % line)
            time.sleep(1)
            retryTimes -= 1
        else:
            break
    if html == None:
        logging.warning("Request error for %s, retrying failed." % line)
        return

    # Extract meta and title data
    soup = BeautifulSoup(html, 'html.parser')
    meta = list(soup.find_all(name='meta'))
    title = list(soup.find_all(name='title'))

    try:
        description = soup.find(attrs={"name": "description"})['content']
    except:
        description = None

    try:
        keywords = soup.find(attrs={"name": "keywords"})['content']
    except:
        try:
            keywords = soup.find(attrs={"name": "keyword"})['content']
        except:
            keywords = None

    # Save data to file
    result = {'uid': uid,
              'url': url,
              'title': str(title),
              'keywords': keywords,
              'description': description,
              'meta': str(meta),
              'html': html}

    with open(file=os.path.join(OUTPUT_PATH, uid + '-'
            + time.strftime("%Y%m%d%H%M%S", time.localtime()) + '.json'),
              mode='x',
              encoding='utf-8') as file_obj:
        json.dump(result, file_obj)


    # Random request to avoid detection
    time.sleep(random.uniform(0, MAX_SLEEP_TIME))


class UsrUrlIter:
    # Establish a memory-friendly, thread-safe generator
    def __init__(self, fname, record_fname=RECORD_PATH):
        self.file = open(fname, 'rt', encoding='utf-8')
        try:
            with open(RECORD_PATH, 'rb') as fin:
                self.processed_record = pickle.load(fin)
        except IOError:
            self.processed_record = set()
        self.lock = threading.Lock()
        self.count = 0

    def saveRecord(self):
        with open(RECORD_PATH, 'wb') as fout:
            pickle.dump(self.processed_record, fout, -1)

    def __iter__(self):
        while True:
            with self.lock:
                line = self.file.readline()
                if line in self.processed_record:
                    logging.debug('Duplicate found for %s' % line.strip('\n'))
                    continue
                if not line:
                    raise StopIteration
                self.processed_record.add(line)
                self.count += 1
            yield line.strip('\n').split('\t')

    def __del__(self):
        self.file.close()


def crawlerThread(lines):
    for line in lines:
        logging.debug('Processing line %s' % line)
        try:
            processUsrUrl(line)
        except:
            url = line[1]
            with open(url, 'w') as fout:
                fout.write(sys.exc_info()[0])
            logging.warning('Unknow error for %s, error message saved' % line)



def selfCheckThread(lines, prev_num):
	# thread for self check. Pringing information and save records of processed item.
    if not threading.main_thread().is_alive():
        exit()
    global SELF_CHECK_TIMER
    lines.saveRecord()
    logging.info('%d processed totally, %d processed in the last %d seconds. Process record saved.'
                 % (lines.count, lines.count - prev_num, TIMER_INTER))
    SELF_CHECK_TIMER = threading.Timer(TIMER_INTER, selfCheckThread, args=(lines, lines.count))
    SELF_CHECK_TIMER.start()


if __name__ == '__main__':
    logging.info('Start crawling users and urls in %s' % FILE_PATH)
    time_start = time.time()
    # read usr-url line by line
    # with minimal memory consumption
    lines = UsrUrlIter(FILE_PATH)
    logging.info('%d threads are used to crawl web pages' % THREAD_NUM)
    threads = [threading.Thread(target=crawlerThread, args=(lines,)) for i in range(THREAD_NUM)]
    for thread in threads:
        thread.start()
    logging.info('Starting self check thread...')
    self_check_thread = threading.Thread(target=selfCheckThread, args=(lines, 0))
    self_check_thread.start()
    self_check_thread.join()
    for thread in threads:
        thread.join()
    logging.info('All crawling threads completed')
    SELF_CHECK_TIMER.cancel()
    lines.saveRecord()
    totaltime = time.time() - time_start
    logging.info('Mission completed, %d processed. time used: %s sec' % (lines.count, totaltime))


'''
############################## Load data by json in Windows ########################
### in Windows, the text is likely to be coded by utf-8 but in str type already
### If that's the case, convert it into bytes again and decode with following function:

def mydecode(str):
    return bytes((ord(i) for i in str)).decode('utf-8')

def decodeJSON(file):
    with open(file) as in_file:
	    d = json.load(in_file)
    for key in d.keys():
	    d[key] = mydecode(d[key])
	return d
	
dic = decodeJSON(filename)
'''

'''
################### Load data by json in Linux ########################
### In linux, the encoding seems to work well

# dict = json.load(open(file))

# dict['keywords']

'''
