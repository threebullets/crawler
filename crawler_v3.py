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
    1. Managing Exceptions (E.g. Re-connection, Max waiting time)
    2. Better anti-anti-crawler practice
    
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
MAX_SLEEP_TIME = 0.1
# num of thread used
THREAD_NUM = 5
# time interval for self check. unit: s. Use lower value for debugging
TIMER_INTER = 60
# self check thread
SELF_CHECK_TIMER = None

with open('usr_agent.json') as fin:
    USR_AGENTS = json.load(fin)

logFormatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt='%a, %d %b %Y %H:%M:%S')
rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler('crawler.log')
fileHandler.setFormatter(logFormatter)
fileHandler.setLevel(logging.INFO)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
consoleHandler.setLevel(logging.DEBUG)
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
    page = requests.get(url, headers=headers)
    html = page.text
    # html = page.content.decode('utf-8')
    return html


def processUsrUrl(line):
    # get usr id
    uid = line[0]

    # get html file
    url = line[1]

    try:

        html = getHtml(url)

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

    except Exception as e:
        logging.warning("Error for %s" % line)

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
            pickle.dump(self.processed_record, fout)

    def __iter__(self):
        while True:
            with self.lock:
                line = self.file.readline()
                if line in self.processed_record:
                    logging.debug('Duplicate found for %s' % line)
                    continue
                if not line:
                    raise StopIteration
                self.processed_record.add(line)
                self.count += 1
            yield line.strip('\n').split('\t')

    def __del__(self):
        self.file.close()


def crawlerThread(lines):
    # thread for single crawler
    for line in lines:
        logging.debug('Processing line %s' % line)
        processUsrUrl(line)


def selfCheckThread(lines, prev_num):
    # thread for self check. Pringing information and save records of processed item.
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

