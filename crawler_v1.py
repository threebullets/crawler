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
    1. Develop a Multi-Thread Approach
    2. Managing Exceptions (E.g. Re-connection, Max waiting time) 
    3. Better anti-anti-crawler practice 
    4. Writing Logs
    
"""
import time
import random
import requests
from bs4 import BeautifulSoup
import json

# usr-url file path
FILE_PATH = 'E:\\BiheTech\\sample10000.csv'

# denote output path
OUTPUT_PATH = 'E:\\BiheTech\\result'

# max sleep time(in sec)
# uniform distributed (0,max sleep time)
# set near to zero, get max speed
# But under greater detection risk
MAX_SLEEP_TIME = 0.1

# Fake headers:
# When Blocked by certain website,
# change this and try again
# for more kinds of headers, search online
headers = {'User-Agent':'Mozilla/4.0(compatible;MSIE7.0;WindowsNT5.1;360SE)',
           'Referer':'https://www.baidu.com'}

def getHtml(url):
    # This function gets a page
    # returns a html file in text
    page = requests.get(url, headers = headers)
    html = page.text
    # html = page.content.decode('utf-8')
    return html

class MyUsrUrl(object):
    # Establish a memory-friendly generator
    def __init__(self, fname):
        self.fname = fname

    def __iter__(self):
        for line in open(self.fname, 'rt', encoding='utf-8'):
            yield line.strip('\n').split('\t')
            
# read usr-url line by line
# with minimal memory consumption            
lines = MyUsrUrl(FILE_PATH)

time_start = time.time()

count = 0

for line in lines:
    
    # get usr id
    uid = line[0]
    print('Current uid:')
    print(uid)
    
    # get html file
    url = line[1]
    print('Current url:')
    print(url)
    
    try:
        
        print('Getting html file...')
        html = getHtml(url)
    
        # Extract meta and title data
        print('Extracting meta and title data...')
        print('Soup it...')
        soup = BeautifulSoup(html, 'html.parser')
        
        print('Getting meta list...')
        meta = list(soup.find_all(name = 'meta')) 
        
        print('Getting title...')
        title = list(soup.find_all(name = 'title')) 
        
        print('Getting description...')
        try:
            description = soup.find(attrs={"name":"description"})['content']  
        except:
            description = None
            
        print('Getting keywords...')
        try:
            keywords = soup.find(attrs={"name":"keywords"})['content']
        except:
            try:
                keywords = soup.find(attrs={"name":"keyword"})['content']
            except:
                keywords = None
                
                
        # Save data to file
        result = {'uid':uid, 
                  'url':url,
                  'title':str(title),
                  'keywords':keywords,
                  'description':description,
                  'meta':str(meta),
                  'html':html}
        
        print('Trying to save file...')
        with open(file = OUTPUT_PATH + '\\' + uid + '-' 
                  + time.strftime("%Y%m%d%H%M%S",time.localtime()) + '.json',
                  mode = 'x', 
                  encoding = 'utf-8') as file_obj:
            json.dump(result, file_obj)
    
    except Exception as e:
        print (e)
    
    print('Time when completed:')
    print(time.strftime("%Y-%m-%d-%H:%M:%S",time.localtime()))
    
    # Random request to avoid detection
    time.sleep(random.uniform(0,MAX_SLEEP_TIME))
    
    # count # of urls that have been processed
    count += 1
    print(str(count)+' urls have been processed.')
    print()
    print()

time_end = time.time()

totaltime = time_end - time_start

print('Mission completed.')
print('time used: ' + str(totaltime) + ' sec')

################### Load data by json ########################
# dict = json.load(open(file))
# dict['keywords']
    