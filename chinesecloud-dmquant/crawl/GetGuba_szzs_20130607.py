#coding=GBK
import Queue
import threading
import urllib2
import time
import re
import os
from BeautifulSoup import *
from urlparse import urljoin
import pyodbc 
import string
import sys
reload(sys)
from ctypes import *
dll=CDLL('divide_word.dll')
dllfenci=CDLL('ICTCLAS50.dll')
dll.divide_init()
sys.setdefaultencoding('gbk')
keyword={}
filr=file('keyWordsValue.txt','r')
kwArray=filr.readlines()
for kw in kwArray :
     try:
        Items=kw.split(' ')
        keyword[Items[0]]=string.atoi(Items[1][:-1])
     except:
        continue
	print 'There is an erro in keywordValue Addiction\n'
l2='\xe3\x80\x82|\xef\xbc\x81|\xef\xbc\x9f|\xef\xbc\x8c| '#r'。|！|？|，'
rep=re.compile(l2)



#hosts = ["http://guba.eastmoney.com/gblb.html"]
hosts=["http://guba.eastmoney.com/list,szzs.html"]
queue = Queue.Queue()
out_queue = Queue.Queue()

class ThreadUrl(threading.Thread):
    def __init__(self, queue, out_queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        

    def run(self):
        while True:
           try:
               #grabs host from queue
               host = self.queue.get()
	       if self.out_queue.qsize()>5000:
		       time.sleep(500)
           
               #grabs urls of hosts and then grabs chunk of webpage
               if 'list'  in host:
                  print self.getName()+'Now Grabbing'+host
            
                  url = urllib2.urlopen(host)
           #    print self.getName()+'END___Grabbing'+host
                  chunk = url.read()

               #place chunk into out queue
                  self.out_queue.put([host,chunk])

               #signals to queue job is done
               #self.queue.task_done()
           except Exception,e:
               writeindex=file('GrabErro.txt','a+')
               writeindex.write(str(e)+'\n')
               writeindex.write('the length of queue is: '+str(self.queue.qsize())+'\n')
               writeindex.write('the length of out queue is: '+str(self.out_queue.qsize())+'\n')
               writeindex.close()
               continue

class DatamineThread(threading.Thread):
    def __init__(self,queue,out_queue,dbname):
        threading.Thread.__init__(self)
        self.out_queue = out_queue
        self.queue = queue
        self.dbname = dbname

    
    def run(self):
        crawler1=crawler(self.dbname,self.getName())
        while True: 
           try:
               chunkUrl = self.out_queue.get()
	       print 'QUEUE_____SIZE:::*********************'+str(self.queue.qsize())
               chunk=chunkUrl[1]
               page=chunkUrl[0]
           #   print 'addtoindex::'+page
               #parse the chunk
   #            print self.getName()+'Beginning Souping'+page
               soup=BeautifulSoup(chunk,fromEncoding='GBK')
	       if 'list'in page:
	   	   crawler1.insertTextInfo(soup,page)
      #Not every topic page existed in the source ,and some made by programm   
	       if 'list'in page and '_'not in page:
	          numberString=str(soup.find('div',{"class":"pager"}))
		  NumOfPageTemps=numberString.split(',')
		  NumOfPage1=NumOfPageTemps[-2]
		  NumOfPage2=NumOfPageTemps[-3]
	          NumberOfPage=string.atoi(NumOfPage2)/string.atoi(NumOfPage1)
		  for num1 in range(2,int((NumberOfPage+1)/20)):
	              Plink=page[0:-5]+'_%i.html'%num1
		      if not crawler1.isindexed(Plink):
		        # print 'inserted Plink'+Plink
		         self.queue.put(Plink)
               crawler1.dbcommit()
               #signals to queue job is done
               #self.out_queue.task_done()
	   except Exception,e:
               writeindex2=file('DatamineErro.txt','a+')
               writeindex2.write(str(e)+'\n')
               writeindex2.close()
               continue    


###################################################################################
class crawler:
 
  def __init__(self ,dbname,name):
   ##  self.conn=sqlite.connect(dbname)
     self.conn=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=crawl;UID=crawl2;PWD=crawl")
     self.cursor=self.conn.cursor()
    ## self.cursor.execute('ALTER DATABASE DEFAULT CHARACTER SET GBK')
    ## self.cursor.execute('set names GBK')
     self.name=name
     



  def __del__(self):
     self.cursor.close()

  def dbcommit(self):
     self.conn.commit()
      

  def isindexed(self,url):
     self.cursor.execute("select rowid from urllist where url='%s'" % url)
     u=self.cursor.fetchone()
     if u!=None:
         return True
     return False

  def insertTextInfo(self,soup,url):
     links=soup.findAll('div',{"class":"articleh"})
     print url
     for link in links:
       try:
	 contents=link.findAll('span')
	 replyNum=string.atoi(contents[1].string)
	 accessNum=string.atoi(contents[0].string)
	 pubDatestring=(contents[-1].string)
	 subjectString=(link.find('a').string).encode('GBK')
	 tempValue=contents[2].find('a')["href"]
         url='http://guba.eastmoney.com/'+tempValue
	 try:
           texttime=time.strptime(str(pubDatestring[0:2]+pubDatestring[3:5]),"%m%d")
           Nowtime=time.strftime("%Y%m%d",time.localtime())
           systime=time.strptime(Nowtime[4:],"%m%d")
           if texttime>systime:
	    pubDate=str(string.atoi(Nowtime[0:4])-1)+str(pubDatestring[0:2]+pubDatestring[3:5])
           else:
	    pubDate=str(string.atoi(Nowtime[0:4]))+str(pubDatestring[0:2]+pubDatestring[3:5])
#         print pubDate
	 #topicid=str(tempValue[5:11])
	 except Exception,e:
	    print e
	    pubDate='20120229'
	 topicid=str(tempValue.split(',')[1])
         #id=string.atoi(str(tempValue[5:11])+str(tempValue[12:-5]))
	 
         id=str(tempValue.split(',')[1])+str(tempValue.split(',')[2].replace(".html",''))
	 
         emotionValue=0
         self.cursor.execute("select * from text where id = '%s' "%id)
	 u=self.cursor.fetchone()
	 if u==None:
	   if(string.atoi(pubDate)>20130602):
             self.cursor.execute("insert into text values('%s','%s','%s','%s','%d','%d','%s','0','%d')"%(id,url,pubDate,topicid,replyNum,accessNum,subjectString,emotionValue))
	 #print 'insert '+url+'into database'
         self.conn.commit()
       except Exception,e:
	  continue

    



def main():
    #spawn a pool of threads.
    for i in range(5):
        t = ThreadUrl(queue, out_queue)
        t.setDaemon(True)
        t.start()


    #populate queue with data
    for host in hosts:
        queue.put(host)


    for i in range(100):
        dt = DatamineThread(queue,out_queue,'crawl')
        dt.setDaemon(True)
        dt.start()


    #wait on the queue until everything has been processed
    queue.join()
    out_queue.join()


main()


