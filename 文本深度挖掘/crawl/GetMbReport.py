#coding=GBK
import Queue
import threading
import urllib2
import time
import re
from BeautifulSoup import *
from urlparse import urljoin
import pyodbc
from PAM30 import PAMIE
import Queue
import string
import sys
reload(sys)
sys.setdefaultencoding('utf-8')




hosts = Queue.Queue()

pages = Queue.Queue()

class ThreadUrl(threading.Thread):
    def __init__(self, hosts, pages):
        threading.Thread.__init__(self)
        self.hosts = hosts
        self.conn=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=crawl;UID=crawl2;PWD=crawl")
        self.cursor=self.conn.cursor()
	
        self.pages = pages
        

    def run(self):
       	ie=PAMIE()
	urlre=re.compile('docdetail_.*html')
        while True:
           try:
	       host=hosts.get()
	       try:
	          #url=urllib2.urlopen(host)
		  ie.navigate(host)
		  chunk=ie.getPageText()
	       except:
		  try:     
		     ie.navigate(host)
		     chunk=ie.getPageText()
		  except:
                     ie.navigate(host)
		     chunk=ie.getPageText()
               print 'souping+'+host+'\n'
               links=urlre.findall(chunk)
	       for link in links:
		       totallink='http://www.microbell.com/'+link
		       if not self.isindexed(totallink):
			        pages.put(totallink)

		       
               #soup=BeautifulSoup(chunk,fromEncoding='GBK')
               #areas=soup.findAll('div',{"class":"classbaogao_sousuo_new"})
	       #print '##########################'
	       #for area in areas:
	       #  link=area.find('a',href=True)['href']
	       #   totallink='http://www.microbell.com/'+link
	#	  if not self.isindexed(totallink):
	#              pages.put(totallink)
	#       print 'END***************'     
           except:
               writeindex=file('hostsErro.txt','a+')
               writeindex.write(host+'\n')
               writeindex.close()
               continue
        ie.quit()
    def isindexed(self,url):
        self.cursor.execute("select * from MBStrategy where url='%s'" % url)
        u=self.cursor.fetchone()
        if u!=None:
            return True
        return False
     

class ThreadUrlSecond(threading.Thread):
    def __init__(self, hosts, pages):
        threading.Thread.__init__(self)
        self.hosts = hosts
        self.pages = pages
        self.conn=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=crawl;UID=crawl2;PWD=crawl")
        self.cursor=self.conn.cursor()
	
        

    def run(self):
       	ie1=PAMIE()
        while True: 
	   
           try:
	       
               TotalAbstract=''
	       page=pages.get()
	       try:
	          ie1.navigate(page)
		  chunk=ie1.getPageText()
	       except:
		  try:     
		     ie1.navigate(page)
		     chunk=ie1.getPageText()
		  except:
                     ie1.navigate(page)
		     chunk=ie1.getPageText()
               print 'souping+'+page+'\n'
               
               soup=BeautifulSoup(chunk,fromEncoding='GBK')
	       soup1=soup.find('div',{"class":"baogaonews"})
	       chunks=soup1.findAll('td')
               stockName=chunks[1].find('b').string  #股票名称
	       if stockName==None:
		       stockName=str(stockName)
	       print stockName+'\n'
               reportAuthor=chunks[3].find('b').string #研究报告作者
	       if reportAuthor==None:
		       reportAuthor=str(reportAuthor)
               print reportAuthor+'\n'
	       stockCode=chunks[6].find('b').string #股票代码
	       if stockCode==None:
			       stockCode=str(stockCode)
               print stockCode+'\n'
	       reportBelongCompany=chunks[8].find('b').string #研究报告出处
	       if reportBelongCompany==None:
		       reportBelongCompany=str(reportBelongCompany)
               print reportBelongCompany+'\n'
	       rateLevel=chunks[11].find('b').string #推荐评级	
	       if rateLevel==None:
		       rateLevel=str(rateLevel)
               print rateLevel+'\n'
	       reportType=chunks[13].find('b').string  #研究报告栏目
	       if reportType==None:
		       reportType=str(reportType)
	       print reportType+'\n'
	       Title=soup.find('strong').string
               print Title+'\n'
	       time=chunks[18].find('b').string
               tmptimes=time.split(' ')
               time=tmptimes[0]
	       timechunks=time.split('-')
               if len(timechunks)==3:
                  if len(timechunks[1])==1:
	             timechunks[1]='0'+timechunks[1]
                  if len(timechunks[2])==1:
	             timechunks[2]='0'+timechunks[2]
               time=timechunks[0]+timechunks[1]+timechunks[2]
	       Title=Title
               ContentAbstractArea=soup.find('div',{"class":"new_content"})
               if ContentAbstractArea !=None:
                   tmp=ContentAbstractArea.find('td')
                   tmpStr=str(tmp)
                   abstract=tmpStr[34:-12].replace('<font style="DISPLAY:none;"><a href="http://www.microbell.com/">www.microbell.com</a>(\xe8\xbf\x88\xe5\x8d\x9a\xe6\xb1\x87\xe9\x87\x91)\xe3\x80\x82</font>','\n')  
	           abstract=abstract.replace('&nbsp;','')
		   abstract=abstract.replace('<br />','')
	           abstract=abstract.replace('\n','')
                   TotalAbstract=TotalAbstract+abstract+'\n'
	       ##INSERT THE INFO TYPE:GBK
	       print '#################################'
	       try:
                  self.cursor.execute("insert into MBStrategy values('%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(page,Title,TotalAbstract.decode('utf-8'),time,reportAuthor,stockCode,reportBelongCompany,rateLevel,reportType))
                  self.conn.commit()
	       except Exception,e:
		       print e
	          
               print '************************************' 
           except:
               writeindex=file('pagesErro.txt','a+')
               writeindex.write(page+'\n')
               writeindex.close()
               continue
        ie1.quit()



def main():
    #oriUrl='http://www.microbell.com/result.asp?lm=0&area=DocTitle&timess=13&key=&page=1'
    oriUrl='http://www.hibor.com.cn/result.asp?lm=0&area=DocTitle&timess=13&key=&dtype=&page=1'

    #spawn a pool of threads.
    for i in range(1):
        t = ThreadUrl(hosts, pages)
        t.setDaemon(True)
        t.start()




    for i in range(1,80):
        hosts.put(oriUrl[:-1]+str(i))

    for i in range(1):
        dt = ThreadUrlSecond(hosts,pages)
        dt.setDaemon(True)
        dt.start()
        print 'bbbb'


    #wait on the queue until everything has been processed
    hosts.join()
    pages.join()


main()
filr2.close()
filr1.close()

     
	     
