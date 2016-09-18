#coding=GBK
import Queue
import threading
#from pymmseg import mmseg    ##load seperateword tool
#mmseg.dict_load_defaults()   ##load seperateword default dictionary
#mmseg.dict_load_words('stockname.dic') ##load special stockname and stock numberdictionary  
import urllib2
import time
import re
from BeautifulSoup import *
from urlparse import urljoin
import pyodbc 
#import MySQLdb
import string
import sys
reload(sys)
sys.setdefaultencoding('gbk')





originalURL="http://money.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php?symbol=sh600418&Page=1"
 

queue = Queue.Queue()
out_queue = Queue.Queue()

hosts=[]

	
conn1=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=feptrade;UID=crawl2;PWD=crawl")
cur=conn1.cursor()
cur.execute("select code,market from eb_stock_main where class like 'A%' and closedate is Null order by code")
result=cur.fetchall()
stockCodes=[]
for i in range(len(result)):
    if result[i][1]=='SZ'.encode('GBK'):
       stockCodes.append('sz'+result[i][0])
    if result[i][1]=='SH'.encode('GBK'):
       stockCodes.append('sh'+result[i][0])

for stock_code in stockCodes:
	oriURL=originalURL[:-15]+stock_code+originalURL[-7:]
	print oriURL
	#time.sleep(1)
	hosts.append([oriURL,stock_code[2:]])
	
	

class ThreadUrl(threading.Thread):
    def __init__(self, queue, out_queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.out_queue = out_queue
        

    def run(self):
        while True:
           try:
               #grabs host from queue
               hostchunk = self.queue.get()
	       host=hostchunk[0]
               queuestock_code=hostchunk[1]
               #grabs urls of hosts and then grabs chunk of webpage
            #   print self.getName()+'Now Grabbing'+host
            
               url = urllib2.urlopen(host)
             #  print self.getName()+'END___Grabbing'+host
               chunk = url.read()

               #place chunk into out queue
               self.out_queue.put([host,chunk,queuestock_code])

               #signals to queue job is done
               #self.queue.task_done()
           except Exception,e:
               #writeindex=file('GrabErro.txt','a+')
       	       print 'There is Problem@@@@@@@@@@@@@@@@@@@GRABErro'
              # writeindex.write(str(e)+'\n')
              # writeindex.close()
	       if 'HTTP Error' in str(e):
		   time.sleep(10)
		   filer=file('stock_code.txt','r')
                   stock_codes=filer.readlines()
		   for stock_code in stock_codes:
	              oriURL=host[:71]+stock_code[:-1]+host[79:]
	              hosts.append([oriURL,stock_code[:-1]])
               continue

class DatamineThread(threading.Thread):
    def __init__(self,queue,out_queue):
        threading.Thread.__init__(self)
        self.out_queue = out_queue
        self.queue = queue

    
    def run(self):
        crawler1=crawler(self.getName())
        while True: 
           try:
               chunkUrl = self.out_queue.get()
	      # print 'QUEUE_____SIZE:::*********************'+str(self.out_queue.qsize())
               chunk=chunkUrl[1]
               page=chunkUrl[0]
	       tempstock_code=chunkUrl[2]
           #   print 'addtoindex::'+page
               #parse the chunk
	       if 'Page' in page:
                  print self.getName()+'Beginning Souping'+page
               soup=BeautifulSoup(chunk,fromEncoding='GBK')
               crawler1.addtoindex(page,tempstock_code)
######################新浪中有些页面是跳转到另一个页面的,下面有进行相应的处理###
               if 'AllNews'not in page:
		   InCaseTransLink=soup.find('meta',{"http-equiv":"Refresh"})
		   if InCaseTransLink==None:
	             
	   	      crawler1.insertTextInfo(soup,page,tempstock_code)
		   else:
		      transLink=InCaseTransLink['content'][6:]
		      if not crawler1.isindexed(transLink,tempstock_code):
		         self.queue.put([transLink,tempstock_code])
	       if 'AllNews' in page:
		   soup1=soup.find('table',{"class":"table2"})
                   if soup1!=None:
		    links=soup1.findAll('a',href=True)
		    indexFlag=0
		    EarlyindexFlag=0
                    for link in links :
                       link=link["href"]
		       if 'Early' in page: 
			       if not crawler1.isInserted(link,page[78:84]):
				  if not crawler1.isindexed(link,page[78:84]):
				   #if EarlyindexFlag==0:
				      self.queue.put([link,page[78:84]])
			       else:
				   EarlyindexFlag=1
                       else:
			       if not crawler1.isInserted(link,page[73:79]):
				  if not crawler1.isindexed(link,page[73:79]):
				  # if indexFlag==0:
				      self.queue.put([link,page[73:79]])
			       else:
				    indexFlag=1

               #print self.getName()+'Ending Souping'+page
	       ##因为在新浪个股早期资讯中,下一页的链接指向错误,因此写了下面的代码
	       if 'Early' in page:
		   lastlink=links[-1]["href"]
		   if 'News' in lastlink :
			if 'Early' not in lastlink:
		           newlastlink=lastlink[:59]+'Early'+lastlink[59:]
			   if not crawler1.isindexed(newlastlink,page[78:84]):
				   self.queue.put([newlastlink,page[78:84]])
               crawler1.dbcommit()     
           except Exception,e:
               writeindex2=file('DatamineErro.txt','a+')
  	       print 'There is Problem@@@@@@@@@@@@@@@@@@@DATAMINEErro'
               writeindex2.write(str(e)+'\n')
               writeindex2.close()
               continue    


###################################################################################
class crawler:
 
  def __init__(self, name):
   ##  self.conn=sqlite.connect(dbname)
     self.conn=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=crawl;UID=crawl2;PWD=crawl")
    # self.conn=MySQLdb.connect(host='localhost',user='root',passwd='233218')
    # self.conn.select_db(dbname)
     self.cursor=self.conn.cursor()
    # self.cursor.execute('ALTER DATABASE DEFAULT CHARACTER SET GBK')
    # self.cursor.execute('set names GBK')
     self.name=name
     



  def __del__(self):
     self.cursor.close()

  def dbcommit(self):
     self.conn.commit()

  def getentryid(self ,table,field1,value1,field2,value2,createnew=True):
     self.cursor.execute("select rowid from %s where %s='%s' and %s='%s'"%(table,field1,value1,field2,value2))
     res=self.cursor.fetchone()
 #    if table=='urllist':
 #        print self.name+table+'*****the rowid******'+str(res)
     if res==None:
        #I have changed the cursor.py  line 127  charset 
###################sql server的写法##################################	  
         self.cursor.execute("insert into %s  values('%s','%s')"%(table,value1,value2))
         self.conn.commit()
         self.cursor.execute("select max(rowid) from %s"%(table))
         LastRowID=self.cursor.fetchone()[0]
######################################################################

         return LastRowID
     else:
         return res[0]
  
  def addtoindex(self,url,stockcode):
     if (self.isindexed(url,stockcode)==False):
         urlid=self.getentryid('sinaNewsUrllist','url',url,'stockcode',stockcode)
 
  def gettextonly(self,soup):
     v=soup.string
     if v==None:
        c=soup.contents
        resulttext=''
        for t in c :
            subtext=self.gettextonly(t)
            resulttext+=subtext
        return resulttext
     else:
        return v.strip() 

  def getContentsText(self,soup,url):
     if url[26:30]=='look':
        content=soup.find('div',{"class":"huifu"})	
        resultContenttext=self.gettextonly(content)
        return resultContenttext

  def getTitleText(self,soup,url):
     if url[26:30]=='look':
        content1=soup.find('div',{"class":"biaoti"})	
        resultTitletext=self.gettextonly(content1)
        return resultTitletext
      
  def separatewords(self,text):
     utf8text=text.encode('utf-8')     #because mmseg can only deal with utf-8
     algor=mmseg.Algorithm(utf8text)
     resulttext=[]
     for tok in algor:
	resulttext.append(tok.text.decode('utf-8').encode('GBK'))      
     return resulttext

  def isindexed(self,url,stockcode):
     self.cursor.execute("select rowid from sinaNewsUrllist where url='%s' and stockcode='%s'" % (url,stockcode))
     u=self.cursor.fetchone()
     if u!=None:
         return True
     return False


  def isInserted(self,url,stockcode):
     self.cursor.execute("select * from sinaStockNews where url='%s' and stockcode='%s'" % (url,stockcode))
     u=self.cursor.fetchone()
     if u!=None:
         return True
     return False

  def insertTextInfo(self,soup,url,stock_code):
     # print '@@@@@@@@@@@@@@@@@@@@@@@@'
      tempsoup=soup.find('div',{"class":"blkContainerSblk"})
      title=tempsoup.find('h1').string.encode('GBK')
      timestring=soup.find('span',{"id":"pub_date"}).string
      time=timestring[0:4]+timestring[5:7]+timestring[8:10]
      print time	  
      if tempsoup != None:
         texts=tempsoup.findAll('p')
	 if len(texts)>0:
            resultText=''
            for text in texts:
	    ##有些是没有用的代码的片段,过滤掉#
	       tmptext=self.gettextonly(text)
	       if 'document.getElementById("artibodyTitle").innerHTML'not in tmptext:
	           resultText=resultText+tmptext+'\n'
      resultText.replace("'"," ")
      self.cursor.execute("insert into sinaStockNews values('%s','%s','%s','%s','%s')"%(url,stock_code,str(time),title,resultText.decode('GBK').encode('GBK')))
             # writeindex.write(resultText.decode('GBK').encode('utf-8'))
                 



  def insertWordInfo(self,soup,url):
     id=string.atoi(url[31:37]+url[38:-5])
     self.cursor.execute("select * from wordlocation where textid='%d'"%(id))
     u=self.cursor.fetchone()
     if u==None:
         contentsText=str(self.getContentsText(soup,url))
         titleText=str(self.getTitleText(soup,url))
         contentsWords=self.separatewords(contentsText)
         titleWords=self.separatewords(titleText)
         for i in range(len(contentsWords)):
             word=contentsWords[i]
             self.cursor.execute("insert into wordlocation(textid,word,location) values('%d','%s','contents')"%(id,word))
         for i in range(len(titleWords)):
             word=titleWords[i]
             self.cursor.execute("insert into wordlocation(textid,word,location) values('%d','%s','title')"%(id,word))
     self.conn.commit()
  def createindextables(self):
    self.cursor.execute('create database gubaSearch character set gbk')
    self.cursor.execute('create table urllist(rowid int identity(1,1) ,url varchar(100))default charset=gbk')
    self.cursor.execute('create table wordlocation(textid bigint,word varchar(100) ,location varchar(100))default charset=gbk')
    self.cursor.execute('create index urlidx on urllist(url)')
    self.cursor.execute('create index textidx on wordlocation(textid)')
    self.cursor.execute("create table text(id bigint Primary key,url varchar(100),pub_date date,topic_id int,reply_num int,access_num int ,subject varchar(100))default charset=gbk")
    self.cursor.execute('create index idx on text(id)')
    self.cursor.execute('create index pubdatex on text(pub_date)')


    self.conn.commit()
    



def main():
    #spawn a pool of threads.
    for i in range(15):
        t = ThreadUrl(queue, out_queue)
        t.setDaemon(True)
        t.start()


    #populate queue with data
    for host in hosts:
        queue.put(host)


    for i in range(40):
        dt = DatamineThread(queue,out_queue)
        dt.setDaemon(True)
        dt.start()
        print 'bbbb'


    #wait on the queue until everything has been processed
    queue.join()
    out_queue.join()


main()


