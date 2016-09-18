import pyodbc
import sys
import os
from ctypes import *
import datetime

startDatestr = sys.argv[1]
endDatestr = sys.argv[2]
startDate = datetime.datetime.strptime(startDatestr, '%Y%m%d')
endDate = datetime.datetime.strptime(endDatestr, '%Y%m%d')

divide = cdll.LoadLibrary("D:\\ICTCLAS\\ICTCLAS50.dll")
IfInit = divide.ICTCLAS_Init(c_char_p("D:\\ICTCLAS"))
print IfInit
wordcount = divide.ICTCLAS_ImportUserDictFile('D:\\ICTCLAS\\userdict.txt',"CODE_TYPE_UTF8")
print wordcount
print "Word Done"

conn=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=crawl_pylucene;UID=crawl2;PWD=crawl")
conn1=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=crawl;UID=crawl2;PWD=crawl")
cur=conn.cursor()
cur1=conn1.cursor()
cur1.execute("select * from [crawl].[dbo].[text] where pub_date >= '" + startDatestr + "' and pub_date <= '" + endDatestr + "'")
while 1:
   print "Get 5000 Datas"	
   All=cur1.fetchmany(50000)
   if len(All)==0:
	   break		   
   for one in All:
	id=one[0]
	url=one[1]
	print url
	pubDate=one[2]
	topicid=one[3]
	replyNum=one[4]
	accessNum=one[5]
	subjectString=one[6]
	emotionValue=one[7]
	cur.execute("select * from text where id='%s'" % id)
	IfExist=cur.fetchone()
	if IfExist==None:
	       try:
	       # Lucene Version
	                filedir=str(pubDate)[0:10].replace('-','')
			if(os.path.isdir("D:\\DATA\\text\\" +filedir)):             
	       	 		filepath = "D:\\DATA\\text\\" +filedir+"\\"+ str(pubDate)[0:10].replace('-','')+ "_" + str(topicid) + ".txt"
			else:
			        os.makedirs("D:\\DATA\\text\\" +filedir)
			filew = open(filepath,'a')
			filew.write(subjectString.decode('gb2312','ignore').encode('utf8'))
			filew.write('\n')
			filew.close()
			if(os.path.isdir("D:\\DATA\\Lucene\\text\\" +filedir)):
				print "divide begin!"
				print divide.ICTCLAS_FileProcess(filepath,filepath[0:8]+'Lucene\\'+filepath[8:],'CODE_TYPE_UTF8',0)
				print "divide end"
			else:
				os.makedirs("D:\\DATA\\Lucene\\text\\" +filedir)
			cur.execute("insert into text values('%s','%s','%s','%s','%d','%d','%s','0','%d')"%(id,url,pubDate,topicid,replyNum,accessNum,filepath,emotionValue))
	     		conn.commit()
			print 'insert '+url+'into database'
	       except Exception,e:
	      	        print e
	      		continue

