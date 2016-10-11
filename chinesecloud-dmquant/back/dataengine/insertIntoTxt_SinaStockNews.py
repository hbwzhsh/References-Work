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
cur1.execute("select * from [crawl].[dbo].[sinaStockNews] where pubdate >= " + startDatestr + ' and pubdate <= ' + endDatestr)
while 1:
   print "Get 5000 Datas"	
   All=cur1.fetchmany(5000)
   if len(All)==0:
	   break		   
   for one in All:
	url = one[0]
	stock_code = one[1]
	time = one[2]
	title = one[3]
	resultText = one[4]
	id = one[5]
	cur.execute("select * from sinaStockNews where url='%s' and stockcode='%s'" % (url,stock_code))
	IfExist=cur.fetchone()
	if IfExist==None:
	       try:
	       # Lucene Version
	         news_id = url.split('/')[-1][:-6]
      	         filepath = "D:\\DATA\\sinaStockNews\\" + str(time) + "_" + str(stock_code) + "_" + str(news_id) +".txt"
      	         filew = open(filepath,'w')
      	         filew.write(str(title).decode('GBK','ignore').encode('UTF8'))
      	         filew.write('\n')
      	         filew.write(resultText.decode('GBK','ignore').encode('UTF8'))
      	         filew.write('\n')
      	         filew.close()
      	         #print "title:" + title
      	         #print "resultText:" + resultText.decode('GBK').encode('GBK')
      	         print "divide begin!"
		 print divide.ICTCLAS_FileProcess(filepath,filepath[0:8]+'Lucene\\'+filepath[8:],'CODE_TYPE_UTF8',0)
		 print "divide end"
      	         cur.execute("insert into sinaStockNews values('%s','%s','%s','%s','%s')"%(url,stock_code,str(time),title,filepath))
      	         # writeindex.write(resultText.decode('GBK').encode('utf-8'))
      	         conn.commit()
		 print "insert into database..."
	       except Exception,e:
	      	        print e
	      		continue

