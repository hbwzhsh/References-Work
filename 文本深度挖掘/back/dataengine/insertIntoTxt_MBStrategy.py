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
cur1.execute("select * from [crawl].[dbo].[MBStrategy] where time >= " + startDatestr + ' and time <= ' + endDatestr)
while 1:
   print "Get 5000 Datas"	
   All=cur1.fetchmany(5000)
   if len(All)==0:
	   break		   
   for one in All:
	page=one[0]
	print page
	Title=one[1]
	TotalAbstract=one[2]
	TotalAbstract=TotalAbstract.replace('www.microbell.com\xe3\x80\x90\xe8\xbf\x88\xe5\x8d\x9a\xe6\xb1\x87\xe9\x87\x91\xe3\x80\x91</a></font>','')
	TotalAbstract=TotalAbstract.replace('www.microbell.cn\xef\xbc\x88\xe8\xbf\x88\xe5\x8d\x9a\xe6\xb1\x87\xe9\x87\x91\xef\xbc\x89</a></font>','')
	time=one[3]
	reportAuthor=one[4]
	stockCode=one[5]
	reportBelongCompany=one[6]
	rateLevel=one[7]
	reportType=one[8]
	cur.execute("select * from MBStrategy where url='%s'" % page)
	IfExist=cur.fetchone()
	if IfExist==None:
	       try:
	       # Lucene Version
			filepath = "D:\\DATA\\MBStrategy\\" + str(time) + "_" + str(stockCode) + "_" + str(page[35:-5]) +".txt"
			#filepath = "D:\\DATA\\MBStrategy\\bbbb.txt"
			filew = open(filepath,'w')
			try:
			   filew.write(Title.decode('gbk').encode('utf-8'))
			except:
			   filew.write(Title.decode('utf-8').encode('utf-8'))
			filew.write('\n')
			try:
			   filew.write(TotalAbstract.decode('utf-8').encode('utf-8'))
                        except:
			   filew.write(TotalAbstract.decode('gbk').encode('utf-8'))
			filew.write('\n')
			filew.close()
			print "divide begin!"
			if os.path.isfile(filepath):
				print divide.ICTCLAS_FileProcess(filepath,filepath[0:8]+'Lucene\\'+filepath[8:],'CODE_TYPE_UTF8',0)
			else:
				print "filepath does not exist!"
			print "divide end"
			cur.execute("insert into MBStrategy values('%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(page,Title,filepath,time,reportAuthor,stockCode,reportBelongCompany,rateLevel,reportType))
                	conn.commit()
			print "insert into database..."
	       except Exception,e:
	      	        print e
	      		continue

