#!/usr/bin/env python
# -*- coding: utf-8 -*-

##########################
# Fuction: 
# Author: Daoming, Wujing, Luojian
# Date: 2012-09-07
##########################

import time
import pyodbc
import os
import string
import sys
import re
import collections
from PAM30 import PAMIE
from BeautifulSoup import *
import codecs
from ctypes import *
import datetime
sys.path.append("D:\\TotalCode\\LuceneCode\\Index_Search")
import SearchFiles_pylucene
sys.path.append("D:\\TotalCode\\LuceneCode\\WordNet")
import WordDoc_stock
import WordDoc
sys.path.append("D:\\TotalCode\\LuceneCode\\RollNews")
import SendMail
sys.path.append("D:\\TotalCode\\LuceneCode\\WordSeq")
import sigWordSeq
import smtplib
from email.mime.text import MIMEText
from lucene import \
    QueryParser, IndexSearcher, StandardAnalyzer, SimpleFSDirectory, File, StandardAnalyzer, \
    VERSION, initVM, Version

############################ Function Define ###########################
def LayoutWordNet(topWordLev1, input, sourcedata):
  vnafile = "D:\\TotalCode\\LuceneCode\\WordNet\\Result_stock\\Layout_" + input + "_" + sourcedata + ".vna"
  cmdfile = "D:\\TotalCode\\LuceneCode\\WordNet\\Result_stock\\Layout_" + input + "_" + sourcedata + ".txt"
  jpgfile = "D:\\TotalCode\\LuceneCode\\WordNet\\Result_stock\\Layout_" + input + "_" + sourcedata + ".jpg"

  filewvna = open(vnafile,'w')
  filewvna.write(codecs.BOM_UTF8)
  filewvna.write("*Tie data\n")
  filewvna.write("from to strength\n")
  for wordLev1 in topWordLev1:
    if wordLev1[0] == input.decode('gbk').encode('utf8'):
      continue
    filewvna.write("\""+input.decode('gbk').encode('utf8')+"\"")
    filewvna.write(" ")
    filewvna.write(wordLev1[0])
    filewvna.write(" ")
    filewvna.write(str(int(wordLev1[1])))
    filewvna.write("\n")
  
  filewvna.write("*node properties\n")
  filewvna.write("Id color shape size labelsize labelcolor\n")
  filewvna.write("\""+input.decode('gbk').encode('utf8')+"\"")
  filewvna.write(" 65535 1 12 12 0\n")
  for wordLev1 in topWordLev1:
    if wordLev1[0] == input.decode('gbk').encode('utf8'):
      continue
    filewvna.write(wordLev1[0])
    filewvna.write(" 112214218 2 9 9 0\n")
  filewvna.close()
  
  filewcmd = open(cmdfile,'w')
  filewcmd.write("loadvna " + vnafile + "\n")
  filewcmd.write("runlayout\n")
  filewcmd.write("savejpg " + jpgfile + "\n")
  filewcmd.write("close\n")
  filewcmd.close()

  command = "netdraw.exe batch " + cmdfile
  os.system(command)

  return jpgfile

def Divline(divide, line):
  srclen = len(c_char_p(line).value)
  dstp = c_buffer(srclen*6)
  divide.ICTCLAS_ParagraphProcess(c_char_p(line), c_int(srclen), dstp, 'CODE_TYPE_UTF8', 0)
  dst = dstp.value
#  print divline.decode("utf8").encode("gbk")
#  os.system("pause")
  return dst

def Title2Input(title, divide):
  wordDic = {}
  userdictPath = "D:\\ICTCLAS\\userdict.txt"
  uncheckPath = "D:\\ICTCLAS\\uncheck.txt"
  userdictFile = open(userdictPath, 'r')
  uncheckFile = open(uncheckPath, 'r')
  for userdictLine in userdictFile:
    userdictLine = userdictLine.replace('\n','').strip()
    if userdictLine != '':
      wordDic[userdictLine] = 0
#  print "Userdict word amount: " + str(len(wordDic))
  userdictFile.close()
  for uncheckLine in uncheckFile:
    uncheckLine = uncheckLine.replace('\n','').strip()
    try:
      del wordDic[uncheckLine]
    except Exception, e:
      continue
#  print "After Uncheck word amount: " + str(len(wordDic))
  uncheckFile.close()
  
  resultinput=''
  #print "title" + title
  titleDiv = Divline(divide, title).strip()
  #print "titleDiv:" + titleDiv
  for word in titleDiv.split(' '):
    word = word.strip()
    if word in wordDic:
      resultinput = resultinput + word + " "

  resultinput = resultinput.strip().replace(' ',' AND ')

  return resultinput



########################### Main Function ##############################
if __name__ == '__main__':
  if len(sys.argv) < 2:
    print "Usage: <source_data>"

#------------- Variable Define -----------------
#  url = "http://www.xinhuanet.com/fortune/gd.htm"
  url = "http://roll.finance.sina.com.cn/s/channel.php?ch=03#col=43&spec=&type=&ch=03&k=&offset_page=0&offset_num=0&num=60&asc=&page=1"
#  url = "http://roll.finance.sina.com.cn/s/channel.php?ch=03#col=43&spec=&type=&ch=03&k=&offset_page=0&offset_num=0&num=60&asc=&page=1"
  sourcedata = sys.argv[1]
  filepath = "D:\\TotalCode\\LuceneCode\\Index_Search\\Output_pylucene.txt"

  conn=pyodbc.connect("DRIVER={SQL Server};SERVER=localhost,9944;DATABASE=crawl_pylucene;UID=crawl2;PWD=crawl")
  cur=conn.cursor()

  if sourcedata == "研究报告".decode('utf8').encode('gbk'):
      STORE_DIR = "D:\\DATA\\Index\\MBStrategy"
      idfpath = "D:\\ICTCLAS\\wordIDF_MBStrategy.txt"
      totalfile = 659796
      stockcodeflag = 1
      sentencefile = "D:\\ICTCLAS\\sentence_MBStrategy.txt"
  if sourcedata == "股票论坛".decode('utf8').encode('gbk'):
      STORE_DIR = "D:\\DATA\\Index\\text"
      idfpath = "D:\\ICTCLAS\\wordIDF_text.txt"
      totalfile = 1487094
      stockcodeflag = 1
      sentencefile = "D:\\ICTCLAS\\sentence_text.txt"
  if sourcedata == "个股新闻".decode('utf8').encode('gbk'):
      STORE_DIR = "D:\\DATA\\Index\\sinaStockNews"
      idfpath = "D:\\ICTCLAS\\wordIDF_sinaStockNews.txt"  
      totalfile = 441061
      stockcodeflag = 0
      sentencefile = "D:\\ICTCLAS\\sentence_sinaStockNews.txt"

#------------- Lucene Init -----------------
  initVM(maxheap='512m')
  print 'lucene', VERSION
  directory = SimpleFSDirectory(File(STORE_DIR))
  searcher = IndexSearcher(directory, True)
  analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
  print "Lucene Search Init Done..."

#------------- Divde Word Init -----------------
  divide = cdll.LoadLibrary("D:\\ICTCLAS\\ICTCLAS50.dll")
  IfInit = divide.ICTCLAS_Init(c_char_p("D:\\ICTCLAS"))
  print IfInit
  wordcount = divide.ICTCLAS_ImportUserDictFile('D:\\ICTCLAS\\userdict.txt',"CODE_TYPE_UTF8")
  print "Divde Word Init Done..."
  #print

  i=0
  while(i==0):
    i+=1	  	
    recentWeekfile = "D:\\TotalCode\\LuceneCode\\GetData\\RecentWeek.txt"
    recentWeekfilew = open(recentWeekfile, 'r')
    lastFridaystr = recentWeekfilew.readline().split(' ')[1]
    endDate = datetime.datetime.strptime(lastFridaystr, '%Y%m%d')

#    endDate = datetime.datetime.today()
#修改时间
#个股挖掘
    startDate = endDate - datetime.timedelta(days=30*3)
#概念关注图    
    startDate1 = endDate - datetime.timedelta(days=365*2)
#    endDate = datetime.datetime.strptime('20120907', '%Y%m%d')
#    startDate = datetime.datetime.strptime('20100901', '%Y%m%d')

##    ie = PAMIE()
#------------------- Get Rolling News Page --------------------#
##    try:
      
##      ie.navigate(url)
##      chunk = ie.getPageText()
#  print chunk.encode('gbk', 'ignore')
#  ie.quit()

#------------------- Soup News Page --------------------#
##      newsTitle = []
##      newsHref = []
##      soup = BeautifulSoup(chunk, fromEncoding='gbk')
##      soupTitle = soup.findAll('span', {"class":"c_tit"})
##      for title in soupTitle:
##        newsTitle.append(title.find('a').string)
##        newsHref.append(title.find('a')['href'])

##    except Exception,e:
#3      continue
    newsTitle = []
    newsHref = []
    newsTitle.append(u'上海自贸区29日挂牌部分概念股遭国资减持')  # 输入需要搜索的
    newsHref.append('www.baidu.com')
    #------------------- WordNet_stock --------------------#
    for count in range(len(newsTitle)):
#      print newsHref[count]
      print "newsTitle; " + newsTitle[count].encode('gbk')
      try:
        input = Title2Input(newsTitle[count].encode('utf8'), divide)
        input = input.decode('utf8').encode('gbk')
	print "input: " + input 

#------------------- insert database --------------------#
      #try:
        urlnews = str(newsHref[count])
        title = str(newsTitle[count].encode('gbk'))
        newstime = datetime.datetime.today().strftime('%Y%m%d')
        #cur.execute("insert into RollNews values('%s','%s','%s')"%(urlnews, title, newstime))
        print "insert " + urlnews + " into database..."
        conn.commit()
      except Exception,e:
        print e
	#continue

      print newsTitle[count].encode('gbk')

#------------- Net Word Generate -----------------
      if input != '':
        SearchFiles_pylucene.run(searcher, analyzer, input, filepath) 
        topWordLev1 = [] # top 20 word
        topWordLev1 = WordDoc_stock.run(divide, filepath, startDate, endDate, input, idfpath, totalfile, 30, stockcodeflag)
        #topWordLev1 = WordDoc.run(divide, filepath, startDate, endDate, input, idfpath, totalfile, 30, stockcodeflag)
        print "***************** Lev1 Word Net " + input + " Generated *********************"
#        os.system("pause")
#        print

        filecname = open("D:\\ICTCLAS\\cname_20120718.txt", 'r')
        stockName = {}
        for line in filecname:
          stockName[line[7:].strip()] = line[:6]
        filecname.close()

        if len(topWordLev1) != 0:
	  outputfile = "D:\\TotalCode\\LuceneCode\\WordNet\\WordNet_stock\\"+sourcedata+"_"+input+"_"+startDate.strftime('%Y%m%d')+"-"+endDate.strftime('%Y%m%d')+".txt"
          filew = open(outputfile, 'w')
          for wordLev1 in topWordLev1:
#            print wordLev1[0].decode('utf8').encode('gbk')
            try:
              filew.write(input.decode('gbk').encode('utf8'))
              filew.write(' ### ')
              filew.write(wordLev1[0])
#            filew.write(' ### ')
#            filew.write(stockName[wordLev1[0]])
              filew.write(' ### ')
              filew.write(str(wordLev1[1]))
              filew.write('\n')
            except Exception,e:
              continue
          filew.close()

          jpgfile = LayoutWordNet(topWordLev1, input, sourcedata)

#------------- Word Seq -----------------
	 
	  filerName2Code=file('StockName2Code.txt','r')
          chunkName2Code=filerName2Code.readlines()
	  Name2Code={}
	  for onechunkName2Code in chunkName2Code:
		Name2Code[onechunkName2Code.split('\t')[0]]=onechunkName2Code.split('\t')[1].replace('\n','')
	  
	  print topWordLev1[0][0].decode('utf8')
          print onechunkName2Code.split('\t')[0]		
	  ticker=Name2Code[topWordLev1[0][0].decode('utf8').encode('gbk')].replace('SZ','sz').replace('SH','ss')
	  print ticker.replace('SZ','sz').replace('SH','ss')
	  #ticker = '399005.sz'
          #ticker='000906.ss'
	  try:
	  	sigwordSeq = sigWordSeq.run(filepath, startDate1, endDate)
#	  	PrintwordSeq(sigwordSeq, outfile, startDate, endDate)
	  	sentenceSeq = sigWordSeq.runsentenceSeq(sentencefile,startDate1,endDate)
  	  	wordSeqpic = sigWordSeq.PlotwordSeq(sigwordSeq,startDate1,endDate,input,ticker,sourcedata,sentenceSeq)
	  except:
		sigwordSeq = sigWordSeq.run(filepath, startDate1, endDate) 
		sentenceSeq = sigWordSeq.runsentenceSeq(sentencefile,startDate1,endDate)
		wordSeqpic = sigWordSeq.PlotwordSeq(sigwordSeq,startDate1,endDate,input,ticker,sourcedata,sentenceSeq)
		continue
#------------- Send Email -----------------
#          MAIL_LIST = ["jeremy_luojian@163.com"]
#          MAIL_LIST = ["jeremy_luojian@163.com", "wujing@ebscn.com", "liudaoming@ebscn.com"]
          MAIL_LIST = ["fengj@ebscn.com","wujing@ebscn.com","zhangsihui@ebscn.com"]
          MAIL_HOST = "mail.ebscn.com"
          MAIL_USER = "wujing"
          MAIL_PASS = "233218wjtc"
          MAIL_POSTFIX = "ebscn.com"
          MAIL_FROM = MAIL_USER + "<"+MAIL_USER + "@" + MAIL_POSTFIX + ">"

	  contents = "新闻内容: \t" + title.decode('gbk').encode('utf8') + "\n"
	  contents = contents + "新闻链接: \t" + urlnews + "\n"
	  contents = contents + "搜索关键词: \t" + input.decode('gbk').encode('utf8') + "\n\n"
	  contents = contents + "关联股票: \t"
          for wordLev1 in topWordLev1:
            contents = contents + str(wordLev1[0]) + " "
	    if topWordLev1.index(wordLev1) == 9 and len(topWordLev1) != 10:
	      contents = contents + "\n\t\t"
	  contents = contents + "\n"

	  title=u'[光大新闻分析引擎]'.encode('utf8')+title.decode('gbk').encode('utf8')
	  if SendMail.send_mail(title, contents ,[wordSeqpic,jpgfile], MAIL_LIST, MAIL_HOST, MAIL_USER, MAIL_PASS, MAIL_POSTFIX, MAIL_FROM):
            print "发送成功...".decode('utf8').encode('gbk')
          else:
            print "发送失败...".decode('utf8').encode('gbk')

#------------------- End --------------------#
    #ie.quit()  
    time.sleep(5)
  
#    os.system('pause')
