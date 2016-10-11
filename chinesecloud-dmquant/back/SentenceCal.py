#!/usr/bin/env python
# -*- coding: utf-8 -*-

##########################
# Fuction: 
# Author: Daoming, Wujing, Luojian
# Date: 2012-07-23
##########################

import sys, os, string, time
from ctypes import *
import datetime


############################ Function Define ###########################
def Printsentence(sentence, filepath, startDate, endDate):
  filew = open(filepath, 'a')
  for datecount in range(len(sentence)):
    today = startDate + datetime.timedelta(days=datecount)
    todaystr = today.strftime('%Y%m%d')
    filew.write(todaystr)
    filew.write(" ### ")
    filew.write(str(sentence[datecount]))
    filew.write("\n")
  filew.close()

def run(root, startDate, endDate):
  daynum = (endDate - startDate).days + 1
  sentence = [0 for x in range(daynum)]
  
#------------- Sentence Calculate -------------------
  doc_num = 0
  docindex_num = 0
  for root, dirnames, filenames in os.walk(root):
    for filename in filenames:
      doc_num += 1
      if doc_num % 1000 == 0:
        print "Sentence Calculated " + str(doc_num) + " files..."
      if not filename.endswith('.txt'):
        continue
      filedate = datetime.datetime.strptime(filename[:8], '%Y%m%d')
      if not (filedate >= startDate and filedate <= endDate):
	continue
      docindex_num += 1
      path = os.path.join(root, filename)
      filew = open(path, 'r')        
#-------- Read Divided File & Sentence Count----------
      try:
	fileDate = datetime.datetime.strptime(filename[:8], '%Y%m%d')
#	print str(fileDate)
	dayindex = (fileDate - startDate).days
	contents = unicode(filew.read(), 'utf8')
        filew.close()
	contents = contents.replace('\n','')
	contents = contents.replace(unicode("。", 'utf8'),'###')
#	print str(len(contents.split('###')))
	sentence[dayindex] = sentence[dayindex] + len(contents.split('###'))
      except Exception,e:
	continue
  print "Sentence Cal Done..."
  print "Sentence Added " + str(docindex_num) + " files..."

  return sentence


########################### Main Function ##############################
if __name__ == '__main__':
  if len(sys.argv) < 4:
    print "Usage: <source_data>"
#------------- Variable Define -----------------
  sourcedata = sys.argv[1]
  startDate = datetime.datetime.strptime(sys.argv[2], '%Y%m%d')
  endDate = datetime.datetime.strptime(sys.argv[3], '%Y%m%d')

  if sourcedata == "研究报告".decode('utf8').encode('gbk'):
    root = "D:\\DATA\\Lucene\\MBStrategy"
    filepath = "D:\\ICTCLAS\\sentence_MBStrategy.txt"
  if sourcedata == "股票论坛".decode('utf8').encode('gbk'):
    root = "D:\\DATA\\Lucene\\text"
    filepath = "D:\\ICTCLAS\\sentence_text.txt"
  if sourcedata == "个股新闻".decode('utf8').encode('gbk'):
    root = "D:\\DATA\\Lucene\\sinaStockNews"
    filepath = "D:\\ICTCLAS\\sentence_sinaStockNews.txt"

  sentence = []
  sentence = run(root, startDate, endDate)

  Printsentence(sentence, filepath, startDate, endDate)
#------------------- END -----------------------
#  os.system("pause")

