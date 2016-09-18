#!/usr/bin/env python
# -*- coding: utf-8 -*-

##########################
# Fuction: 
# Author: Daoming, Wujing, Luojian
# Date: 2012-07-23
##########################

import sys, os, string, time
from ctypes import *
from datetime import datetime


############################ Function Define ###########################
def ReadwordIDF(filepath):
  wordIDF_read = {}
  idffilew = open(filepath, 'r')
  for idfline in idffilew:
    idfline = idfline.replace('\n','').strip()
    word = idfline.split("###")[0].strip()
    #print word.decode('utf8').encode('gbk')
    idfcount = idfline.split("###")[1].strip()
    #print idfcount
    try:
      wordIDF_read[word] = int(idfcount)
    except Exception, e:
      continue
  idffilew.close()
  return wordIDF_read

def PrintwordIDF(wordIDF, filepath):
  filew = open(filepath, 'w')
  for word in wordIDF:
    filew.write(word)
    filew.write(" ### ")
    filew.write(str(wordIDF[word]))
    filew.write("\n")
  filew.close()

def run(root, filepath, startDate, endDate):
#------------- UserDict Init -------------------
  wordIDF_read = ReadwordIDF(filepath)
  wordIDF = {}
  userdictPath = "D:\\ICTCLAS\userdict.txt"
  uncheckPath = "D:\\ICTCLAS\uncheck.txt"
  userdictFile = open(userdictPath, 'r')
  uncheckFile = open(uncheckPath, 'r')
  for userdictLine in userdictFile:
    userdictLine = userdictLine.replace('\n','').strip()
    if userdictLine != '':
      try:
	wordIDF[userdictLine] = wordIDF_read[userdictLine]
      except Exception,e:
	wordIDF[userdictLine] = 0
  print "Userdict word amount: " + str(len(wordIDF))
  userdictFile.close()
  for uncheckLine in uncheckFile:
    uncheckLine = uncheckLine.replace('\n','').strip()
    try:
      del wordIDF[uncheckLine]
    except Exception, e:
      continue
  print "After Uncheck word amount: " + str(len(wordIDF))
  uncheckFile.close()
  #os.system("pause")

#------------- WordIDF Calculate -------------------
  doc_num = 0
  docindex_num = 0
  for root, dirnames, filenames in os.walk(root):
    for filename in filenames:
      doc_num += 1
      if doc_num % 1000 == 0:
        print "Word IDF Calculated " + str(doc_num) + " files..."
      if not filename.endswith('.txt'):
        continue
      filedate = datetime.strptime(filename[:8], '%Y%m%d')
      if not (filedate >= startDate and filedate <= endDate):
        continue
      docindex_num += 1
      path = os.path.join(root, filename)
#      print path
      filew = open(path, 'r')        
#-------- Read Divided File & WordIDF Count----------
      for line in filew:
	for word in line.strip().split(" "):
	  word = word.strip()
	  try:
	    wordIDF[word] = wordIDF[word] + 1
          except Exception, e:
            continue
      filew.close()
  print "WordIDF Done..."
  print "Word IDF Added " + str(docindex_num) + " files..."
  
  return wordIDF


########################### Main Function ##############################
if __name__ == '__main__':
  if len(sys.argv) < 4:
    print "Usage: <source_data>"
#------------- Variable Define -----------------
  sourcedata = sys.argv[1]

  startDate = datetime.strptime(sys.argv[2], '%Y%m%d')
  endDate = datetime.strptime(sys.argv[3], '%Y%m%d')

  if sourcedata == "研究报告".decode('utf8').encode('gbk'):
    root = "D:\\DATA\\Lucene\\MBStrategy"
    filepath = "D:\\ICTCLAS\\wordIDFWord_MBStrategy.txt"
  if sourcedata == "股票论坛".decode('utf8').encode('gbk'):
    root = "D:\\DATA\\Lucene\\text"
    filepath = "D:\\ICTCLAS\\wordIDFWord_text.txt"
  if sourcedata == "个股新闻".decode('utf8').encode('gbk'):
    root = "D:\\DATA\\Lucene\\sinaStockNews"
    filepath = "D:\\ICTCLAS\\wordIDFWord_sinaStockNews.txt"

  wordIDF = {}
  wordIDF = run(root, filepath, startDate, endDate)

  PrintwordIDF(wordIDF, filepath)
#------------------- END -----------------------
#  os.system("pause")

