#!/usr/bin/env python
# -*- coding: utf-8 -*-

##########################
# Fuction: 
# Author: Daoming, Wujing, Luojian
# Date: 2012-07-19
##########################

import sys, os, string, time, math
from ctypes import *
from datetime import datetime


############################ Function Define ###########################
## divide one line ##
## return divided string ##
def Divline(divide, line):
  srclen = len(c_char_p(line).value)
  dstp = c_buffer(srclen*6)
  divide.ICTCLAS_ParagraphProcess(c_char_p(line), c_int(srclen), dstp, 'CODE_TYPE_UTF8', 0)
  dst = dstp.value
#  print divline.decode("utf8").encode("gbk")
#  os.system("pause")
  return dst

def PrintWord(input, topWord):
  print "***** " + input + ": *****"
  for word in topWord:
    print (word[0]).decode('utf8').encode('gbk') + "[" + str(word[1]) + "]"
  print

def run(divide, filepath, startDate, endDate, input, idfpath, totalfile, wordnum, stockcodeflag):
#------------- UserDict Init -------------------
  wordDic = {}
  userdictPath = "D:\\ICTCLAS\\userdict.txt"
  uncheckPath = "D:\\ICTCLAS\\uncheck.txt"
  userdictFile = open(userdictPath, 'r')
  uncheckFile = open(uncheckPath, 'r')
  for userdictLine in userdictFile:
    userdictLine = userdictLine.replace('\n','').strip()
    if userdictLine != '' and userdictLine != input.decode('gbk').encode('utf8'):
      wordDic[userdictLine] = 0
  print "Userdict word amount: " + str(len(wordDic))
  userdictFile.close()
  for uncheckLine in uncheckFile:
    uncheckLine = uncheckLine.replace('\n','').strip()
    try:
      del wordDic[uncheckLine]
    except Exception, e:
      continue
  print "After Uncheck word amount: " + str(len(wordDic))
  uncheckFile.close()
  #os.system("pause")

#-------- Read OutputFile & Word Count----------
  filew = open(filepath, 'r')
  totalword = 0
  for line in filew:
    linedate = datetime.strptime(line[:8], '%Y%m%d')
    #datetime filter#
    if linedate >= startDate and linedate <= endDate:
      try:
        #print linedate
        dst = Divline(divide, line[line.find("\t")+1:])
        #print dst.decode("utf8").encode("gbk")
        for word in dst.strip().split(" "):
	  #print word.decode("utf8").encode("gbk") + "..."
	  totalword += 1
	  word = word.strip()
	  #print str(totalword)
	  #print word.decode('utf8').encode('gbk')
	  try:
	    wordDic[word] = wordDic[word] + 1
	  except Exception, e:
            continue
      except Exception, e:
	error = 1
  print str(totalword),
  print " Word Count Done..."
  filew.close()
  #os.system("pause")

#-------- Read OutputFile & Add Stock Code To wordDic ----------
  filecname = open("D:\\ICTCLAS\\cname_20120718.txt", 'r')
  stockName = {}
  for line in filecname:
    stockName[line[:6]] = line[7:].strip()
  filecname.close()
  ## merge stockcode count to stockname
  for code in stockName:
    try:
      name = stockName[code]
      wordDic[name] = wordDic[name] + wordDic[code]
      wordDic[code] = 0
    except Exception,e:
      wordDic[code] = 0
      continue
  ## Add filename's stockcode to wordDic
  if stockcodeflag == 1:
    filew = open(filepath, 'r')
    for line in filew:
      linedate = datetime.strptime(line[:8], '%Y%m%d')
      if linedate >= startDate and linedate <= endDate:
        lineStockCode = line[9:15]
        try:
          word = stockName[lineStockCode]
          wordDic[word] = wordDic[word] + 1
	  totalword += 1
#         print word.decode('utf8').encode('gbk')
        except Exception, e:
          continue
    filew.close()
    print "Add Stock Code To wordDic Done..."

#-------- Read IDF File & Calculate tf-idf ----------
  wordDicUserdict = {}
  idffilew = open(idfpath, 'r')
  for idfline in idffilew:
    idfline = idfline.replace('\n','').strip()
    word = idfline.split("###")[0].strip()
    #print word.decode('utf8').encode('gbk')
    idfcount = idfline.split("###")[1].strip()
    #print idfcount
    try:
      if int(idfcount) == 0:
        wordDicUserdict[word] = 0
      else:
	wordDicUserdict[word] = 100000*float(wordDic[word])/float(totalword)*math.log(float(totalfile)/float(idfcount),10)
	#wordDic[word] = float(wordDic[word])/float(idfcount)
    except Exception, e:
      continue
    #print wordDic[word]
    #os.system("pause")

#--------------- Get top 20 Word ---------------
  topWord = sorted(wordDicUserdict.iteritems(), key=lambda d:d[1], reverse=True)
  topWord = topWord[:wordnum]
  topWord = filter(lambda d: int(d[1])>0, topWord)
  PrintWord(input, topWord)

  return topWord


########################### Main Function ##############################
if __name__ == '__main__':
#------------- Variable Define -----------------
  filepath = "D:\\TotalCode\\LuceneCode\\Index_Search\\Output_pylucene.txt"
  idfpath = "D:\\ICTCLAS\\wordIDF_sinaStockNews.txt"
#  filepathdiv = "D:\\TotalCode\\PyluceneSample\\Output_pylucene_div.txt"
  startDate = datetime.strptime("20090101", '%Y%m%d')
  endDate = datetime.strptime("20091231", '%Y%m%d')
  input = "银行".decode('utf8').encode('gbk')
  totalfile = 441061 

#------------- Divde Word Init -----------------
  divide = cdll.LoadLibrary("D:\\ICTCLAS\\ICTCLAS50.dll")
  IfInit = divide.ICTCLAS_Init(c_char_p("D:\\ICTCLAS"))
  wordcount = divide.ICTCLAS_ImportUserDictFile('D:\\ICTCLAS\\userdict.txt',"CODE_TYPE_UTF8")
  print "Divde Word Init Done..."
  
  topWord = [] # top 20 word
  topWord = run(divide, filepath, startDate, endDate, input, idfpath, totalfile, 15, 1)
#------------------- END -----------------------
  os.system("pause")
