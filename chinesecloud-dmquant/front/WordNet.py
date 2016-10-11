#!/usr/bin/env python
# -*- coding: utf-8 -*-

##########################
# Fuction: 
# Author: Daoming, Wujing, Luojian
# Date: 2012-07-21
##########################

import sys, os, string, time, collections
import codecs
from ctypes import *
from datetime import datetime
sys.path.append("D:\\TotalCode\\LuceneCode\\Index_Search")
import SearchFiles_pylucene
sys.path.append("D:\\TotalCode\\LuceneCode\\WordNet")
import WordDoc
from lucene import \
    QueryParser, IndexSearcher, StandardAnalyzer, SimpleFSDirectory, File, StandardAnalyzer, \
    VERSION, initVM, Version

############################ Function Define ###########################
def PrintWordNet(wordNet, input):
  print "################## Print Word Net For String: " + input + " ##################"
  for wordLev1 in wordNet:
    print "***** " + (wordLev1[0]).decode('utf8').encode('gbk') + "[" + str(wordLev1[1]) + "]: *****"
    for wordLev2 in wordNet[wordLev1]:
      print (wordLev2[0]).decode('utf8').encode('gbk'),
    print

def LayoutWordNet(wordNet, input, sourcedata):
  input = input.replace(' ','')
  vnafile = "D:\\TotalCode\\LuceneCode\\WordNet\\Result\\Layout_" + input + "_" + sourcedata + ".vna"
  cmdfile = "D:\\TotalCode\\LuceneCode\\WordNet\\Result\\Layout_" + input + "_" + sourcedata + ".txt"
  jpgfile = "D:\\TotalCode\\LuceneCode\\WordNet\\Result\\Layout_" + input + "_" + sourcedata + ".jpg"

  filewvna = open(vnafile,'w')
  filewvna.write(codecs.BOM_UTF8)
  filewvna.write("*Tie data\n")
  filewvna.write("from to strength\n")
  wordlist = []
  for wordLev1 in wordNet:
    if wordLev1[0] == input.decode('gbk').encode('utf8'):
      continue
    filewvna.write(input.decode('gbk').encode('utf8'))
    filewvna.write(" ")
    filewvna.write(wordLev1[0])
    filewvna.write(" ")
    filewvna.write(str(int(wordLev1[1])))
    filewvna.write("\n")
    wordlist[len(wordlist):len(wordlist)] = [wordLev1[0]]
  for wordLev1 in wordNet:
    for wordLev2 in wordNet[wordLev1]:
      if wordLev2[0] == input.decode('gbk').encode('utf8') or wordLev2[0] in wordlist:
        continue
      filewvna.write(wordLev1[0])
      filewvna.write(" ")
      filewvna.write(wordLev2[0])
      filewvna.write(" ")
      filewvna.write(str(int(wordLev2[1])))
      filewvna.write("\n")
  filewvna.write("*node properties\n")
  filewvna.write("Id color shape size labelsize labelcolor\n")
  filewvna.write(input.decode('gbk').encode('utf8'))
  filewvna.write(" 65535 1 12 12 0\n")
  for wordLev1 in wordNet:
    if wordLev1[0] == input.decode('gbk').encode('utf8'):
      continue
    filewvna.write(wordLev1[0])
    filewvna.write(" 112214218 2 9 9 0\n")
  for wordLev1 in wordNet:
    for wordLev2 in wordNet[wordLev1]:
      if wordLev2[0] == input.decode('gbk').encode('utf8') or wordLev2[0] in wordlist:
        continue 
      filewvna.write(wordLev2[0])
      filewvna.write(" 16776960 3 8 8 0\n")
  filewvna.close()
  
  filewcmd = open(cmdfile,'w')
  filewcmd.write("loadvna " + vnafile + "\n")
  filewcmd.write("runlayout\n")
  filewcmd.write("savejpg " + jpgfile + "\n")
  filewcmd.write("close\n")
  filewcmd.close()

  command = "netdraw.exe batch " + cmdfile
  os.system(command)

########################### Main Function ##############################
if __name__ == '__main__':
  if len(sys.argv) < 5:
    print "Usage: <source_data> <search_string> <startDate> <endDate>"
#------------- Variable Define -----------------
  sourcedata = sys.argv[1]
  input = sys.argv[2]
  startDate = datetime.strptime(sys.argv[3], '%Y%m%d')
  endDate = datetime.strptime(sys.argv[4], '%Y%m%d')
  filepath = "D:\\TotalCode\\LuceneCode\\Index_Search\\Output_pylucene.txt"

  if sourcedata == "研究报告".decode('utf8').encode('gbk'):
    STORE_DIR = "D:\\DATA\\Index\\MBStrategy"
    idfpath = "D:\\ICTCLAS\\wordIDF_MBStrategy.txt"
    totalfile = 659796
    stockcodeflag = 1
  if sourcedata == "股票论坛".decode('utf8').encode('gbk'):
    STORE_DIR = "D:\\DATA\\Index\\text"
    idfpath = "D:\\ICTCLAS\\wordIDF_text.txt"
    totalfile = 1487094
    stockcodeflag = 1
  if sourcedata == "个股新闻".decode('utf8').encode('gbk'):
    STORE_DIR = "D:\\DATA\\Index\\sinaStockNews"
    idfpath = "D:\\ICTCLAS\\wordIDF_sinaStockNews.txt"  
    totalfile = 441061
    stockcodeflag = 0

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
  wordcount = divide.ICTCLAS_ImportUserDictFile('D:\\ICTCLAS\\userdict.txt',"CODE_TYPE_UTF8")
  print "Divde Word Init Done..."
  print

#------------- Net Word Generate -----------------
  SearchFiles_pylucene.run(searcher, analyzer, input, filepath) 
  topWordLev1 = [] # top 20 word
  topWordLev2 = []
  wordNet = collections.OrderedDict()
  topWordLev1 = WordDoc.run(divide, filepath, startDate, endDate, input, idfpath, totalfile, 20, stockcodeflag)
  print "***************** Lev1 Word Net " + input + " Generated *********************"
#  os.system("pause")
  print

  for wordLev1 in topWordLev1:
    SearchFiles_pylucene.run(searcher, analyzer, wordLev1[0].decode('utf8').encode('gbk'), filepath)
    topWordLev2 = WordDoc.run(divide, filepath, startDate, endDate, wordLev1[0].decode('utf8').encode('gbk'), idfpath, totalfile, 5, stockcodeflag)
    wordNet[wordLev1] = topWordLev2
    print "------------- Lev2 Word Net " + wordLev1[0].decode('utf8').encode('gbk') + "[" + str(wordLev1[1]) + "] Generated -------------"
    print
#  os.system("pause")
  print

#------------- Net Word Print -----------------
  PrintWordNet(wordNet, input)
  LayoutWordNet(wordNet, input, sourcedata)
  
#------------------- END -----------------------
  searcher.close()
  os.system("pause")

