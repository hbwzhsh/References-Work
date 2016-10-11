#!/usr/bin/env python
# -*- coding: utf-8 -*-

###
# Usage: <index_directory> <search_string> <output_directory>
# Example: python SearchFiles_pylucene.py D:\DATA\Index\MBStrategy "经济 AND 放缓,回落,衰退,下降,向下" D:\TotalCode\PyluceneSample\Output_pylucene.txt
###

import os, sys, string, time
from lucene import \
    QueryParser, IndexSearcher, StandardAnalyzer, SimpleFSDirectory, File, StandardAnalyzer, \
    VERSION, initVM, Version


"""
This script is loosely based on the Lucene (java implementation) demo class 
org.apache.lucene.demo.SearchFiles.  It will prompt for a search query, then it
will search the Lucene index in the current directory called 'index' for the
search query entered against the 'contents' field.  It will then display the
'path' and 'name' fields for each of the hits it finds in the index.  Note that
search.close() is currently commented out because it causes a stack overflow in
some cases.
"""
def convert(input):
        comma = ","#"，"
# Wujin Query
        if len(input.split('AND'))==2 and comma in input:
            word1 = input.split('AND')[0].split(comma)
            word2 = input.split('AND')[1].split(comma)
            command = "("
            for i in word1:
                command += word2query(i) + " "
            command +=") AND ("
            for i in word2:
                if i[0]==' ': i=i[1:]
                command += word2query(i) + " "
            command +=")"
        else: 
             command = ""
# Lucene Query
             state = 0
             countc= 0
             for i in input:
                if i<='z':
                   if state == 0:
                      command += i
                   else:
                      command += "\"" +i
                      countc +=1
                      state = 0
                else:
                   if state == 0:
                      command += "\"" + i
                      countc += 1
                      state = 2
                   elif state == 3:
                      command += i+" "
                      state = 1
                   else:
                      command += i
                      state += 1
             if countc%2==1: command += "\""
        return command


def word2query(input):
    output ="\""
    wordLength = len(input)
    for i in range(0,wordLength/3):
        output += input[i*3:i*3+3]
        output += " "
    output+="\""
    return output

def run(searcher, analyzer, input, filepath):
        #input = raw_input("Query:").decode('gbk').encode('utf8')
	#print "Search for: " + input
	command = convert(input.decode('gbk').encode('utf8'))
	print "Search for:" + command.decode('utf8').encode('gbk')
        qp = QueryParser(Version.LUCENE_CURRENT, "sentence", analyzer)
        #qp.setPhraseSlop(0)
        query = qp.parse(command)
        scoreDocs = searcher.search(query, 1000000).scoreDocs
        print "%s total matching documents." % len(scoreDocs)
        print
        
	try:
		#filepath = "D:\\TotalCode\\PyluceneSample\\Output_pylucene.txt"
		filew = open(filepath, 'w')
		result_num = 0
        	for scoreDoc in scoreDocs:
			try:
				result_num += 1
				if result_num % 1000 == 0:
				#	time.sleep(5)
		    			print "Search added " + str(result_num) + " sentences..."
			#print 'scoreDoc.doc:', scoreDoc.doc
            			doc = searcher.doc(scoreDoc.doc)
	    			path = doc.get("path")
				#print "path:" +  path
			#print 'name:', doc.get("name")
	    		#print 'sentence_num:', str(doc.get("sentence_num"))
	    		#print 'sentence:', doc.get("sentence")
				#sentence = GetSentence(doc.get("sentence_num"), path)
				sentence = doc.get("sentence")
	    			#print 'sentence:', sentence
				OutputSentence(filew, doc.get("name"), sentence)
			except:
				continue
		filew.close()
	except: #Exception, e:
		print "Failed in Outputsentence:"#, e
  #  while True:
        #print
        #print "Hit enter with no input to quit."
        
      
#        command = raw_input("Query:")
#        if command == '':
#            return

'''
        command = "(\"经济\") AND (\"放缓\" OR \"回落\"  OR \"衰退\" OR \"下降\" OR \"向下\")"
        #command1 = "\"经济\""
	#command2 = "\"放缓\""
	#command3 = "\"回落\""
	#command = "(" + command1 + ") AND (" + command2 + " OR " + command3 + ")"       
        print
        print "Searching for:", command.decode("utf-8").encode("gbk")
'''


#从原始文本中获得包含关键字的句子
def GetSentence(sentence_num, path):
	try:
                file = open(path)
                contents = unicode(file.read(), 'utf8')
                file.close()
		#替换逗号，句号为空格，并以空格为分割符切割句子
		#print "contents:" + conteits.encode('utf8')
		contents = contents.replace(unicode("，", 'utf8')," ")
		contents = contents.replace(unicode("。", 'utf8')," ")
		sentence = contents.split()[string.atoi(sentence_num)]
		return sentence
	except Exception, e:
		print "Failed in Getsentence:", e
		return null

def OutputSentence(filew, name, sentence):
	filew.write(name[:-4].encode("utf8") + '\t')
	filew.write(sentence.encode("utf8") + '\n')


########################### Main Function ##############################
if __name__ == '__main__':
    #STORE_DIR = "D:\\TotalCode\\PyluceneSample\\Index_pylucene"
    if len(sys.argv) < 4:
	    print "Usage: <index_directory> <search_string> <output_directory>"
#------------- Variable Define -----------------
    STORE_DIR = sys.argv[1]
    input = sys.argv[2]
    filepath = sys.argv[3]

    initVM(maxheap='512m')
    print 'lucene', VERSION
    directory = SimpleFSDirectory(File(STORE_DIR))
    searcher = IndexSearcher(directory, True)
    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)
    run(searcher, analyzer, input, filepath)
    searcher.close()   
#------------------- END -----------------------
#    os.system("pause")
