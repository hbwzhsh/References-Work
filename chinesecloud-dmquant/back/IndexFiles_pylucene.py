#!/usr/bin/env python
# -*- coding: utf-8 -*-

###
# Usage: python IndexFiles_pylucene.py <doc_directory> <index_directory>
# Example: python IndexFiles_pylucene.py D:\workspace\PyluceneSample\SrcTxt_pylucene D:\workspace\PyluceneSample\Index_pylucene
###
import sys, os, lucene, threading, time, string
from datetime import datetime

"""
This class is loosely based on the Lucene (java implementation) demo class 
org.apache.lucene.demo.IndexFiles.  It will take a directory as an argument
and will index all of the files in that directory and downward recursively.
It will index on the file path, the file name and the file contents.  The
resulting Lucene index will be placed in the current directory and called
'index'.
"""

class Ticker(object): # 类Ticker继承类object 

    def __init__(self): # 魔法方法 构造函数__init__, 会根据情况被python调用，不需要直接调用；self相当于this指针
        self.tick = True

    def run(self):
        while self.tick:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1.0)

class IndexFiles(object):
    """Usage: python IndexFiles_pylucene <doc_directory> <index_directory>"""

    def __init__(self, root, storeDir, analyzer, startDate, endDate):

        if not os.path.exists(storeDir):
            os.mkdir(storeDir)
        store = lucene.SimpleFSDirectory(lucene.File(storeDir))
        # 创建IndexWriter对象,第一个参数是Directory,第二个是分词器,
        # 第三个表示是否是创建,如果为false为在此基础上面修改,
        # 第四表示表示分词的最大值，比如说new MaxFieldLength(2)，就表示两个字一分，一般用IndexWriter.MaxFieldLength.LIMITED
        writer = lucene.IndexWriter(store, analyzer, False,
                                    lucene.IndexWriter.MaxFieldLength.LIMITED)
        writer.setMaxFieldLength(1048576)
        self.indexDocs(root, writer, startDate, endDate)
        ticker = Ticker()
        print 'optimizing index',
        threading.Thread(target=ticker.run).start()
        writer.optimize()
        writer.close()
        ticker.tick = False
        print 'done'

    def indexDocs(self, root, writer, startDate, endDate):
        doc_num = 0
	docindex_num = 0
	for root, dirnames, filenames in os.walk(root):
            for filename in filenames:
                doc_num += 1
		if doc_num % 1000 == 0:
		    print "Index Searched " + str(doc_num) + " files..."
		if not filename.endswith('.txt'):
                    continue
	        filedate = datetime.strptime(filename[:8], '%Y%m%d')
		if not (filedate >= startDate and filedate <= endDate):
		    continue
                #print "adding", filename
		docindex_num += 1
                try:
                    path = os.path.join(root, filename)
                    file = open(path)
                    contents = unicode(file.read(), 'utf8')
                    file.close()
		    #替换逗号，句号为空格，并以空格为分割符切割句子
		    #print "contents:" + conteits.encode('utf8')
		    contents = contents.replace('\n','')
		    contents = contents.replace(unicode("。", 'utf8'),'###')
		    sentence_num = 0
		    for sentence in contents.split('###'):
			#print "sentence:" + sentence.encode("gbk",'ignore')
			#time.sleep(1)
                    	doc = lucene.Document()
                    	doc.add(lucene.Field("name", filename,
                                         lucene.Field.Store.YES,
                                         lucene.Field.Index.NOT_ANALYZED))
                    	doc.add(lucene.Field("path", path,
                                         lucene.Field.Store.YES,
                                         lucene.Field.Index.NOT_ANALYZED))
			doc.add(lucene.Field("sentence_num", str(sentence_num),
                                         lucene.Field.Store.YES,
                                         lucene.Field.Index.NOT_ANALYZED))
			if len(sentence) > 0:
                        	doc.add(lucene.Field("sentence", sentence,
                                             lucene.Field.Store.YES,
                                             lucene.Field.Index.ANALYZED))
                    	else:
                        	print "warning: no content in sentence %d of file %s" % sentence_num,filename
                    	writer.addDocument(doc)
			sentence_num += 1
                except Exception, e:
                    #print "Failed in indexDocs:", e
		    error = 1
        print "Index has Added " + str(docindex_num) + " files..."

if __name__ == '__main__': # 即可以作为主程序运行，也可以作为模块导入
    if len(sys.argv) < 5:
        print IndexFiles.__doc__
        sys.exit(1)

    startDate = datetime.strptime(sys.argv[3], '%Y%m%d')
    endDate = datetime.strptime(sys.argv[4], '%Y%m%d')

    lucene.initVM() # 初始化java虚拟机
    print 'lucene', lucene.VERSION
    start = datetime.now()
    try:
        IndexFiles(sys.argv[1], sys.argv[2], lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT), startDate, endDate)
        end = datetime.now()
        print end - start
    except Exception, e:
        print "Failed: ", e

#    os.system("pause")
