from HTMLParser import HTMLParser
import logging
from threading import Timer
from operator import itemgetter
import pickle

def sort_dic(d,reverse=False):
	return sorted(d.iteritems(), key=itemgetter(1), reverse=reverse)

class MLStripper(HTMLParser):
	def __init__(self):
		self.reset()
		self.fed = []
 	def handle_data(self, d):
		self.fed.append(d)
	def get_data(self):
		return ''.join(self.fed)
								
def strip_tags(html):
	s = MLStripper()      
	s.feed(html)
	return s.get_data()

class LineLogger():
	def __init__(self,name="",interval=10000,msg="lines loaded"):
		self.cnt =0
		self.interval = interval
		self.msg = msg
		if len(name) >0:
	   		name = name.split('/')
			name = name[-1]
		self.name=name
		logging.info(name+" begins")
	def end(self):
		logging.info(self.name+" ends")
	def inc(self):
		self.cnt += 1
		if self.cnt % self.interval == 0 :
			logging.info(self.name+' '+str(self.cnt) + " " + self.msg)


class DataLoader() :
        def __init__(self,pass_head_line=0) :
                self.filename = '/dev/null'
                self.dump_path = '/dev/null'
                self.data = []
                self.pass_head_line = pass_head_line
                self.data_source = None
        def load_data(self) :
                try:
                        logging.info('loading data from :' + self.dump_path )
                        fin = open(self.dump_path)
                        self.data = pickle.load(fin)
                except :
                        logging.info('load data fail! try to construct from :' + self.filename )
                        self.data_source = self.load_data_source()
                        logger = LineLogger(name = self.filename.split('/')[-1])
                        self.preprocess()
                        for line in self.data_source :
                                self.deal_line(line)
                                logger.inc()
                        self.postprocess()
                        logger.end()
                        with open(self.dump_path,'w') as fout :
                                logging.info('saving to dump file : '+self.dump_path)
                                pickle.dump(self.data,fout)
                logging.info('load data finished!')
                return self.data
        def load_data_source(self) :
                fin = open(self.filename)                                                                                                   
                for i in range(0,self.pass_head_line) :                                                                                     
                        fin.readline()                                                                                                      
                return fin                                                                                                                  
        def preprocess(self)  :                                                                                                             
                pass                                                                                                                        
        def deal_line(self,line)  :                                                                                                         
                pass                                                                                                                        
        def postprocess(self) :                                                                                                             
                pass


