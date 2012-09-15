import logging
from tools import *
from conf import *
import csv
from operator import itemgetter
import time

logging.basicConfig(level=logging.NOTSET,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')

DUMP_PATH_BASE = TMP_DIR + '/pydumps/'
#DUMP_PATH_BASE = TMP_DIR + '/jsons/'

class RawuidUid(DataLoader):
	def __init__(self):
		DataLoader.__init__(self,1)
		self.dump_path = DUMP_PATH_BASE + 'rawuid_uid.pydump'
		self.filename = DATA_DIR + 'users.tsv'
		self.data = {}
	def preprocess(self):
		self.cnt = 0
	def deal_line(self,line):				
		sp = line.split('\t')
		uid = int(sp[0])
		if not uid in self.data :
			self.data[uid] = self.cnt
			self.cnt +=1

def load_rawuid_id():
	''' Output {raw_userid : uid } '''
	return RawuidUid().load_data();

class RawjidJid(DataLoader):
	def __init__(self):
		DataLoader.__init__(self,1)
		self.dump_path = DUMP_PATH_BASE + 'rawjid_jid.pydump'
		self.filename = DATA_DIR + 'jobs.tsv'
		self.data = {}
	def preprocess(self):
		self.cnt = 0
	def deal_line(self,line):				
		sp = line.split('\t')
		jid = int(sp[0])
		if not jid in self.data :
			self.data[jid] = self.cnt
			self.cnt +=1

def load_rawjid_id():
	''' Output {raw_userid : uid } '''
	return RawjidJid().load_data();


def get_time_struct(line):
	return time.strptime(line[:line.find('.')],"%Y-%m-%d %H:%M:%S")
def get_time(line):
	return time.mktime(get_time_struct(line))
def collapes(sa,ea,sb,eb):
	return ea>sb and sa<eb

class Window(DataLoader):
	def __init__(self):
		DataLoader.__init__(self,1)
		self.dump_path = DUMP_PATH_BASE + 'window.pydump'
		self.filename = DATA_DIR + 'window_dates.tsv'
		self.data = {}
	def load_data_source(self):
		infile = open(self.filename);
		reader = csv.reader(infile, delimiter="\t",quoting=csv.QUOTE_NONE, quotechar="")
		reader.next() # burn the header
		return reader
	def deal_line(self,line):				
		(wid,st,mt,et) = line
		st = get_time(st)
		self.data[int(wid)] = [st,st + 2*24*60*60,get_time(mt),get_time(et)]

def load_wid_times():
	''' Output {window_id : [ start_time,verify_time,test_time,end_time ] } '''
	return Window().load_data();

class JidInsets(DataLoader):
	def __init__(self):
		DataLoader.__init__(self,1)
		self.dump_path = DUMP_PATH_BASE + 'jid_insets.pydump'
		self.filename = DATA_DIR + 'jobs.tsv'
		self.data = {}
	def load_data_source(self):
		infile = open(self.filename);
		reader = csv.reader(infile, delimiter="\t",quoting=csv.QUOTE_NONE, quotechar="")
		reader.next() # burn the header
		return reader
	def preprocess(self):
		self.windows = load_wid_times()
	def deal_line(self,line):				
		(JobID,WindowID,Title,Description,Requirements,City,State,Country,Zip5,StartDate,EndDate) = line
		st = get_time(StartDate)
		et = get_time(EndDate)
		wid = int(WindowID)
		win = self.windows[wid]
		self.data[int(JobID)] = [ collapes(st,et,win[0],win[1]),collapes(st,et,win[1],win[2]),collapes(st,et,win[2],win[3]) ]

def load_jid_insets():
	'''Output { jid : [in_train,in_verify,in_test,wid]  }   '''
	return JidInsets().load_data()

def load_csv_data(filename):
		infile = open(filename)
		reader = csv.reader(infile, delimiter="\t",quoting=csv.QUOTE_NONE, quotechar="")
		reader.next() # burn the header
		return reader


class JidWid(DataLoader):
	def __init__(self):
		DataLoader.__init__(self,1)
		self.dump_path = DUMP_PATH_BASE + 'jid_wid.pydump'
		self.filename = DATA_DIR + 'jobs.tsv'
		self.data = {}
	def load_data_source(self):
		return load_csv_data(self.filename)
	def deal_line(self,line):
		(JobID,WindowID,Title,Description,Requirements,City,State,Country,Zip5,StartDate,EndDate) = line
		self.data[int(JobID)]=int(WindowID)

def load_jid_wid():
	return JidWid().load_data()

class UidWid(DataLoader):
	def __init__(self):
		DataLoader.__init__(self,1)
		self.dump_path = DUMP_PATH_BASE + 'uid_wid.pydump'
		self.filename = DATA_DIR + 'users.tsv'
		self.data = {}
	def load_data_source(self):
		return load_csv_data(self.filename)
	def deal_line(self,line):
		(UserID,WindowID,Split,City,State,Country,ZipCode,DegreeType,Major,GraduationDate,WorkHistoryCount,TotalYearsExperience,CurrentlyEmployed,ManagedOthers,ManagedHowMany) = line
		self.data[int(UserID)] = int(WindowID)

def load_uid_wid():
	return UidWid().load_data()

class JobDistribute(DataLoader):
	def __init__(self):
		DataLoader.__init__(self,1)
		self.dump_path = DUMP_PATH_BASE + 'jid_distributes.pydump'
		self.filename = DATA_DIR + 'jobs.tsv'
		self.data = {}
		for i in range(1,8):
			self.data[i] = [ [[0,0],[0,0]],[[0,0],[0,0]] ]
	def load_data_source(self):
		return load_jid_insets()
	def preprocess(self):
		self.wids = load_jid_wid();
	def deal_line(self,line):
		jid = line	
		job = self.data_source[jid]
		wid = self.wids[jid]
		self.data[wid][job[0]][job[1]][job[2]] +=1

def load_job_distribute():
	''' Output a list cnt 
		cnt[w][x][y][z] means the number of jobs in window w which in_train=x in_verify=y in_test=z 
	'''
	return JobDistribute().load_data()

class MetaDataLoader(DataLoader):
	def load_data_source(self):
		return load_csv_data(self.filename)

class UidApps(MetaDataLoader):
	def __init__(self):
		MetaDataLoader.__init__(self)
		self.filename = DATA_DIR + 'apps.tsv'
		self.dump_path = DUMP_PATH_BASE + 'uid_apps.pydump'
		self.data = {}
	def deal_line(self,line):
		(UserID,WindowID,Split,ApplicationDate,JobID) = line
		uid = int(UserID)
		jid = int(JobID)
		t   = get_time(ApplicationDate)
		if uid in self.data :
		 	self.data[uid][jid] = t
		else :
		 	self.data[uid] = {jid:t}
	def postprocess(self):
		uid_wid = load_uid_wid()
		for uid in uid_wid :
			if not uid in self.data :
				self.data[uid] = {}	
			
def load_uid_apps():
	'''  Output : { uid : { jid : app_time  }  } '''
	return UidApps().load_data()


	
class UidHistory(MetaDataLoader):
	def __init__(self):
		MetaDataLoader.__init__(self)
		self.filename = DATA_DIR + 'user_history.tsv'
		self.dump_path = DUMP_PATH_BASE + 'uid_history.pydump'
		self.data = {}
	def deal_line(self,line):
		(UserID,WindowID,Split,Sequence,JobTitle) = line
		uid = int(UserID)
		seq = int(Sequence)
		if uid in self.data :
			self.data[uid][seq]=JobTitle
		else :
			self.data[uid] = { seq:JobTitle }
	def postprocess(self):
		uid_wid = load_uid_wid()
		for uid in uid_wid :
			if not uid in self.data :
				self.data[uid] = {}


def load_uid_history():
	'''  Output : { uid : { sequence : title  }  } '''
	return UidHistory().load_data()

class JidTitle(MetaDataLoader):
	def __init__(self):
		MetaDataLoader.__init__(self)
		self.filename = DATA_DIR + 'jobs.tsv'
		self.dump_path = DUMP_PATH_BASE + 'jid_title.pydump'
		self.data = {}
	def deal_line(self,line):
		(JobID,WindowID,Title,Description,Requirements,City,State,Country,Zip5,StartDate,EndDate) = line
		self.data[int(JobID)] = Title					
def load_jid_title():
	return JidTitle().load_data()

class UidTags(DataLoader):
	def __init__(self):
		DataLoader.__init__(self)
		self.dump_path = DUMP_PATH_BASE + 'uid_tags.pydump'
		self.filename = 'uid_tags'
		self.data = {}
	def load_data_source(self):
		return load_uid_wid()
	def preprocess(self):
		self.uid_apps = load_uid_apps()
		self.uid_history = load_uid_history()
		self.jid_title = load_jid_title()
	def deal_line(self,line):
		uid = line
		tmp = reduce((lambda x,y:x+y),[ self.jid_title[jid].split() for  jid in self.uid_apps[uid] ]  +  [ title.split()   for title in self.uid_history[uid].values()   ],[])
		self.data[uid] = set(tmp)

def load_uid_tags():
	return UidTags().load_data()


class FavJobs(MetaDataLoader):
	def __init__(self,filename,fields,sort_to_list=True):
		MetaDataLoader.__init__(self)
		self.filename = DATA_DIR + filename
		self.dump_path = DUMP_PATH_BASE + 'fav_jobs_on_' + filename + '_with_' + '_'.join(fields) + '_tolist='+str(sort_to_list)+'.pydump'
		self.data = {}
		self.fields = fields
	def load_data_source(self):
		infile = open(self.filename)
		reader = csv.reader(infile, delimiter="\t",quoting=csv.QUOTE_NONE, quotechar="")
		line = reader.next() # burn the header
		self.fields = [ line.index(f) for f in self.fields ]
		return reader
	def get_dict(self):
		cur = self.data
		for field in self.fields :
			key = line[field]
			if not key in cur :
				cur[key] = {}
			cur = cur[key]
		return cur
	def preprocess(self):
		self.apps = load_uid_apps()
	def deal_line(self,line):
		cur = self.data
		for field in self.fields :
			key = line[field]
			if not key in cur :
				cur[key] = {}
			cur = cur[key]	
		uid = int(line[0])
		for jid in self.apps[uid] :
			if jid in cur:
				cur[jid] +=1
			else :
				cur[jid] =1
	def postprocess(self):
		self.sort_dicts(len(self.fields),self.data)
	def sort_dicts(self,depth,data):
		if depth == 1 :
			for key in data :
				data[key] = sort_dic(data[key],True)				
		else :
			for key in data :
				self.sort_dicts(depth-1,data[key])


def load_fav_jobs(fields):
	''' Output { fields[1]:{fields[2]: {    ...  { jid:cnt }}   }       }   '''
	return FavJobs('users.tsv',fields).load_data()
def load_fav_jobs_wcdmm():
	return load_fav_jobs(['WindowID','City','DegreeType','Major','ManagedOthers'])


def main():
#	load_rawuid_id()
#	load_wid_times()
#	load_jid_insets()
#	print load_job_distribute()	
#	load_jid_wid()
#	load_uid_apps()
#	load_uid_history()
#	load_uid_tags()
#	load_fav_jobs(['City','DegreeType','Major','ManagedOthers'])
	load_fav_jobs_cdmm()
	pass


if __name__ == '__main__' :
	main()

