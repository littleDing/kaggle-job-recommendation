from conf import *
from datas import *
import logging
import random

logging.basicConfig(level=logging.NOTSET,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')





def test_record():
	logging.info('sampling users')
	apps = load_uid_apps()
	users = {}
	ans = {}
	uid_wid = load_uid_wid()
	for (uid,app) in apps.items() :
		if len(app) > 5 and uid_wid[uid] ==1 and random.random() > 0.95  :
			users[uid] = app
			ans[uid] = [ [0,0,0,0] ,[0,0,0,0] ]
	logging.info(str(len(users)) + 'user is selected')

	his = load_uid_history()
	jid_wid = load_jid_wid()
	logging.info('load user history tags')
	uid_tags = {}
	for uid in his :
		ll = len(his[uid])
		old = set([])
		new = set([]) 
		if ll >0 :
			for i in range(1,ll/2):
				old |= set(his[uid][i].split())
			for i in range(max(ll/2,1),ll+1):		
				new |= set(his[uid][i].split())
		uid_tags[uid] = (old,new,old&new,old|new)

	logging.info('culculating answers')
	with open(DATA_DIR + 'jobs.tsv') as fin :
		fin.readline()
		haha = LineLogger()
		for line in fin :	
			(JobID,WindowID,Title,Description,Requirements,City,State,Country,Zip5,StartDate,EndDate) = line.split('\t')
			jid = int(JobID)
			if jid_wid[jid] == 1 :
				jtag = set(Title.split())
				for uid in users :
					if 	uid_wid[uid] == jid_wid[jid] :
						has_app = 1 if jid in apps[uid] else 0
						for i in range(0,3):
							ans[uid][has_app][i] +=len(jtag & uid_tags[uid][i])
						ans[uid][has_app][3] += len(jtag - uid_tags[uid][3])
			haha.inc()
		haha.end()

	logging.info('writing answers to file')
	with open(TMP_DIR + 'uid_app_of_title','w') as fout :
		for uid in ans :
			fout.write(str(uid))
			for i in [0,1] :
				for j in [0,1,2,3]:
					fout.write(' ' + str(ans[uid][i][j] ))
			fout.write('\n')


def main():
	test_record()
	pass

if __name__ == '__main__' :
	main()

