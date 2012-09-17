from tools import *
from conf import *
import myutil

logging.basicConfig(level=logging.NOTSET,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')

def removeSpecial(string,special=('\r','\\r','\n','\\n','\xa0','\xc2')):
	ret = string
	for tag in special :
		ret = ret.replace(tag,' ')
	return ret

def make_output(filename,column,output):
	with open(DATA_DIR+output,'w') as fout:
		with open(DATA_DIR+filename) as fin:
			haha = LineLogger(output)
			fin.readline()
			for line in fin :
				haha.inc()
				sp = line.split('\t')
				outstr = sp[0]+" "+sp[1]+" "
				try:
					outstr += removeSpecial(strip_tags(sp[column]) )	
				except:
					outstr += removeSpecial(myutil.myparser(sp[column]))
					pass
				fout.write(outstr +'\n')
			haha.end()

def main():
	make_output('jobs.tsv',3,'jid_wid_description')
	make_output('jobs.tsv',4,'jid_wid_requirement')

if __name__ == '__main__':
	main()

