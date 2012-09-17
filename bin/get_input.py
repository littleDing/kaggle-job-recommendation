#!/usr/bin/python 

import sys
import logging
from myutil import *

if len(sys.argv) != 2:
    print >> sys.stderr, 'usage: <windowid>'
    sys.exit(-1)

windowid = int(sys.argv[1])

headers = True
for line in file('/home/lijiefei/match/kaggle/JobRec/Data/splitjobs/jobs%d.tsv' % (windowid)):
    if headers:
        headers = False
        continue
    cols = line.strip().split('\t')
    jobid = int(cols[0])

    try:
        disc = strip_tags(cols[3]).replace('\\r',' ').replace('\\n',' ')
    except:
        print >> sys.stderr, cols[3]
        disc = myparser(cols[3])
        #disc = cols[3].replace('\r',' ').replace('\n',' ')
#    disc.encode('utf-8','ignore')
    print '%d en %s' % (jobid,disc)

    




