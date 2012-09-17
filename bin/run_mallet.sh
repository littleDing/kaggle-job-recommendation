#!/bin/bash

if [ $# -ne 1 ]
then
    echo "usage: <windowid>"
    exit -1
fi

python get_input.py $1 > jobdisc$1 2> err$1

iconv -c -f "utf-8" -t "gbk" jobdisc$1 > jobdisc$1.gbk

~/Downloads/mallet-2.0.6/bin/mallet import-file --input jobdisc$1.gbk --output jobdisc$1.mallet --keep-sequence TRUE --remove-stopwords TRUE --encoding GBK


~/Downloads/mallet-2.0.6/bin/mallet train-topics --input jobdisc$1.mallet --inferencer-filename job$1.infer --evaluator-filename job$1.eval --output-topic-keys job$1-topic-key --topic-word-weights-file job$1-topic-word-weight --word-topic-counts-file job$1-word-topic-counts --output-doc-topics job$1-output-doc-topics --num-topics 200 --num-threads 8


#./kmeans_cluster.py $1

