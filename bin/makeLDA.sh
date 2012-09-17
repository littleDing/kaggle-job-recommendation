include="/home/share/kaggle/kaggle-job-recommendation/bin/"
DATA_DIR="$include/../data/"
LOG_DIR="$include/../log/"
MALLET_ROOT="/home/littleding/IDE/mallet/mallet-2.0.7/"
MALLET="${MALLET_ROOT}/bin/mallet"

## Input: $1 => train data
##Output: $1.mallet => data imported into mallet
##		  $1.lda => lda outputs
function train(){
local input=$1;
local input_file=${input##*/}
	$MALLET import-file --input ${input} --output ${input}.mallet   --keep-sequence --remove-stopwords
	$MALLET train-topics --input ${input}.mallet --num-topics 200 --output-state ${input}.state --num-threads 8 --output-doc-topics ${input}.lda 
}

stdout=$LOG_DIR/stdout.makeLDA
stderr=$LOG_DIR/stderr.makeLDA

train $DATA_DIR/jid_wid_description >$stdout 2>$stderr
train $DATA_DIR/jid_wid_requiredment >>$stdout 2>>$stderr

