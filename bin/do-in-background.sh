include=`dirname $0`
source $include/../conf/conf

if [ $# -ne 1 ] ; then 
	echo haha
fi

$1 2>$LOG_DIR/stderr.$1 >$LOG_DIR/stdout.$1 &







