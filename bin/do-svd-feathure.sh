include=`dirname $0`
source $include/../conf/conf


function do_svd()
{
#	"$BIN_DIR/do-svd-feathure-makeinput.py"	
			
	model_dir=$TMP_DIR/svd/model/
	mkdir -p $model_dir

	$SVD_TRAIN $CONF_DIR/svd.conf model_out_folder=$model_dir
	$SVD_TEST $CONF_DIR/svd.conf pred=100 model_out_folder=$model_dir  name_pred=$TMP_DIR/svd-output.data

	paste "$TMP_DIR/svd-test.data"  "$TMP_DIR/svd-output.data"  | sort -nr -k5,5 -k7,7 > "$TMP_DIR/svd-result.data" 	
	return 
	awk 'BEGIN{
		last=-1
	}ARGIND==1{
		uids[$2]=$1
	}ARGIND==2{
		jids[$2]=$1
	}ARGIND==3{
		uid=substr($5,0,length($5)-2)
		jid=substr($6,0,length($6)-2)
		uid=uids[uid]
		jid=jids[jid]
		if(uid !=last){
			if(last !=-1){
				printf("%d",last);
				for(jid in rec){
					printf(" %d",jid)
				}
				printf(
			}
			last = uid
			delete a
		}
	}' $TMP_DIR/uids.map $TMP_DIR/jids.map  $TMP_DIR/svd-result.data   > $TMP_DIR/svd-rec.data 

	return 

}

do_svd

