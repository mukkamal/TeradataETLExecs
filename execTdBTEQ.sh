#!/bin/ksh
function _help {
   exec 1>&2
   echo  "Name: execTdBTEQ.sh"
   echo
   echo  "Description:" 
   echo  
   echo  " Emulate db2 behavior in bteq. Executes bteq SQL statements/files that are passed as"
   echo  " parameters. The output of the SQL may also be exported to a file."
   echo  " An error in execution of any of the sql stmts/files"
   echo  " will cause BTEQ to exit with a return code equal to that stmt/file's sequence number."
   echo  " e.g. if the 1st SQL file fails, return code will be 1  "
   echo  " if the 2nd SQL file fails, return code will be 2 and so on."
   echo
   echo  "Variables:"
   echo  "  LOGON_DIR - required,this must point to the directory containing"
   echo  "  your teradata logon files."
   echo  "  TD_SEC - optional,this defaults to td.sec and points to the file in \$LOGON_DIR"
   echo  "  containing the logon information." 
   echo
   echo  "Variables with respect to logonmgr"
   echo  "  LOGON_DIR - required,this must point to the directory that will contain"
   echo  "  your teradata logon files. you need read and write access"
   echo  "  TD_SEC - used internally do not change"
   echo
   echo  "Options"
   echo  " -x \"<SQL-statement>\" - pass in a quoted sql statement"
   echo  " -xo \"<SQL-statement>\"  <export-file-name> - pass in a quoted sql statement and"
   echo  "     places output in file"     
   echo  " -f <SQL-input-file-name> - name of file containing sql to run"
   echo  " -lc logon connection name - name of connection saved in logonmgr optional if the variable \$LOGON_CONNECTION is exported with the connection name"
   echo  " -fo <SQL-input-file>  <export-file-name> - runs the sql and places output in file"
   echo  " -xv \"SQL-statement>\" - will redirect non-data information to stderr so that only the data goes to stdout."
   echo  "        This is useful when you want to place the data in a variable, for instance." 
   echo  "Usage:"
   echo  
   echo  "execTdBTEQ.sh"
   echo  "   -x \"<SQL-statement>\""                           
   echo  "   -f <SQL-input-file-name>"                          
   echo  "   -lc <logon connection name>"                          
   echo  "   -xo \"<SQL-statement>\" <export-file-name>"    
   echo  "   -xv \"<SQL-statement>\""
   echo  "   -fo <SQL-input-file>  <export-file-name>"     
   echo
   echo  "Notes:" 
   echo  " 1. When using STDIN as input, then the bteq default behavior is assumed."
   echo  " In other words when using STDIN, then you are responsible for coding error"
   echo  " checks, maxerrors, exports and so on."
   echo  " 2. Logon files must contain the standard teradata logon string:"
   echo  "    .logon tpd/id,password"
   echo  " 3. If using Logonmgr then disregard #2, just ensure that a valid connection "
   echo  "    has been setup in logonmgr"


   test  
}

function _exit {
  rc=$?
  rm -f $BTEQ_SCRIPT
  #rm -f $NAMED_PIPE
  #rm -f $LOGON_DIR/$TD_SEC
  echo $1 1>&2
  echo "execTdBTEQ.sh cleaning up" 1>&2
  trap -
  exit $rc
}

#TD_SEC=logon_file$$.sec

test -n "$TMP_FILE_PATH" || _exit "\$TMP_FILE_PATH is not set" 

test -n "$LOGON_DIR" || _exit "Logon dir \$LOGON_DIR not set" 

#rm -f $LOGON_DIR/$TD_SEC
#mkfifo $LOGON_DIR/$TD_SEC
#chmod 600 $LOGON_DIR/$TD_SEC

if [[ -z "$TD_SEC" ]]
then
    echo "TD_SEC is not set default to td.sec" 1>&2
    export TD_SEC="td.sec"
fi

test -f "$LOGON_DIR/$TD_SEC" || test -p "$LOGON_DIR/$TD_SEC" || _exit "File $LOGON_DIR/$TD_SEC does not exist"

#Build BTEQ script
BTEQ_SCRIPT=$TMP_FILE_PATH/BTEQ_file$$.dat

trap "_exit"  ERR
		
cat <<EOF >$BTEQ_SCRIPT
.RUN FILE=$LOGON_DIR/$TD_SEC


EOF



#Check the number of arguments passed to the script.
#if no parms passed in then we assume input is from stdin
#in this case the only thing added will be the logon command
#bteq default error handling is in effect for this option
if [ $# == 0 ]
then
    cat - >>$BTEQ_SCRIPT
    echo ".quit" >>$BTEQ_SCRIPT

else
  #stdin was not used and we shall parse the command line options
  cnt=1
  while [[ $# != 0 ]]; do
    case "$1" in
    -x) 
	shift;SQL_STMNT=$1

	cat <<-EOF >>$BTEQ_SCRIPT
 
	.LABEL EXEC_SQL_STMNT_$cnt
	$SQL_STMNT
  
	.IF ERRORCODE <> 0 THEN .QUIT $cnt
 
	EOF
 
	cnt=`expr $cnt + 1`
	;;

    -f) 
	shift; SQL_FILE=$1

	cat<<-EOF >>$BTEQ_SCRIPT
	.LABEL EXEC_SQL_FILE_$cnt
	.RUN FILE=$SQL_FILE

	.IF ERRORCODE <> 0 THEN .QUIT $cnt

	EOF

	cnt=`expr $cnt + 1`	 

	;;

    -fo) 
	 shift; SQL_FILE=$1
	 shift; EXPORT_FILE=$1
	 rm -f $EXPORT_FILE 2>/dev/null
	cat<<-EOF >>$BTEQ_SCRIPT
         .SET WIDTH 300
	 .EXPORT DATA FILE $EXPORT_FILE
	 .SET RECORDMODE OFF
	EOF

	cat $SQL_FILE >>$BTEQ_SCRIPT
 
	cat<<-EOF >>$BTEQ_SCRIPT
  
	.IF ERRORCODE <> 0 THEN .QUIT $cnt
 
	.EXPORT RESET
	
	EOF
 
	cnt=`expr $cnt + 1`
	 ;;

    -lc) 
	shift; connection=$1

	;;


    -xo) 
	 shift; SQL_STMNT=$1
	 shift; EXPORT_FILE=$1
	 rm -f $EXPORT_FILE 2>/dev/null
	cat<<-EOF >>$BTEQ_SCRIPT
         .SET WIDTH 300
	 .EXPORT DATA FILE $EXPORT_FILE
	 .SET RECORDMODE OFF

	 $SQL_STMNT
 
	 .IF ERRORCODE <> 0 THEN .QUIT $cnt
 
	 .EXPORT RESET
 
	EOF
 
	 cnt=`expr $cnt + 1`
	 ;;

    -xv) 
	 shift; SQL_STMNT=$1
	 NAMED_FILE=$TMP_FILE_PATH/pipe.$$
	 #mknod $NAMED_PIPE p || _exit "Cannot make named pipe $TMP_FILE_PATH/pipe.$$" 
	 rm -f $EXPORT_FILE 2>/dev/null
	cat<<-EOF >>$BTEQ_SCRIPT
    .SET WIDTH 300
	.EXPORT DATA FILE $NAMED_FILE
	.SET RECORDMODE OFF

	$SQL_STMNT
 
	.IF ERRORCODE <> 0 THEN .QUIT $cnt
 
	.EXPORT RESET

	EOF
     #cat $NAMED_PIPE &
	 #cat $NAMED_FILE
	 cnt=`expr $cnt + 1`
	 ;;

    -h)
	_help;;
    *) 
       echo 1>&2
       echo "execTdBTEQ.sh unexpected parameter: '$1'"  1>&2
       _help
    esac
    shift
  done

#if [ -z "$connection" ];then connection=$LOGON_CONNECTION
#fi

#if [ -z "$connection" ];then _exit "connection name not passed via the -lc switch and variable \$LOGON_CONNECTION has not been set, please do one or the other"
#fi

#get_logon_info.sh $connection $LOGON_DIR/$TD_SEC td || _exit "Connection error check the details of the connection named \"$connection\" in logonmgr"
#chmod 400 $LOGON_DIR/$TD_SEC

cat<<-EOF >>$BTEQ_SCRIPT
  .LABEL FINISH
  .QUIT 0

EOF
fi


# Invoke BTEQ script to do execute the BTEQ script

#if [[ -z "$NAMED_PIPE" ]]
#then
#    bteq<$BTEQ_SCRIPT 1>&2
#	#cat $NAMED_FILE
#	#rm -f $NAMED_FILE
#else
#    test -p "$NAMED_PIPE" || _exit "$NAMED_PIPE is not a pipe"
#    bteq<$BTEQ_SCRIPT 1>&2
#fi

bteq<$BTEQ_SCRIPT 1>&2

if  [[ ! -z "$NAMED_FILE" ]]
then
cat $NAMED_FILE
rm -f $NAMED_FILE
fi

_exit "Complete"

