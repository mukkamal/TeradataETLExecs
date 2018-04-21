#! /bin/ksh
this=${0##*/}
# this file is best viewed with tabstop=2
#==========================================================================================
# Filename:    execFEXP_ESB.sh
# Description: wrapper around the Teradata fastexport utility. 
#              Given a file as input parameter, usually .sql suffixed, it takes the 
#              file and , reads the SQL that it contains, produces a fastexport control file and
#              runs it against the default TD database server, connecting as 
#              - the td account specified in ~dw_adm/.logins/td_logins.dat, for that SQL script
#              OR, if there is no entry in ~dw_adm/.logins/td_logins.dat,
#              - dw_batch[_dev] 
#              The script also allows for dynamic SQL by resolving Shell-style variables ($var) at 
#              run-time. The variables present in the environment are resolved automatically, the others
#              can be passed from the command line as name=value pairs.
#              The script will also insert a record in a table called dw_monitor.execFEXP_monitor
#              for each run, recording the start and end times , the status (S-successful, F-failed),
#              the exit error code . The insert / update operations on this table are done using B_elf TD login.
#
USAGE_exfexp () {
cat << EOF
#
# USAGE : $this help | -help    <-- any of these will give you a full description
#       : $this <SQL script> [<var_name>=<value>] [<var_name>=<value>] ..."
#       :       [delimiter=<delim_in_hex>] [date_format=<date_format>] [compress=<Y|N|1-9>] 
#       :       [out_file=<out_file_full_path>]"
#       :       [charset=UTF8]"
#       :       [sessions=<nr_sessions>] [tenacity=<nr_of_hours>] [sleep=<nr_of_minutes>]"
#       :       [on_exception=<no_email|warning|warning,no_email>]"
#       :       [log_dir=<alternative_log_dir>]"
#       :       [DEBUG=1]"
# WHERE : 
#       : <SQL script> = the script that contains the SQL to be used for extraction (full path)
#       : <var_name>=<value> = parameters that are used in the SQL script and need to 
#       :                      be replaced at run time. 
#       : E.g. in the SQL script, you can have : ... where prd_id = \$PRD_ID
#       : and then when you invoke $this, you would go :
#       : $this \$DW_SQL/prog.sql PRD_ID=2004M07
#       : Supported datatypes : decimal, char, varchar, integer, byteint, smallint, date, timestamp
#       : for all other datatypes you will get the following in the control file e.g. VARBYTE - UNSUPPORTED DATATYPE
#       : <delimiter> = the delimiter to be used to separate the fields in the output file. 
#       :               Default is : 7C (| pipe)
#       :               NOTE: it has to be expressed in hexadecimal:
#       :               e.g. 7F=DEL(delete), 7C=|(pipe), 07=BEL (bell), 2C=,(comma)
#       : <date_format> = the format to be used for dates (years,months,days)
#       :               Default is : 'DD-MMM-YY' (this makes the file ready to load into Oracle)
#       :               e.g. 'YYYY-MM-DD'
#       : <compress> = Default is : N (no compression)
#       :              if you pass 'Y', the final file will be gzipped, using gzip -9
#       :              You can pass a value between 1 and 9 to adjust the compression factor gzip uses
#       : <charset> = if you pass UTF8, then fastexport is going to be invoked with -c 'UTF8'
#       : <out_file> = by default, the output file will be placed under \$DW_OUT and will have the following 
#       :              name : execFEXP.<sql_script_name>.<YYYYMMDDHHMISS>.PID.gz
#       :              for example : execFEXP.test.20041115191557.20746.gz
#       :              You can override this by passing this parameter e.g. out_file=\$DW_OUT/mydir/myfile 
#       : <no_email|warning|warning,no_email> = the default behavior in case an exception occurs is :
#       :  error,email (raise an error, send email to DW_BACK_END)
#       :  You can bypass this, by invoking it with the 'on_exception'
#       :  parameter. The accepted options are :
#       : on_exception=no_email
#       : on_exception=warning
#       : on_exception=warning,no_email
#       : on_exception=warning,no_email
#       : <log_dir> = redirect the log file to another directory (Default is : $DW_LOG)
#       :             e.g. log_dir=\$DW_HOME/tmp
#       : <DEBUG> = if you pass a value, the logs will be more verbose, and the helper temp files 
#       :           are not going to be removed at the end
#       : <sessions> = by default fexp is invoked with 16 sessions. You can by-bass this by passing this parameter.
#       :              (for higher volumes you might want to go 64, 96 sessions, for low volumes 4-8 would probably suffice)
#       : <tenacity> = how many hours will the fastexport keep on trying if it doesn't find an empty slot (max 15 slots per database)
#       :              Default is : 4 
#       : <sleep> = how many minutes will the fastexport sleep before trying to get a slot again.
#       :           Default is : 10
# EXAMPLES 
#       : $this \$DW_SQL/foo.sql PRD_ID=2004M01"
#       : $this \$DW_SQL/bar.sql FROM_DATE=\$FROM_DATE TO_DATE=\$TO_DATE"
#       : $this \$DW_SQL/bar.sql on_exception=warning,no_email"
#       : $this \$DW_SQL/foo.sql param1=value param2=value on_exception=warning
#       : $this \$DW_SQL/foo.sql param1=value param2=value on_exception=warning,no_email
#       : $this \$DW_SQL/foo.sql param1=value param2=value charset=UTF8
#       : $this \$DW_SQL/foo.sql param1=value param2=value log_dir=\$DW_LOG/my_project
#       : $this \$DW_SQL/foo.sql param1=value param2=value date_format=YYYY-MM-DD
#       : $this \$DW_SQL/foo.sql param1=value param2=value delimiter=7F
#       : $this \$DW_SQL/foo.sql param1=value param2=value delimiter=2C out_file=\$DW_OUT/mydir/myfile.dat date_format=YYYY-MM-DD log_dir=\$DW_LOG/mydir
#       :
# NOTES : [a] means this param is optional. <a|b|c> means a OR b OR c
#
EOF
}
#         
# Return : 101 - incorrect usage.Incorrect nr of params.
#          102 - incorrect usage. '=' not found in one of the params 
#          103 - can not write to the given log_dir
#          104 - delimiter does not have 2 characters
#          105 - delimiter is not a hex value
#          106 - date format contain characters other than Y,M,D and - (dash)
#          107 - can not write to the given out_file
#          108 - the gz file exists already
#          109 - can not read sql_script
#          110 - create temp volatile table failed
#          111 - execution of the perl piece failed
#          112 - execution of the first temp ksh failed
#          113 - execution of the second temp ksh failed
#          114 - strip trailing blanks failed
#          115 - compress failed
#          other non-zero - fastexport error
#
VERSION="DW_ELF_3.1"
# Modification History:
#
#
#==============================================================================

. $HOME/.dwProfile_ppadm
. $DW_HOME/lib/MsgHandler.ksh

#------------------------------------------
#-- Functions
#------------------------------------------
HELP_exfexp () {
eq_line=0
cat $0|while read line
do
  no_eq=`echo $line|grep "^#===="> /dev/null;echo $?`
  if (( no_eq == 1 )); then 
    if (( eq_line == 0 )); then
      continue;
    else
      printf "$line\n"|egrep -ve "{|}|EOF"|sed -e "s/\$this/$this/g"
    fi
  else
    printf "$line\n"|egrep -ve "{|}|EOF"|sed -e "s/\$this/$this/g"
    (( eq_line = eq_line + 1 ))
  fi  
  if (( eq_line == 2 )); then
    break
  fi
done
}
	
processException () {
  if [[ -z "$on_exception" ]];then
   	MsgHandler $this ERROR "$*" email |tee -a $log_file

 	elif [[ "$on_exception" == "no_email" ]];then
   	MsgHandler $this ERROR "$*" |tee -a $log_file

 	elif [[ "$on_exception" == "warning" ]];then
   	MsgHandler $this WARNING "$*" email |tee -a $log_file

 	elif [[ "$on_exception" == "warning,no_email" ]];then
   	MsgHandler $this WARNING "$*" |tee -a $log_file
 	fi
}

#------------------------------------------
#-- Process input parameters - nr. of params
#------------------------------------------
if [[ "$1" == "help" || "$1" == "-help" ]]; then
	HELP_exfexp
	exit 0
fi

nr_parms=$#
if (( nr_parms < 1 )) ; then
		USAGE_exfexp
		exit 101
fi

sql_script=$1
sql_script_b=${1##*/}

now=`date '+20%y%m%d%H%M%S'`
file_prefix=${this%.sh}.${sql_script_b%.sql}.$now.$$

temp_ksh_file_1=$DW_TMP/$file_prefix.1.ksh
temp_ksh_file_2=$DW_TMP/$file_prefix.2.ksh
temp_pre_log_file=$DW_TMP/$file_prefix.temp_pre.log
temp_post_log_file=$DW_TMP/$file_prefix.temp_post.log
temp_1=$DW_TMP/$file_prefix.1.tmp
temp_2=$DW_TMP/$file_prefix.2.tmp
temp_3=$DW_TMP/$file_prefix.3.tmp

ELF_MON_FILE=$DW_LOG/.elf_monitor
if [ ! -f $ELF_MON_FILE ]; then
	touch $ELF_MON_FILE
	chmod 660 $ELF_MON_FILE
fi
ELF_MON_TBL="dw_monitor.elf_monitor"

VOL_TEMP_TBL=execFEXP_${now}_$$
logtable=${workingDB}.felog_${now}_$$


#----------------------------------------
#-- Start building the temp ksh files
#----------------------------------------
cat<<-'EOC'> $temp_ksh_file_1
#! /bin/ksh

#. /.dwProfile
. $HOME/.dwProfile_ppadm
. $DW_HOME/lib/MsgHandler.ksh

processException () {
  if [[ -z "$on_exception" ]];then
   	MsgHandler $this ERROR "$*" email |tee -a $log_file

 	elif [[ "$on_exception" == "no_email" ]];then
   	MsgHandler $this ERROR "$*" |tee -a $log_file

 	elif [[ "$on_exception" == "warning" ]];then
   	MsgHandler $this WARNING "$*" email |tee -a $log_file

 	elif [[ "$on_exception" == "warning,no_email" ]];then
   	MsgHandler $this WARNING "$*" |tee -a $log_file
 	fi
}
EOC

cp $temp_ksh_file_1 $temp_ksh_file_2

cat<<EOC1>> $temp_ksh_file_1
this=execFEXP.sh-ksh1
EOC1

cat<<EOC2>> $temp_ksh_file_2
this=execFEXP.sh-ksh2
EOC2


#------------------------------------------
#-- Process input params - value and type of params
#------------------------------------------
#sql_script=$1
shift

params="$*"

i=0
for p in $params;do
	eq=`echo $p|grep = >/dev/null 2>&1;echo $?`
	if (( eq != 0 )); then
		msg="One of the parameters does not have '=', or there are extra spaces on the command line. Command was : $this $sql_script $*"
		processException $msg 
		USAGE_exfexp
		exit 102
	fi
	arr[$i]="$p"

	name=`echo ${arr[$i]%%=*}`
	value=`echo ${arr[$i]##*=}`

	eq_vars=`echo ${arr[$i]}|egrep -e "charset=|out_file=|compress=|delimiter=|date_format=|on_exception=|log_dir=|DEBUG=|sessions=|tenacity=|sleep=" >/dev/null 2>&1;echo $?`

	if (( eq_vars == 0 )); then
		eval `echo $name=$value`
	fi
	
	echo "$name=$value" >> $temp_ksh_file_1
	echo "$name=$value" >> $temp_ksh_file_2
	(( i = i + 1 ))
done    

if [[ -n $log_dir ]]; then
	DW_LOG=$log_dir
	if [[ ! -w $log_dir ]]; then
		msg="Can not write to specified log_dir $log_dir"
		processException $msg 
		exit 103
	fi
fi

log_file=$DW_LOG/$file_prefix.log

if [[ -z "$sessions" ]]; then
	sessions=8
fi
if [[ -z "$tenacity" ]]; then
	tenacity=4
fi
if [[ -z "$sleep" ]]; then
	sleep=5
fi

if [[ ! -z "$delimiter" ]]; then
	n_c=$(echo $delimiter|wc -m)
	if (( n_c != 3 )); then
		msg="Delimiter does not have 2 characters. It has to be a 2 hex digit value, e.g. 7C for | (pipe)"
		processException $msg
		exit 104
	fi
	d=$(echo $delimiter |tr -d '[:digit:]'|tr -d '[A-F]')
	if [[ ! -z "$d" ]]; then
		msg="Delimiter has to be a hex value , e.g. 7C for | (pipe)"
		processException $msg
		exit 105
	fi
else
	#-- Default delimiter is PIPE
	delimiter="7C"
fi

if [[ ! -z "$date_format" ]]; then
	d=$(echo $date_format |tr -d 'M'|tr -d 'Y'|tr -d 'D'|tr -d '-')
	if [[ ! -z "$d" ]]; then
		msg="Date format can contain only Y,M,D and - (dash), e.g. YYYY-MM-DD"
		processException $msg
		exit 106
	fi
else
	#-- Default date format is Oracle format
	date_format='DD-MMM-YY'
fi

do_compress=0
if [[ -n "$compress" ]]; then
	if [[ "$compress" == "Y" ]]; then
		do_compress=9
	else
		do_compress=$compress
	fi	
fi

if [[ -n "$out_file" ]]; then
	#-- Append .tmp to the name passed from outside
	out_file=$out_file.tmp
else
	out_file=$DW_OUT/$file_prefix.tmp
fi

#-- Create the fifo . If you can't
#-- throw an error and exit
#-- If it exists (from a previous 
#-- unsuccessful run), remove it
rm -f $out_file > /dev/null 2>&1
mkfifo $out_file

if [[ ! -p $out_file ]];then
	msg="Can not create fifo pipe: $out_file"
	processException $msg 
	exit 107
fi

if [[ ! -z "$charset" ]]; then
	charset='utf8'
fi

if [[ -n $DEBUG ]]; then
	echo "temp_ksh_file_1=$temp_ksh_file_1"
	echo "temp_ksh_file_2=$temp_ksh_file_2"
	echo "temp_pre_log_file=$temp_pre_log_file"
	echo "temp_post_log_file=$temp_post_log_file"
	echo "temp_1=$temp_1"
	echo "temp_2=$temp_2"
	echo "temp_3=$temp_3"
	echo "out_f=$out_f"
	echo "out_file=$out_file"
	echo "log_file=$log_file"
	echo "date_format=$date_format"
	echo "delimiter=$delimiter"
	echo "compress=$compress"
	echo "charset=$charset"
fi

#----------------------------------------
#-- Can I read the sql_script ?
#----------------------------------------
if [[ ! -r $sql_script ]];then
	msg="Can not read file: $sql_script"
	processException $msg 
	exit 109
fi

MsgHandler $this INFO "Starting $this script" > $log_file
START_TS=$(date '+%Y-%m-%d %H:%M:%S')

#------------------------------------------
#-- Global variables - temp ksh 1
#------------------------------------------
cat<<EOC>>$temp_ksh_file_1
sessions=$sessions
tenacity=$tenacity
sleep=$sleep
now=$now
sql_script=$sql_script
sql_script_b=$sql_script_b
temp_pre_log_file=$temp_pre_log_file
temp_post_log_file=$temp_post_log_file
temp_1=$temp_1
temp_2=$temp_2
temp_3=$temp_3
log_file=$log_file
out_file=$out_file
temp_ksh_file_2=$temp_ksh_file_2
VOL_TEMP_TBL=$VOL_TEMP_TBL
on_exception=$on_exception
delimiter=$delimiter
date_format=$date_format
DEBUG=$DEBUG
DW_LOG=$DW_LOG
EOC

#------------------------------------------
#-- Process the SQL. Turn it into a control 
#-- file that can be then passed directly
#-- to fastexport
#------------------------------------------

cat<<-'EOFX'>> $temp_ksh_file_1
(bteq<<EOBT
.logon $DWDB; 
.set width 254;
create volatile table $VOL_TEMP_TBL as 
(
EOFX

cat $sql_script|tr -d ';'|egrep -ve "^--" >> $temp_ksh_file_1

cat<<-'EOFX'>> $temp_ksh_file_1
) with no data;
help column $VOL_TEMP_TBL.*;
.exit;
EOBT
:) >> $temp_1 2>&1
err=`grep  "RC (return code) =" $temp_1|sed -e 's/^..*=//g'`
if (( err != 0 )); then
	msg="Create temp table failed. See $temp_1 for details."
	processException $msg
	exit 110
fi

MsgHandler $this INFO "Executing perl piece - sql2fexp"
$DW_LIB/sql2fexp.pl $sql_script $temp_1 $temp_2 $temp_3 $date_format $delimiter $DEBUG
err=$?
if (( err != 0 ));then
	msg="Execution of the perl piece failed. See $log_file for details."
	processException $msg
	exit 111
fi
EOFX

#------------------------------------------
#-- Execute the first ksh script
#------------------------------------------
chmod +x $temp_ksh_file_1
$temp_ksh_file_1 >> $log_file 2>&1
err=$?
if (( err != 0 ));then
	msg="Execution of the first temp ksh failed. See $log_file for details."
	processException $msg
	exit 112
fi

#-----------------------------------------
#-- Global variables temp ksh 2
#-----------------------------------------
cat<<EOFX>> $temp_ksh_file_2
sessions=$sessions
tenacity=$tenacity
sleep=$sleep
now=$now
sql_script=$sql_script
sql_script_b=$sql_script_b
temp_post_log_file=$temp_post_log_file
log_file=$log_file
out_file=$out_file
logtable=$logtable
SEQ=$SEQ
on_exception=$on_exception
delimiter=$delimiter
date_format=$date_format
charset=$charset
DEBUG=$DEBUG
DW_LOG=$DW_LOG
if [[ ! -z "\$charset" ]]; then
	cmd="/usr/bin/fexp -c \$charset"
	MsgHandler $this INFO "Charset set to : \$charset"
else
	cmd="/usr/bin/fexp"
fi

MsgHandler \$this INFO "Invoking : \$cmd"
\$cmd <<EOFEXP>>$log_file 2>&1
.logtable $logtable;
.logon $DWDB;
.begin export sessions $sessions tenacity $tenacity sleep $sleep; 
EOFX

cat $temp_3 >> $temp_ksh_file_2

cat<<EOFX>> $temp_ksh_file_2
.export outfile $out_file format text mode record;
.end export;
.logoff;

EOFEXP
EOFX

cat<<-'EOFX'>>$temp_ksh_file_2
real_err=`grep  "return code encountered =" $log_file|cut -d"'" -f2`
exit $real_err
EOFX


#-- Start the stripper and compressor 
#-- and attach them to the consumer
#-- end of the fifo pipe
if (( do_compress )); then
	#-- If the gzip file exists, remove it first
	if [[ -f ${out_file%%.tmp}.gz ]]; then
		if [[ ! -w ${out_file%%.tmp}.gz ]]; then
			#-- Try to chmod the file
			MsgHandler $this INFO "Existing compressed file is not writable. Attempting chmod." >> $log_file
			\chmod 777 ${out_file%%.tmp}.gz > /dev/null 2>&1
		fi

		#-- Check the write perms again. If you still can't write, bail
		if [[ ! -w ${out_file%%.tmp}.gz ]]; then
			msg="Can not overwrite existing compressed file. Check permissions. (${out_file%%.tmp}.gz)" >> $log_file
			\rm $out_file > /dev/null 2>&1
			processException $msg
			exit 112
		else
			MsgHandler $this INFO "Existing compressed file will be overwritten." >> $log_file
			\rm ${out_file%%.tmp}.gz > /dev/null 2>&1
		fi
	fi
	MsgHandler $this INFO "Starting stripper and compressor in the background, listening to pipe." >> $log_file
	cat $out_file | /usr/local/bin/perl -p -e 's{\s+$}{\n}g;' | /usr/bin/gzip -$do_compress > ${out_file%%.tmp}.gz &
else
	MsgHandler $this INFO "Starting stripper in the background, listening to pipe." >> $log_file
	cat $out_file | /usr/local/bin/perl -p -e 's{\s+$}{\n}g;' > ${out_file%%.tmp} &
fi

#------------------------------------------
#-- Execute the second ksh script
#------------------------------------------
chmod +x $temp_ksh_file_2 
$temp_ksh_file_2 >> $log_file 2>&1
err=$?
if (( err != 0 ));then
	msg="Execution of the second temp ksh failed. See $log_file for details."
	processException $msg
	exit 113
fi


#-- Inspect the ldrlog of the fastexp to see if there are 0 
#-- records returned
#**** 14:25:17 UTY8722 233 total records written to output file.
nr_records=$(grep '^..* total records' $log_file|sed -e 's/^..* \([0-9][0-9]*\) total records..*/\1/g')

if (( ! nr_records )); then
	msg="Output file : ${out_file%%.tmp} is empty."

	if [[ -z "$on_exception" ]];then
		MsgHandler $this WARNING "$msg" email |tee -a $log_file
	 
	elif [[ "$on_exception" == "no_email" ]];then
		MsgHandler $this WARNING "$msg" |tee -a $log_file
	 
	elif [[ "$on_exception" == "warning,no_email" ]];then
		MsgHandler $this WARNING "$msg" |tee -a $log_file
	fi
else
	if (( do_compress )); then
		MsgHandler $this INFO "Output file is : ${out_file%%.tmp}.gz" >> $log_file
	else
		MsgHandler $this INFO "Output file is : ${out_file%%.tmp}" >> $log_file
	fi
fi

#-- Insert a record into the elf_monitor file. execTdSQL will insert this record
#-- into dw_monitor.elf_monitor via the elf_adm daemon, FlushMonitor.ksh
END_TS=$(date '+%Y-%m-%d %H:%M:%S')

(( err )) && STATUS='F' || STATUS='S'

TD_LOGIN=$(grep "^..*\.logon" $log_file)
TD_LOGIN=${TD_LOGIN##*/}
TD_LOGIN=${TD_LOGIN%%,*}

echo "insert into $ELF_MON_TBL values ('execFEXP','$sql_script_b','$START_TS','$END_TS',$err,'$STATUS',NULL,'$TD_LOGIN',$nr_records);" >> $ELF_MON_FILE

chmod 777 $ELF_MON_FILE >/dev/null 2>&1

#-- If everything goes well, remove all the temporary and helper files.
if [[ -z $DEBUG ]]; then
	\rm -f $out_file
	\rm -f $temp_ksh_file_1
	\rm -f $temp_ksh_file_2
	\rm -f $temp_1
	\rm -f $temp_2
	\rm -f $temp_3
	\rm -f $temp_pre_log_file
  \rm -f $temp_post_log_file
fi

MsgHandler $this INFO "Finished successfully." >> $log_file

exit 0

