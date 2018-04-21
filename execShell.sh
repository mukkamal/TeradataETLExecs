#! /bin/ksh
this=${0##*/}
# this file is best viewed with tabstop=2
#==========================================================================================
# FILE  : execShell.sh
# TYPE  : shell script
# DESC  : wrapper for running a shell script from within Appworx, or Unix command line
#
#
# CALL  : it can be called at the command line like a regular script, or as a library from Appworx (LIB_EXECFTP)
#
USAGE_exshell () {
cat << EOF
#
# USAGE : $this help | -help    <-- any of these will give you a full description
#       : $this <shell script> [param1] [param2] ..."
#       :  [on_exception=<no_email|warning|warning,no_email>]"
#       :  [log_dir=<alternative log directory (default=\$DW_LOG>]"
#       :  [redirect_stdinerr=<alternative log file> append_stdinerr=<alternative log file>]"
#       :  [record_stats=<Y|N>] - not needed anymore, it is always Y, see full description below"
#       :  [DEBUG=1]"
#       :
#       : on_exception=<no_email|warning|warning,no_email> - the default behavior in case an exception occurs is :
#       :  error,email (raise an error, send email to DW_BACK_END)
#       :  You can bypass this, by invoking it with the 'on_exception'
#       :  parameter. The accepted options are :
#       : on_exception=no_email
#       : on_exception=warning
#       : on_exception=warning,no_email
#       : log_dir=<alternate log directory> - this script produces a log file in \$DW_LOG. In case you need to override that
#       : you can pass this option as : log_dir=<some other directory>
# NOTES : [a] means this param is optional. <a|b|c> means a OR b OR c
#       :
#       : record_stats=Y - starting with ELF release 3.0, this is turned on by default. Preserved
#       :               for backward compatibility but not used. Running stats are always collected
#       :             (the elf_adm daemon FlushMonitor.ksh is inserting them into dw_monitor.elf_monitor)
#       : redirect_stdinerr=<some log file>  - by default, the STDOUT and STDERR will be 
#       :      redirected to the execShell log. If you want to bypass that, use this option to specify
#       :      another file Note: an unfortunate typo made this stick as *stdinerr rather than *stdouterr, too late to change now :(
#       : append_stdinerr=<some log file> - same as above, just append to an existing log file (and same note)
#       : $this \$DW_EXE/foo.sh param1 param2 ... append_stdinerr=\$DW_LOG/foo.log
#       : <DEBUG> = if you pass a value, the logs will be more verbose, and the helper temp files 
#       :           are not going to be removed at the end
# EXAMPLES 
#       : $this \$DW_SQL/foo.sh 
#       : $this \$DW_EXE/foo.sh param1 param2 ... on_exception=no_email
#       : $this \$DW_EXE/foo.sh param1 param2 ... on_exception=warning
#       : $this \$DW_EXE/foo.sh param1 param2 ... on_exception=warning,no_email
#       : $this \$DW_EXE/foo.sh param1 param2 ... record_stats=Y
#       : $this \$DW_EXE/foo.sh param1 param2 ... redirect_stdinerr=\$DW_LOG/foo.log
#       :      the above ex. will launch the script as : \$DW_EXE/foo.sh > \$DW_LOG/foo.log 2>&1
#       : $this \$DW_EXE/foo.sh param1 param2 ... append_stdinerr=\$DW_LOG/foo.log
#       :      the above ex. will launch the script as : \$DW_EXE/foo.sh >> \$DW_LOG/foo.log 2>>&1
EOF
}
#         
# Return : 101 - incorrect usage.Incorrect nr of params.
#          102 - shell script does not exist or is not readable 
#          103 - shell script is not executable
#          > 0 and != 101, 102, 103 - in case the called shell script failed, its return code.
#
VERSION="DW_ELF_3.0"
#==============================================================================
. $HOME/.dwProfile_ppadm
. $DW_HOME/lib/MsgHandler.ksh

#------------------------------------------
#-- Functions
#------------------------------------------
HELP_exshell () {
eq_line=0
cat $0|while read line
do
  no_eq=$(echo $line|grep "^#===="> /dev/null;echo $?)
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
   	MsgHandler $this ERROR "$*" email >> $log_file

 	elif [[ "$on_exception" == "no_email" ]];then
   	MsgHandler $this ERROR "$*" >> $log_file

 	elif [[ "$on_exception" == "warning" ]];then
   	MsgHandler $this WARNING "$*" email >> $log_file

 	elif [[ "$on_exception" == "warning,no_email" ]];then
   	MsgHandler $this WARNING "$*" >> $log_file
 	fi
}

#------------------------------------------
#-- Process input params - value and type of params
#------------------------------------------
if [[ "$1" == "help" || "$1" == "-help" ]]; then
	HELP_exshell
	exit 0
fi

nr_parms=$#
if (( nr_parms < 1 )) ; then
		USAGE_exshell
		exit 101
fi

sh_script=$1
sh_script_b=${1##*/}
shift

params="$*"

for p in $params;do
	eq=$(echo $p|grep = >/dev/null 2>&1;echo $?)

	if (( eq == 0 )); then
		exc=$(echo $p|egrep -e "on_exception=|record_stats=|redirect_stdinerr=|append_stdinerr=|log_dir|DEBUG=" >/dev/null 2>&1;echo $?)
		if (( exc == 0 )); then
			name=$(echo ${p%%=*})
			value=$(echo ${p##*=})
			eval $(echo $name=$value)
		fi
	fi
done    

params=$(echo ${params%%record_stats=*})
params=$(echo ${params%%on_exception=*})
params=$(echo ${params%%redirect_stdinerr=*})
params=$(echo ${params%%append_stdinerr=*})
params=$(echo ${params%%log_dir=*})
params=$(echo ${params%%DEBUG=*})

now=$(date '+20%y%m%d%H%M%S')
file_prefix=${this%.sh}.${sh_script_b%.sql}.$now.$$
temp_pre_log_file=$DW_HOME/tmp/$file_prefix.temp_pre.log
temp_post_log_file=$DW_HOME/tmp/$file_prefix.temp_post.log

if [[ -n $log_dir ]]; then
	if [[ ! -w $log_dir ]]; then
		msg="Can not write to specified log_dir $log_dir"
		processException $msg 
		exit 106
	else
		log_file=$log_dir/$file_prefix.log
	fi
else
	log_file=$DW_LOG/$file_prefix.log
fi

if [[ -n $DEBUG ]]; then
	cat<<EOF
  sh_script=$sh_script
  sh_script_b=$sh_script_b
  params=$params
  on_exception=$on_exception
  redirect_stdinerr=$redirect_stdinerr
  append_stdinerr=$append_stdinerr
  log_dir=$log_dir
  DEBUG=$DEBUG
EOF
fi

MsgHandler $this INFO "Starting" >> $log_file
START_TS=$(date '+%Y-%m-%d %H:%M:%S')

#----------------------------------------
#-- Can I execute the sh_script ? 
#----------------------------------------
if [[ ! -x $sh_script ]];then
	MsgHandler $this ERROR "Can not execute file: $sh_script"  email
	exit 103
fi

MsgHandler $this INFO "Starting $sh_script_b script" >> $log_file

if   [[ -n "$redirect_stdinerr" ]]; then
	$sh_script $params > $redirect_stdinerr 2>&1
	ret=$?
	MsgHandler $this INFO "STDOUT and STDERR were redirected to $redirect_stdinerr" >> $log_file
elif [[ -n "$append_stdinerr" ]]; then
	$sh_script $params >> $append_stdinerr 2>&1
	ret=$?
	MsgHandler $this INFO "STDOUT and STDERR were appended to $append_stdinerr" >> $log_file
else
	$sh_script $params >> $log_file 2>&1 
	ret=$?
fi

MsgHandler $this INFO "Finished $sh_script_b script. Code returned : $ret." >> $log_file

#-- Insert a record into the elf_monitor file. execTdSQL will insert this record
#-- into dw_monitor.elf_monitor via the elf_adm daemon, FlushMonitor.ksh
ELF_MON_FILE=$DW_LOG/.elf_monitor
if [[ ! -f $ELF_MON_FILE ]]; then
 touch $ELF_MON_FILE
 chmod 660 $ELF_MON_FILE
fi
ELF_MON_TBL="pp_interim.execTdSQL_monitor"

END_TS=$(date '+%Y-%m-%d %H:%M:%S')

(( ret )) && STATUS='F' || STATUS='S'

echo "insert into $ELF_MON_TBL values (111,'$sh_script','$START_TS','$END_TS',$ret,'$STATUS',$TD_LOGIN);" >> $ELF_MON_FILE
	 
#chmod 777 $ELF_MON_FILE >/dev/null 2>&1

#if (( ret )) ; then
#  msg="Error executing $sh_script. It returned error code : $ret. See $log_file for more details."
#	processException $msg 
#else
	MsgHandler $this INFO "Finished execShell.sh successfully" >> $log_file
#fi
MsgHandler $this INFO "Running statistics will be saved in pp_interim.execTdSQL_monitor table." >> $log_file

exit 0

