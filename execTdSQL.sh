#! /bin/ksh
this=`basename $0`
# this file is best viewed with tabstop=2
#==========================================================================================
# Filename:    execTdSQL.sh
# Description: wrapper around the Teradata bteq command line interface utility. 
#              Given a file as input parameter, usually .sql suffixed, it takes the 
#              file and it runs it against the default TD database server ($DWDB) , connecting as dw_batch[_dev]
#              The script will also insert a record in a table called gdw_tables[_dev].execTdSQL_monitor
#              for each run, recording the start and end times , the status (S-successful, F-failed),
#              the exit error code and the Teradata session number.
#
# Called By:    from Unix or informatica pre/post-session
USAGE_extdsql () {
cat << EOF
#
# USAGE : $this help    <-- will give you a full description
#       : $this <SQL script> [<var_name>=<value>] [<var_name>=<value>] ..."
#       :  [on_exception=<no_email|warning|warning,no_email>]"
#       :  [charset=UTF8]"
#       :  [dbuser=tddw_core]"
# WHERE : 
#       : <SQL script> = the script that needs to be sent to Teradata (full path)
#       : <var_name>=<value> = parameters that are used in the SQL script and need to 
#       :                      be replaced at run time. 
#       : E.g. in the SQL script, you can have : ... where prd_id = $PRD_ID
#       : and then when you invoke $this, you would go :
#       : $this $DW_SQL/prog.sql PRD_ID=2004M07
#       : <no_email|warning|warning,no_email> = the default behavior in case an exception occurs is :
#       :  error,email (raise an error, send email to DW_BACK_END)
#       :  You can bypass this, by invoking it with the 'on_exception'
#       :  parameter. The accepted options are :
#       : on_exception=no_email
#       : on_exception=warning
#       : on_exception=warning,no_email
#       : <charset> = In case the query needs UTF8 support, you would pass this option as charset=UTF8 
#       : If not passed, the default charset is used (ASCII)
#       : <log_dir> = This script produces a log file in $DW_LOG. In case you need to override that
#       you can pass this option as : <log_dir> = <some other directory>
#       : <dbuser> = By default the script would login to database as PP_MERCURY. It can be changed by 
#       passing this parameter as: <dbuser>=<logical name from pass.dat>
# EXAMPLES 
#       : $this $DW_SQL/foo.sql PRD_ID='2004M01'"
#       : $this $DW_SQL/bar.sql FROM_DATE=\$FROM_DATE TO_DATE=\$TO_DATE"
#       : $this $DW_SQL/bar.sql on_exception=warning,no_email"
#       : $this $DW_SQL/bar.sql charset=UTF8"
#       : $this $DW_SQL/foo.sql param1=value param2=value on_exception=warning
#       : $this $DW_SQL/foo.sql param1=value param2=value on_exception=warning,no_email
#       : $this $DW_SQL/foo.sql param1=value param2=value charset=UTF8
#       : $this $DW_SQL/foo.sql param1=value param2=value log_dir=$DW_LOG/my_project
#       : $this $DW_SQL/foo.sql param1=value param2=value dbuser=tddw_core
#       :
# NOTES : [a] means this param is optional. <a|b|c> means a OR b OR c
#
EOF
}
#         
# Return : 101 - incorrect usage.Incorrect nr of params.
#          102 - incorrect usage. '=' not found in one of the params 
#          103 - SQL script does not exist or is not readable 
#          104 - bteq returned non-zero
#          105 - Cannot find entry for $dbuser in pass.dat     (Added by Rajesh Kotecha, Dec 02, 08)
#
# Modification History:
#                             
#==============================================================================
. $HOME/.dwProfile_ppadm
. $DW_HOME/lib/new_MsgHandler.ksh

#------------------------------------------
#-- Functions
#------------------------------------------
HELP () {
eq_line=0
cat $0|while read line
do
  no_eq=`echo $line|grep "^#===="> /dev/null;echo $?`
  if (( no_eq == 1 )); then 
    if (( eq_line == 0 )); then
      continue;
    else
      printf "$line\n"|egrep -ve "{|}|EOF"
    fi
  else
    printf "$line\n"|egrep -ve "{|}|EOF"
    (( eq_line = eq_line + 1 ))
  fi  
  if (( eq_line == 2 )); then
    break
  fi
done
}
	
processException () {
  if [[ -z "$on_exception" ]];then
   	MsgHandler $this "ERROR" "$*" "email" $vrt $vrt_dl $log_file $file_prefix | tee -a $log_file  # new logic

 	elif [[ "$on_exception" == "no_email" ]];then
   	MsgHandler $this ERROR "$*" |tee -a $log_file

 	elif [[ "$on_exception" == "warning" ]];then
   	MsgHandler $this WARNING "$*" email $vrt $vrt_dl $log_file $file_prefix |tee -a $log_file  # new logic

 	elif [[ "$on_exception" == "warning,no_email" ]];then
   	MsgHandler $this WARNING "$*" |tee -a $log_file
 	fi
}
#------------------------------------------
#-- Process input parameters - nr. of params
#------------------------------------------
if [[ "$1" == "help" ]]; then
	HELP
	exit 0
fi

nr_parms=$#
if (( nr_parms < 1 )) ; then
		msg="Incorrect number of parameters. Command was : $this $*"
		processException $msg 
		USAGE_extdsql
		exit 101
fi

sql_script=$1
sql_script_b=`basename $1`
sql_script_d=`dirname $1`
sql_script_t=`basename $sql_script_d`
if [[ $sql_script_t == "." || $sql_script_t == ".." ]]; then
      sql_script_t="UNKNOWN"
fi

now=`date '+20%y%m%d_%H-%M-%S'` # new logic
file_prefix=${this%.sh}.${sql_script_b%.sql}.$now.$$

temp_sql_file=$DW_HOME/tmp/$file_prefix.sql
temp_ksh_file=$DW_HOME/tmp/$file_prefix.ksh
temp_pre_log_file=$DW_HOME/tmp/$file_prefix.temp_pre.log
temp_post_log_file=$DW_HOME/tmp/$file_prefix.temp_post.log

#----------------------------------------
#-- Start building the temp ksh file
#----------------------------------------
cat<<EOC > $temp_ksh_file
#! /bin/ksh

. $HOME/.dwProfile_ppadm

EOC

#------------------------------------------
#-- Process input params - value and type of params
#------------------------------------------
sql_script=$1
shift

params="$*"

i=0
for p in $params;do
	eq=`echo $p|grep = >/dev/null 2>&1;echo $?`
	if (( eq != 0 )); then
		msg="One of the parameters does not have '=', or there are extra spaces on the command line. Command was : $this $sql_script $*"
		processException $msg 
		USAGE_extdsql
		exit 102
	fi
	arr[$i]="$p"

	name=`echo ${arr[$i]%%=*}`
	value=`echo ${arr[$i]##*=}`

	eq_vars=`echo ${arr[$i]}|egrep -e "on_exception=|charset=|log_dir=|dbuser=|final_table=" >/dev/null 2>&1;echo $?`

	if (( eq_vars == 0 )); then
		eval `echo $name=$value`
	fi
	
	echo "$name=$value" >> $temp_ksh_file
	(( i = i + 1 ))
done    

if [[ -n $log_dir ]]; then
	DW_LOG=$log_dir
	if [[ ! -w $log_dir ]]; then
		msg="Can not write to specified log_dir $log_dir"
		processException $msg 
		exit 106
	fi
fi

log_file=$DW_LOG/$file_prefix.log
#----------------------------------------
#-- Can I read the sql_script ?
#----------------------------------------
if [[ ! -r $sql_script ]];then
	msg="Can not read file: $sql_script"
	processException $msg 
	exit 103
fi


MsgHandler $this INFO "Starting $this script\n" > $log_file

#------------------------------------------
#-- Default charset
#------------------------------------------
if [[ -z $charset ]]; then
	echo "charset=ASCII" >> $temp_ksh_file
fi
if [[ ! -n $dbuser ]]; then
        vrt=PP_MERCURY   # default vertivcal name
        vrt_dl=$DW_SUPPORT_EMAIL  # default DL
else
        DWDB=`grep "^$dbuser:" $DW_ADMIN/pass.dat | cut -d: -f2`
        vrt=`grep "^$dbuser:" $DW_ADMIN/pass.dat |cut -d: -f3` # vertical name
        vrt_dl=`grep "^$dbuser:" $DW_ADMIN/pass.dat | cut -d: -f4` # vertical DL
if [[ -z $vrt ]]; then     
        vrt=PP_MERCURY   #if batch id exists and vrt name does not exist default to PP_MERCURY
fi
if [[ -z $vrt_dl ]]; then
       vrt_dl=$DW_OPS_EMAIL
#$DW_OPS_EMAIL #if batch id exists and vrt_dl does not exists default to dl-pp-dm-ops@ebay.com
fi
fi        
if [[ -z $DWDB ]]; then
        msg="Can not find $dbuser in pass.dat."
        processException $msg
        exit 105
fi

if [[ -n $final_table ]]; then
      sql_script_t=$final_table
fi

#------------------------------------------
#-- Global variables 
#------------------------------------------
cat<<EOC>>$temp_ksh_file
sql_script=$sql_script
sql_script_b=$sql_script_b
sql_script_t=$sql_script_t
temp_pre_log_file=$temp_pre_log_file
temp_post_log_file=$temp_post_log_file
log_file=$log_file
temp_sql_file=$temp_sql_file
DWDB=$DWDB
EOC
#------------------------------------------
#-- pre_BTEQ commands
#-- This should be replaced with a stored proc
#-- later :
#-- ins_execTdSQL_monitor <sql_script_name> 
#-- (returns the current run_sequence, -1 if 
#-- the insert fails)
#------------------------------------------
cat<<-'EOC'>> $temp_ksh_file
# echo "---------------------------"
 #echo "-- Start of pre_BTEQ commands"
#echo "---------------------------"
#bteq<<EOB>$temp_pre_log_file 2>&1
#.logon $DWDB
#BT;

EOC

#------------------------------------------
#-- Put the pre_SQL, SQL script and post_SQL in. 
#-- Take any .QUIT; .EXIT; out of the SQL script
#------------------------------------------
cat<<-'EOT'>> $temp_ksh_file
cat<<EOF>>$temp_sql_file
--------------------------------
-- Start of pre_SQL statements
--------------------------------
select 'SESS', session;
--------------------------------
-- End of pre_SQL statements
--------------------------------
--------------------------------
-- Start of $sql_script_b
--------------------------------
EOT

cat $sql_script|sed -e 's/^\.*[Ee][Xx][Ii][Tt].*//g' -e 's/^\.*[Qq][Uu][Ii][Tt].*//g' -e 's/^\.[Ll][Oo][Gg][Oo][Ff][Ff].*//g' >> $temp_ksh_file

cat<<-'EOT'>> $temp_ksh_file
--------------------------------
-- End of $sql_script_b
--------------------------------
--------------------------------
-- Start of post_SQL statements
--------------------------------
--------------------------------
-- End of post_SQL statements
--------------------------------
.EXIT ERRORCODE;
EOT
echo "EOF" >> $temp_ksh_file

#------------------------------------------
#-- Put the bteq invocation in
#------------------------------------------
cat<<-'EOB'>> $temp_ksh_file
bteq<<EOBTEQ>>$log_file 2>&1
.set session charset "$charset"
.logon $DWDB
SET QUERY_BAND = 'Table=$sql_script_t; Script=$sql_script_b;' FOR SESSION;
.RUN FILE=$temp_sql_file
.EXIT ERRORCODE;
EOBTEQ
#real_err=`grep  "RC (return code) =" $log_file|sed -e 's/^..*=//g'`
EOB


#------------------------------------------
#-- post_BTEQ commands
#-- This should be replaced with a stored proc
#-- later :
#-- upd_execTdSQL_monitor <run_sequence> <error_code>
#-- (returns 0, -1 if the update fails)
#------------------------------------------
cat<<-'EOC'>> $temp_ksh_file

EOC
#------------------------------------------
#-- Execute the ksh script
#------------------------------------------
chmod +x $temp_ksh_file
$temp_ksh_file >> $log_file 2>&1

ksh_err=$?

if (( ksh_err != 0 )) ; then
	#----------------------------------
	#-- Grab the real return code (bteq, the Unix
	#-- process returns a different code)
	#-----------------------------------
	real_err=`grep  "RC (return code) =" $log_file|sed -e 's/^..*=//g'`
        echo "Inside if real_err:"$real_err     
	msg="Error executing $sql_script.
Bteq returned error code : $real_err.
See the attached log file $log_file for more details."
	processException "$msg"    # new logic
	exit 104
fi

MsgHandler $this INFO "Finished successfully." >> $log_file

\rm -f $temp_ksh_file
\rm -f $temp_sql_file

exit 0

