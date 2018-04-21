 #======================================================================
 # NAME : MsgHandler
 #======================================================================
 # TYPE : UNIX shell library
 # USAGE : Include the library somewhere at the beggining of your program 
 # . $DW_HOME/lib/MsgHandler.ksh
 #
 # ...
 #
 # MsgHandler <caller_prog> <msg_type> <msg_text> ['email']
 # where 
 # <caller_prog> = the name of the module calling the message handler
 # <msg_type> = one of 'ERROR','INFO','WARNING','DEBUG'
 # <msg_text> = the actual text of the message enclosed in double quotes
 # [email] = optional. If you want the message to be sent to DWDevelopersEmail (warnings)
 # or DWSupportEmail (errors)
 
 # Examples : 
 # MsgHandler foo.sh ERROR "this is an error" email
 # MsgHandler foo.sh WARNING "this is a warning" email
 # MsgHandler foo.sh INFO "this is an informational message"
 # MsgHandler foo.sh DEBUG "this is an debugging messag"
 #  
 # DESC : will print the message to STDOUT #
 # (e.g. I|20031223-18:17:23|Borrowlenses.sh|Starting baseline ...)
 #
 # NOTE : the following environment variables are assumed present :
 # $DWDevelopersEmail, $DWCoreEmail, $DWSupportEMAIL , $DWAllEmail, $servername , $DWErrorLog
 #
 # The caller program has to source /.dwProfile in order to set them
 # Modification History:
 #=======================================================================
 #-----------------------------------------------------------------
 #-- Function : USAGE
 #-----------------------------------------------------------------
 USAGE_msgh () {
 printf "\nUSAGE : MsgHandler <caller_prog> <msg_type> <msg_text> [email]\n"
 printf " : where \n"
 printf " : <caller_prog> = the name of the module calling the message handler\n"
 printf " : <msg_type> = one of 'ERROR','INFO','WARNING','DEBUG'\n"
 printf " : <msg_text> = the actual text of the message enclosed in double quotes\n\n"
 printf " : [email] = optional. If you want the message to be sent to DWCoreEmail (warnings)\n"
 printf " : or DWSuportEmail (errors)\n\n"
 printf " ex : MsgHandler foo.sh ERROR \"this is an error\" email\n"
 printf " ex : MsgHandler foo.sh WARNING \"this is a warning\" email\n"
 printf " ex : MsgHandler foo.sh INFO \"this is an informational message\"\n"
 printf " ex : MsgHandler foo.sh DEBUG \"this is an debugging message\"\n"
 }
 #-----------------------------------------------------------------
 #-- Function : MsgHandler
 #-----------------------------------------------------------------
 MsgHandler () {
 _caller_mh="$1"
 _msg_type_mh="$2" # has to be 'ERROR','INFO','WARNING','DEBUG'
 _txt_mh="$3"
 _email_flag_mh="$4"
 _vrt="$5" # vertical_ name
 _vrt_dl="$6" # Vertical_DL
 _loge="$7" # log_name
 _logn="$8" # script_name
 #-------------------------------------------------------------------
 #-- Check environment variables
 #-------------------------------------------------------------------
 if [ "$#" -lt 3 ]; then
 msg="Programming error in $_caller_mh. Module MsgHandler expects the at least 3 input params."
 printf "$msg\n"
 USAGE_msgh
 exit 101
 fi
 
 if [ aaa$DW_CORE_EMAIL = aaa -o aaa$DW_SUPPORT_EMAIL = aaa -o aaa$servername = aaa -o aaa$DW_ERROR_LOG = aaa ];then
 msg="Programming error in $_caller_mh. Module MsgHandler expects the following env variables to be set : $DWDevelopersEmail, $DWCoreEmail, $DWSupportEMAIL , $DWAllEmail, $servername , $DWErrorLog. The caller program has to source /.dwProfile in order to set them"
 printf "$msg\n"
 USAGE_msgh
 exit 101
 fi
 
 #-----------------------------------------------------------------
 #-- Process input params
 #-----------------------------------------------------------------
 find=`printf "$_msg_type_mh\n" |egrep -e "ERROR|INFO|WARNING|DEBUG"` if [ aaa$find = aaa ];then
 msg="Programming error in : $_caller_mh. Module MsgHandler expects one of the following values for msg_type : 'ERROR','INFO','WARNING','DEBUG'. Received : $_msg_type_mh"
 printf "$msg\n"
 USAGE_msgh
 exit 102
 fi
 
 if [ ! aaa$_email_flag_mh = aaa ]; then
 if [ ! "aaa$_email_flag_mh" = aaaemail ]; then
 msg="Programming error in : $_caller_mh. Module MsgHandler expects parameter <email_flag> to be passed as 'email'.\n"
 printf "$msg\n"
 USAGE_msgh
 exit 103
 fi
 fi
 
 if [ aaa$_msg_type_mh = aaaINFO ]; then
 msg_type_mh_s='I'
 elif [ aaa$_msg_type_mh = aaaERROR ]; then
 msg_type_mh_s='E'
 elif [ aaa$_msg_type_mh = aaaWARNING ]; then
 msg_type_mh_s='W'
 elif [ aaa$_msg_type_mh = aaaDEBUG ]; then
 msg_type_mh_s='D'
 fi 
 
 #-----------------------------------------------------------------
 #-- Print the message the STDOUT
 #-----------------------------------------------------------------
 now=`date '+20%y%m%d-%H:%M'`
 now_email=`date '+%m/%d/20%y-%H:%M'`  
 msg="${msg_type_mh_s}|${now}|${_caller_mh}|${_txt_mh}"
 printf "$msg\n"
 
 #-----------------------------------------------------------------
 #-- Set unpassed params to DEFAULT
 #-----------------------------------------------------------------
 
 if [ aaa$_loge = aaa ];
 then
 _loge="/export/home/pp_adm/lib/default_handler.log"
 else
 echo "The log file is ${_loge}"
 fi
 
 if [ aaa$_logn = aaa ];
 then
 _logn="/export/home/pp_adm/lib/default_handler.script"
 else
 echo "The script name is ${_logn}"
 fi
 
 if [ -e ${_loge} ]
 then
 echo "The logfile ${_loge} exists"
 else
 echo "The default log file will be emailed"
 fi
 
 
 #-----------------------------------------------------------------
 #-- Send email to support team if ERROR
 #-----------------------------------------------------------------
 if [ ! aaa$_email_flag_mh = aaa ]; then #{
 email_subject="$_caller_mh : $now_email"
 if [ aaa$_msg_type_mh = aaaERROR ]; then #{
 `(echo "Msg Type : $_msg_type_mh\nServer : $servername\nProgram : $_caller_mh\nTime : $now_email\n\nMsg Text : $_txt_mh\n";unix2dos -437 $_loge | uuencode $_logn.log) | mailx -s "$_vrt : DW $_msg_type_mh : $servername : $email_subject" -c $DWOpsEmail $_vrt_dl`
 #Server : $servername
 #Program : $_caller_mh
 #Time : $now_email
 #Msg Text : $_txt_mh
 
 #EOE
 else
 
 #-----------------------------------------------------------------
 #-- Send email to dwcore team if any other message type
 #-----------------------------------------------------------------
 mailx -s "DW $_msg_type_mh : $servername : $email_subject" $DW_CORE_EMAIL <<EOW Msg Type : $_msg_type_mh
 Server : $servername
 Program : $_caller_mh
 Time : $now_email
 
 Msg Text : $_txt_mh
 
 EOW
 fi #}
 fi #}
 
 #-----------------------------------------------------------------
 #-- Add the message to the central error log (~dw_adm/error.log) if it's an error
 #-----------------------------------------------------------------
 if [ aaa$_msg_type_mh = aaaERROR ]; then
 echo $msg >> $DW_ERROR_LOG
 fi
 }
