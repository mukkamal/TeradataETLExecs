#!/bin/ksh
#########################################################################
#                                                                                                                                                                     #
#                                                                                                                                                                     #
#----------------------------------------------------------------------------------------------------------------------------#
#  Description: Script for FASTLOAD                                                                                                       #
#                                                                                                                                                                     #
#----------------------------------------------------------------------------------------------------------------------------#
#  Modification Log                                                                                                                                      #
#----------------------------------------------------------------------------------------------------------------------------#
#  Changed By    Changed Date    Change Summary                                                                         #
#----------------------------------------------------------------------------------------------------------------------------#
#########################################################################
_help() {
   echo
   echo "execTdFLD.sh"
   echo
   echo "This script will generate a fast-load script given the database name and table name on Teradata."
   echo "You can create a static fload or you can pipe the output to fload - a dynamic fload."
   echo 
   echo
   echo "Options are:"
   echo
   echo "This script will NOT attempt to use the floadcfg.dat file."
   echo " -axsmod - specify this when you want to process a delimited file with quotes."
   echo " -indic - specify this when you are using the axsmod mode and you want to allow indicators."
   echo " -quoted <c> - when using axsmod specify quote symbol for quoted fields."
   echo "  if there are no quoted fields then set -quoted \"\". Default is quoted when using axsmod"
   echo " -d specifies the database"
   echo " -t specifies the tablename"
   echo " -tenacity specifies the fastload tenacity. Default is from profile \$TENACITY."
   echo " -errlimit specifies the fastload errlimit. Default is from profile \$ERRLIMIT."
   echo " -sessions specifies the max/min number of fastload sessions to use. Default is profile \$MAX_SESS_SMALL"
   echo "  and \$MIN_SESSIONS."
   echo " -record specify the fastload record mode (defaults to VARTEXT unless using -ml )."
   echo " -truncate - delete the target table prior to loading"
   echo " -delimiter - specifes the delimiter character to use, default is pipe \"|\" "
   echo " -f specifies the path and file name of the input data file."
   echo " -l (optional) is the logon string:  \"tdid/uid,pwd\". If not given defaults to \$LOGON_DIR/$TD_SEC"
   echo " -i Your name or initials (optional)"
   echo " -mp convert db2 method p."
   echo "    converting from method P in db2 might look like: -mp \"5:1,4,5\", which will be "
   echo "    interpreted to 5 fields in the input file but fields 1, 4 & 5 as the fields to load. "
   echo " -ml convert db2 method l. "
   echo "    converting from method L in db2 might look like: -ml \"1 3,4 5,10 12\", "
   echo "    which will be interpreted to 3 fields with input fields defined as col 1 for 3, col 4 dor 5 and so on."
   echo "    The number of columns generated is equal to the total number pairs in this case. "
   echo
   echo "examples"
   echo 
   echo "Load a file that is delimited and quoted and use inidicator mode"
   echo
   echo " execTdFLD.sh -d STG -t FOO -f \$TMP/file.dat -axsmod -quoted \"'\" -indic -delimiter \",\"" 
   echo 
   echo "Generate a one to one load using the table layout in dbc.tables and match to the file layou."
   echo
   echo " execTdFLD.sh -d STG -t FOO -f \$TMP/file.dat" 
   echo
   echo "Generate a fastload for table STG.FOO using a  20 field record. The 3rd, 5th and 6th fields from a data file will be used. The remaining 17 fields will be marked as filler. "
   echo "Fields 3, 5 and 6 will be matched sequentially to the table being loaded. It is up to you to pick the correct fields to match to the table."
   echo 
   echo " execTdFLD.sh -mp \"20:3,5,6\" -d STG -t FOO  -f \$TMP/file.dat"
   echo 

   exit 8 
}

function _exit {
  echo "$1" 1>&2
  echo "$0 terminated" 1>&2
  echo "">&2
  test -f $TEMP_DIR/$$.bteq.dat && cat $TEMP_DIR/$$.bteq.dat
  rm -f $TEMP_DIR/$$.*.dat 
  exit 8
}

export TEMP_DIR="/tmp"
export BTEQ_TEMP_DIR=$TEMP_DIR
typeset -x INITS=$(who am i)
METHOD=""

test -z "$TD_SEC" && export TD_SEC="td.sec"
#TD_SEC=fload_logon_file$$.sec
#rm -f $LOGON_DIR/$TD_SEC
#mkfifo $LOGON_DIR/$TD_SEC
#chmod 600 $LOGON_DIR/$TD_SEC

export LOGON=".run file=$LOGON_DIR/$TD_SEC;"
export WHENCE=$(dirname $(whence $0))
export TRUNCATE=""
export RECORD="VARTEXT"
export CAST_AS="VARCHAR"
export DELIM="|"
export AXSMOD_DLM=$DELIM
export MAX_SESSIONS=$MAX_SESS_SMALL
export EMPTYSTR=" "
export INDIC="n"
export QUOTED="-QUOTED_STRINGS \""
while [[ $1 = -* ]]
do
	case $1 in
        -delimiter ) typeset -x DELIM='"'$2'"'
          AXSMOD_DLM=$2         
          shift;;
	-truncate ) typeset -x TRUNCATE="Y";;
	-axsmod ) export AXSMOD=1
		export EMPTYSTR="";;
	-record ) typeset -x RECORD=$2
	        shift;;
        -lc )   typeset -x connection=$2
                shift;;
	-date ) typeset -x DATE_FMT=$2
                shift;;
	-timestamp ) typeset -x TS_FMT=$2
                shift;;
        -indic ) export INDIC="y";;
        -quoted ) if [[ ! -z "$2" ]]
	  	  then
			export QUOTED="-QUOTED_STRINGS $2"
		  else
			export QUOTED=""
		  fi
                  shift;;
        -f )    typeset -x DATAFILE=$2
                shift;;
	-i )    typeset -x INITS=$2
		shift;;
	-l )    typeset -x LOGON=".logon $2;"
					shift;;
	-d )	typeset -x DB_TB_STG=$2
		shift;;
        -sessions )  echo $2 | read MAX_SESSIONS MIN_SESSIONS
	        export MAX_SESSSIONS
		export MIN_SESSIONS
	        shift;;
        -tenacity ) typeset -x TENACITY=$2
	        shift;;
        -errlimit ) typeset -x FLOAD_ERRLIMIT=$2
	        shift;;
	-t ) 	typeset -x TABLE=$2
		typeset -xl SPOOL=$TABLE
                E1=$(echo $TABLE | cut -b  1-25)_ERR1
                E2=$(echo $TABLE | cut -b  1-25)_ERR2
		shift;;
        -mp )    
                METHOD="P"
                if echo $2 | grep ":" > /dev/null
                then
                  typeset -i NBR_FIELDS=$(echo $2 | cut -d: -f 1)
                  POSITIONAL=$(echo $2 | cut -d: -f 2 | sed 's/,/ /g')
                else
                  _exit "-mp command is incorrect. nx:n1,n2,nx"
                fi
                shift;;
        -ml )    
                METHOD="L"
		POSITIONAL=$2
                echo $POSITIONAL | perl -ne 'print tr/\,/\,/'  | read NBR_FIELDS
                shift;;

  	-h )    _help;;
       	*  ) 	_exit "Invalid option - $1"
	esac
	shift

done

#if [ -z "$connection" ];then connection=$LOGON_CONNECTION
#fi
#
#if [ -z "$connection" ];then _exit "connection name not passed via the -lc switch and variable \$LOGON_CONNECTION has not been set, please do one or the other"
#fi
#
#get_logon_info.ksh $connection $LOGON_DIR/$TD_SEC td || _exit "Connection error check the details of the connection named \"$connection\" in logonmgr"
#chmod 400 $LOGON_DIR/$TD_SEC

if [[ -z "$DATAFILE" ]]
then
    _exit "\$DATAFILE not set - use the -f option"
fi

#if [[ ! -z "$AXSMOD" ]]
#then
#	export AXSMOD="AXSMOD $OUTMOD_DIR/libamrmulf.so '-FILE_NAME $DATAFILE  -DELIMITER $AXSMOD_DLM $QUOTED -INDICDATA $INDIC'"
#else
#	if [[  "$INDIC" == "y" ]]
#	then
#		_exit "-indic can only be used with -axsmod"
#	fi	
#fi

if [[ -z "$DB_TB_STG" ]]
then
    _exit "\$DB_TB_STG not defined."
fi

if [[ -z "$TENACITY" ]]
then
    _exit "\$TENACITY not defined."
fi


if [[ -z "$SLEEP" ]]
then
    _exit "\$SLEEP not defined."
fi


if [[ -z "$FLOAD_ERRLIMIT" ]]
then
    _exit "\$FLOAD_ERRLIMIT not defined."
fi

if [[ -z "$DB_TB_UTL" ]]
then
    _exit "\$DB_TB_UTL not defined."
fi


if [[ -z "$MIN_SESSIONS" ]]
then
    _exit "\$MIN_SESSIONS not defined."
fi

if [[ -z "$MAX_SESSIONS" ]]
then
    _exit "\$MAX_SESSIONS not defined."
fi

test -z "$TABLE" &&  _help 

if [[ ! -z $TRUNCATE ]]
then
    export TRUNCATE="delete from $DB_TB_STG.$TABLE all;"
fi

if [[ $METHOD == "L" ]] || [[ $RECORD == "TEXT" ]]
then
    export RECORD="TEXT"
    export CAST_AS="CHAR"
    export DELIM=""
fi

if [[ -n "$AXSMOD" ]]
then
	export COL_COUNT=$(execTdBTEQ.sh -xv "select trim(count(*)) from dbc.columns where tablename='$TABLE' and databasename='$DB_TB_STG';" 2>/dev/null)
	test $COL_COUNT -gt 0 || _exit "Cannot determine number of columns."
	export AXSMOD="AXSMOD $OUTMOD_DIR/libamrmulf.so '-FILE_NAME $DATAFILE  -DELIMITER $AXSMOD_DLM $QUOTED -INDICDATA $INDIC -FIELD_COUNT $COL_COUNT'"
else
	if [[  "$INDIC" == "y" ]]
	then
		_exit "-indic can only be used with -axsmod"
	fi	
fi

if [[ ! -z "$DATE_FMT" ]]
then
    export DATE_FMT="(DATE, FORMAT ''${DATE_FMT}'')"
fi

if [[ ! -z "$TS_FMT" ]]
then
    export TS_FMT="(TIMESTAMP(0), FORMAT ''${TS_FMT}'')"
fi

export BASE_DB=$DB_TB_STG
export BASE_TABLE=$TABLE

execTdBTEQ.sh <<EOF > $TEMP_DIR/$$.bteq.dat 2>&1

  .set maxerror 1

   abort 'Requested table, "$BASE_DB.$BASE_TABLE", does not exist.' where 0 = (select count(*)  from dbc.columns where tablename='$BASE_TABLE' and databasename='$BASE_DB');

   .set width 254
   .export file=$BTEQ_TEMP_DIR/$$.layout.dat

    select case when row_number() over(partition by tablename order by columnid)=1 then '   ' else '  ,' end || trim(lower(columnname)) || 
          '  ($CAST_AS(' || 
          trim(case columntype when 'CV' then columnlength 
               when 'CF' then columnlength 
               when 'DA' then character(trim(columnformat))
               when 'TS' then columnlength 
               when 'D' then  DecimalTotalDigits + 3 
               when 'I' then 11 
               when 'I1' then 1
               when 'F' then 30
               when 'AT' then 10 
               when 'I2' then 7
               when 'I8' then 21
              else null end (format 'ZZZZZZZZZZZZZZZZZ9')) 
           || case when columntype in ('CV','CF') then ')) ' else '), NULLIF=''$EMPTYSTR'')' end (title '')
     from dbc.columns
    where tablename='$BASE_TABLE' and databasename='$BASE_DB'
    order by columnid;


   .export reset


   .export file=$BTEQ_TEMP_DIR/$$.insert.dat

  select 
  		case when row_number() over (partition by tablename order by columnid) =1
		      then  ' :'
		      else  ',:'
		 end 
		 || (trim(columnname)) ||
		 case when columntype = 'DA'
		      then '${DATE_FMT}'  
		      when columntype = 'TS'
		      then '${TS_FMT}'  
		      else ' '
		 end
	 (title '')
  from dbc.columns
  where tablename='$BASE_TABLE' and databasename='$BASE_DB'
  order by columnid;

  .export reset

   .export file=$TEMP_DIR/$$.cols.dat

  select case when row_number() over (partition by tablename order by columnid) =1
              then  (trim(columnname))
              else  ','|| (trim(columnname)) 
         end (title '')
  from dbc.columns
  where tablename='$BASE_TABLE' and databasename='$BASE_DB'
  order by columnid;

  .export reset

  .quit

EOF
 

test $? != "0" &&   _exit

#chmod 600 $LOGON_DIR/$TD_SEC
#get_logon_info.ksh $connection $LOGON_DIR/$TD_SEC td || _exit "Connection error check the details of the connection named \"$connection\" in logonmgr"
#chmod 400 $LOGON_DIR/$TD_SEC


if [[  $METHOD == "P" ]]
then
   rm $TEMP_DIR/$$.layout.dat 
   rm $TEMP_DIR/$$.insert.dat

   typeset -i y=1

   #init the array for all layout fields
   layout[$y]="FILLER_$y (VARCHAR(254))" 
   while (( $y <= $NBR_FIELDS ))
   do
     layout[$y]=",FILLER_$y (VARCHAR(254))" 
     y=$y+1       
   done
   

   typeset -i y=1
   #replace filler in layout with real fields
   for i in $POSITIONAL
   do
     if [ $y -gt 1 ]
     then
       insert[$y]=",FIELD_$i"
       layout[$i]=",FIELD_$i (VARCHAR(254))"
     else
       insert[$y]="FIELD_$i"
       if [ $i -eq 1 ]
       then 
           layout[$i]="FIELD_$i (VARCHAR(254))"
       else
           layout[$i]=",FIELD_$i (VARCHAR(254))"
       fi 
     fi
     y=$y+1       
   done   



   typeset -i y=1
   #output layout and insert to files
   while (( $y <= $NBR_FIELDS))
   do
     echo ${layout[$y]} >> $TEMP_DIR/$$.layout.dat
     if [ ! -z "${insert[$y]}" ]
     then 
        echo ${insert[$y]} | sed 's/FIELD/:FIELD/' >> $TEMP_DIR/$$.insert.dat
     fi
     y=$y+1
   done  

   #count rows
   wc -l $TEMP_DIR/$$.insert.dat | cut -f1 -d" " > $TEMP_DIR/$$.inscount.dat
   wc -l $TEMP_DIR/$$.cols.dat | cut -f1 -d" " > $TEMP_DIR/$$.colcount.dat

   if ! diff $TEMP_DIR/$$.inscount.dat $TEMP_DIR/$$.colcount.dat >/dev/null 2>&1
   then
     echo 
     echo
     _exit ERROR: Number of columns requested in -p option is $(cat $TEMP_DIR/$$.inscount.dat) but $DB_TB_STG.$TABLE has $(cat $TEMP_DIR/$$.colcount.dat) columns
   fi
elif [[ $METHOD == "L" ]]
then
  $WHENCE/method_l.pl -c "$TEMP_DIR/$$.insert.dat" -l "$TEMP_DIR/$$.layout.dat" -p  "$POSITIONAL"  
fi

echo "Now generating Fast-Load script" 1>&2

cat<<EOF
/*****************************************************************/
/*  $SPOOL.fld                                                   */
/*                                                               */
/*     By        Date       Notes                                */
/*                                                               */
/*   $INITS       $(date +'%m/%d/%y')     Initial                */
/*                                                               */
/*                                                               */
/*****************************************************************/

sessions $MAX_SESSIONS $MIN_SESSIONS;

tenacity $TENACITY;

errlimit $FLOAD_ERRLIMIT;

.run $LOGON_DIR/$TD_SEC;

$TRUNCATE

$AXSMOD

.SET RECORD $RECORD $DELIM DISPLAY_ERRORS;

define

$( cat $TEMP_DIR/$$.layout.dat)
file=$DATAFILE;

show;

begin loading 
	${DB_TB_STG}.${TABLE}
errorfiles 
		${DB_TB_UTL}.$E1,
		${DB_TB_UTL}.$E2;

insert into ${DB_TB_STG}.${TABLE}
(
$(cat $TEMP_DIR/$$.cols.dat)
)
values
(
$(cat $TEMP_DIR/$$.insert.dat)
);

end loading;

logoff;

EOF



rm -f $TEMP_DIR/$$.*.dat 


