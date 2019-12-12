set -e
temp_file1=$1
first_char=$(tail -n1 $1)
echo "first_char is: ${first_char}"
if [[ ${first_char} == ''  ]]
then 
	sed -i '$d' $1 > $1_1
	temp_file1=$1_1
else 
	echo "No ^Z character"
fi

temp_file2=$1_2
echo "Going to replace NUL, SI and SO characters with space"
sedcommand=""
# list here the characters in hexadecimal (man ascii) separated by a space
for s in 00 0e 0f
do
    if [ -z "$sedcommand" ]
    then
        sedcommand="sed -e 's/\\x$s/ /g'"
    else
        sedcommand="$sedcommand -e 's/\\x$s/ /g'"
    fi
done

sedcommand="$sedcommand ${temp_file1} > ${temp_file2}"
echo "Going to execute: $sedcommand"
eval $sedcommand
echo "Characters substitution completed"
if [[ -e $1_1 ]]
then
	rm $1_1
fi

temp_file3=$1_3
echo "Fixed to Delim Conversion : $5"
if [ $5 = true ]
then
	command="LC_ALL=$6 /usr/bin/awk -v FIELDWIDTHS='$7' -v OFS='' '{ "\$1"="\$1" ""; print }' ${temp_file2}  > ${temp_file3}"
	echo "Going to execute command: $command"
	eval $command
	rm $1_2
	mv $1_3 $1_1
	echo "Fixed to Delim Conversion completed"
else
	mv $1_2 $1_1
fi