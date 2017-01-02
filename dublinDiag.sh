#!/bin/bash

nicInUse=$(route get 10.19.0.1 | grep interface | awk '{print $2}')
user=$(who am I | awk '{print $1}')
file=(/Users/$user/Desktop/dataLog-$user.txt)
ipToTest=(10.19.0.2 10.19.0.3 10.19.254.254 10.19.254.253 10.19.5.25 10.19.0.1 8.8.8.8 10.2.5.27)
touch $file

echo "----------" >> $file
echo "NIC in use: $nicInUse" >> $file
ifconfig $nicInUse >> $file

for i in ${ipToTest[@]}; do
	testResult=$(ping -qc 1 $i)
	if [ $? = 0 ] ; then
		echo "----------" >> $file
		echo "$i pass" >> $file
	else
		echo "----------" >> $file
		echo "$i FAIL; results:" >> $file
		echo $testResult >> $file
	fi
done

cat $file | pbcopy
rm $file

echo "Results of the test are on your clipboard: please paste in an email to Ze!"
