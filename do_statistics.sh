#!/bin/bash
# usage: bash do_statistics.sh
echo "" > statistics.txt
SHELL_FILE_LOC_PATH=$(pwd)
if [[ ! -e FRA || ! -e INDIA ]]; then
	echo 'error: the dirs of FRA or INDIA no exist'
	exit -1
fi

for dir in FRA INDIA; do
	cd $SHELL_FILE_LOC_PATH/$dir
	for file in $(ls); do
		if [[ $dir == FRA ]]; then
			echo $(pwd)/$file$'\t'0 >> $SHELL_FILE_LOC_PATH/statistics.txt
		else
			echo $(pwd)/$file$'\t'1 >> $SHELL_FILE_LOC_PATH/statistics.txt
		fi
	done
done

