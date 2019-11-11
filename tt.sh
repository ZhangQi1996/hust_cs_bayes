#!/bin/bash
if [[ ! -e shf_div.sh ]]; then
	echo 'error: no exists shf_div.sh, tips: cur dir should under the same dir as tt.sh'
	exit -1
fi
rm -r data
mkdir -p 'data/train/0' 'data/train/1' 'data/test/0' 'data/test/1'
cat test_list_shf.txt | while read arr; do
	cls=${arr#*$'\t'}
	path=${arr%$'\t'*}
	file=${path##*/}
	cp $path data/test/$cls/$file
done

cat train_list_shf.txt | while read arr; do
	cls=${arr#*$'\t'}
	path=${arr%$'\t'*}
	file=${path##*/}
	cp $path data/train/$cls/$file
done
