#!/bin/bash
# Usage: bash 1.sh [Country/ | Industry/] [-p]
# 一定要到Country的同级目录运行, -p表示是否带前缀
dir=$1
if [[ ${dir: 0-1: 1} == '/' ]]; then
	dir=${dir: 0: ((${#dir}-1))}
fi
for i in $(ls $1 | awk '{print $1}'); do
	# == 左右要空格
	if [[ $2 == '-p' ]]; then
		printf $i': '
	fi
	echo $(ls -l $(pwd)/$dir/$i | grep '^-' | wc -l)
done
