#!/bin/bash
# usage: bash train_div.sh [div_nums]
# div_nums是指将train的数据集均分拆分成几个文件
if (($# == 0)); then
	DIV_NUMS=1
fi
DIV_NUMS=$1

# 1分类中的行数

TRAIN_1_LINES=$(cat data/train/1/* | wc -l)
# 0分类中的行数
TRAIN_0_LINES=$(cat data/train/0/* | wc -l)

# (TRAIN_1/0_LINES, 1/0)
function div_func() {
	# 拆分数大于行数
	if (($DIV_NUMS > $1)); then
		echo "error: the lines num lower than div nums"
		return -1
	fi
	cat data/train/$2/* > train_$2_all.txt
	split --additional-suffix=.txt --numeric-suffixes=1 -n l/$DIV_NUMS  train_$2_all.txt train_$2_
	rm train_$2_all.txt
}

div_func $TRAIN_1_LINES 1
div_func $TRAIN_0_LINES 0
