if [[ ! -e statistics_shf.txt ]]; then
	echo 'error: no exists statistics_shf.txt file'
	exit -1
fi
n=$(cat statistics_shf.txt | wc -l)
train_n=$(( $n * 7 / 10))
test_n=$(( $n - $train_n ))
head -n$train_n statistics_shf.txt > train_list_shf.txt

tail -n$test_n statistics_shf.txt > test_list_shf.txt
