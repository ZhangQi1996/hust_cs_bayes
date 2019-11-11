cat test_list_shf.txt | while read line; do
	r=${line##*/}
	echo $r >> test_true.txt
done
