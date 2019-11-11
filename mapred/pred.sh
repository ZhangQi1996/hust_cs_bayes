# 这个是用来统计out.txt中的成功率的
sum=0
right=0
true_pos=0
false_pos=0
true_neg=0
false_neg=0
for i in $(awk '{print substr($2, 12)substr($3, 12)$NF}' -); do
	true_class=${i:0:1}
	pred_class=${i:1:1}
	is_right=${i:2:1}
	if (( $pred_class == 1 )); then
		if (( $true_class == $pred_class )); then
			(( true_pos += 1 ))
		else
			(( false_pos += 1 ))
		fi
	elif (( $true_class == $pred_class )); then
		(( true_neg += 1 ))
	else
		(( false_neg += 1 ))
	fi	
	if (( $is_right == 1 )); then
		(( right += 1))
	fi
	(( sum += 1 ))
done
echo 'True Postives: '$true_pos
echo 'False Postives: '$false_pos
echo 'True Negatives: '$true_neg
echo 'False Negatives: '$false_neg
P=$(echo "scale=2; $true_pos / ($true_pos + $false_pos)" | bc) # 小数
R=$(echo "scale=2; $true_pos / ($true_pos + $false_neg)" | bc) # 小数
echo 'Precision: '$P
echo 'Recall: '$R
echo 'F1-score: '$(echo "scale=2; 2 * $P * $R / ($P + $R)" | bc)
echo 预测准确率为$(echo "scale=2; 100 * $right / $sum" | bc)%
