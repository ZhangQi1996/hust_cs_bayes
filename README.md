##### 注：1.sh是1个用于统计每个Country/Inds的文档数目的脚本，用于选取你想要的数个用于分类的类别
##### 将NBCorpus.rar解压后，将1.sh放入与Country同级目录即可，运行方法见脚本里面的注释


* 基本
    *分为了两个类别
    * 0代表FRA法国 文件数目总数是358
    * 1代表INDIA 印度 文件总数目是326
    * 采用训练比测试为7:3
* 准备
    1. 使用bash do_statistics.sh来生成基本的统计文件statistics.txt
    2. 用cat statistics.txt | python shuffle.py > statistics_shf.txt >/dev/null 来生成打乱后的清单文件
    3. 用bash shf_div.sh来讲打乱后的清单文件生成70%的训练清单文件，30%的测试清单文件
    4. 用bash tt.sh来生成包含训练与测试的数据集合data
    5. 为了解决小文件的问题，而且考虑到hadoop集群就只有两个tasktracker，这里将训练集合中的类别为0/1的数据分别拆分成tasktracker数量的文件。
        通过执行sh train_div.sh来完成此操作从而得到train_0/1_xx.txt的分割文件。
