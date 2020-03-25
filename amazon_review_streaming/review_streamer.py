import time
import os

filepath = '../../amazon_reviews_multilingual_FR_v1_00.tsv'
hdfspath = 'hdfs://sar01:9000/spark_project_2020'
outpath = hdfspath + '/review_stream/'


os.system('hdfs dfs -rm {}/*'.format(outpath))
os.system('hdfs dfs -rmr {}/streaming_output/*'.format(hdfspath))
os.system('hdfs dfs -rmr {}/checkpoint/vertices/*'.format(hdfspath))
os.system('hdfs dfs -rmr {}/checkpoint/edges/*'.format(hdfspath))

with open(filepath) as f:
    headers = f.readline()
    k = 0
    while True:
        lines = f.readlines(1000)
        lines.insert(0, headers)
	content = ('\n'.join(lines)).replace('"', '\"')
        outfilepath = outpath + 'reviews_{}.tsv'.format(k)
	os.system('echo "{content}" | hdfs dfs -put - {path}'.format(content='\n'.join(lines), path=outfilepath))
        k += 1
	print('New file: ' + outfilepath)
        time.sleep(10)
