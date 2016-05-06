import math
from pyspark import SparkContext

cell0 = 1.0
cell1 = cell0 * 0.5


def line_to_row_col(line):
    splits = line.split(',')
    #154737,10.3635,27.4923,N1,4,10                                                                                                                       
    c = int(math.floor(float(splits[1]) / cell0))
    r = int(math.floor(float(splits[2]) / cell0))
    w = int(splits[4]) 
    h= int(splits[5]) 
    return (r, c, w, h), 1


if __name__ == "__main__":
    sc = SparkContext()
    sc.textFile("hdfs:///user/root/gps2"). \
        map(lambda line: line_to_row_col(line)). \
        reduceByKey(lambda a, b: a + b). \
        saveAsTextFile("hdfs:///user/root/bin2")
