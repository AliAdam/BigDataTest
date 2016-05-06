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


def row_col_to_xy(row, col, week, hour, count):
    y = row * cell0 + cell1
    x = col * cell0 + cell1
    return "{0},{1},{2},{3},{4}".format(x, y, week , hour, count)


if __name__ == "__main__":
    sc = SparkContext()
    sc.textFile("hdfs:///user/root/gps2"). \
        map(lambda line: line_to_row_col(line)). \
        reduceByKey(lambda a, b: a + b). \
        map(lambda ((row, col, week, hour), count): row_col_to_xy(row, col, week, hour, count)). \
        saveAsTextFile("hdfs:///user/root/bin2")
