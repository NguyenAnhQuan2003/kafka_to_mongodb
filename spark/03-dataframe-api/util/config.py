import configparser
import os

from pyspark import SparkConf


def get_spark_conf(conf_path_file):
    util_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"util_dir: {util_dir}")

    print(f"conf_path_file: {conf_path_file}")
    conf = configparser.ConfigParser()
    conf.read(conf_path_file)

    spark_conf = SparkConf()

    for (k, v) in conf.items("SPARK"):
        spark_conf.set(k, v)

    return spark_conf
