## 1. Tạo thư mục và copy file lên hdfs

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -mkdir -p /user/spark/data/dataframe-api'
```

**Kiểm tra:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/'
```

**Copy file từ host vào trong container:**

```shell
docker cp 03-dataframe-api/data/survey.csv hadoop-namenode-1:/tmp/
```

**Đẩy file lên hdfs:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -put /tmp/survey.csv /user/spark/data/dataframe-api/ && hdfs dfs -chown -R spark:spark /user/spark'
```

**Kiểm tra**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/dataframe-api/'
```

## 2. Chạy chương trình

```shell
docker container stop dataframe-api || true &&
docker container rm dataframe-api || true &&
docker run --rm -ti --name dataframe-api \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/03-dataframe-api && zip -r /tmp/util.zip util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/util.zip,/spark/03-dataframe-api/spark.conf \
--deploy-mode client \
--master yarn \
/spark/03-dataframe-api/dataframe_api.py"
```

## 3. Yêu cầu

Làm lại các yêu cầu của phần [spark-sql](../02-spark-sql) nhưng sử dụng `DataFrame API`