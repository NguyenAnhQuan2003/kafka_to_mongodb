## 1. Run netcat service

```shell
docker container stop netcat || true &&
docker container rm netcat || true &&
docker run -ti --name netcat \
--network=streaming-network \
alpine:3.14 \
/bin/sh -c "apk add --no-cache netcat-openbsd && nc -lk 9999"
```

## 2. Chạy chương trình

```shell
docker container stop spark-streaming || true &&
docker container rm spark-streaming || true &&
docker run --rm -ti --name spark-streaming \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/09-spark-streaming && zip -r /tmp/util.zip util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/util.zip,/spark/09-spark-streaming/spark.conf \
--deploy-mode client \
--master yarn \
/spark/09-spark-streaming/spark_streaming.py"
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình đếm từ và in ra danh sách các từ có số lần xuất hiện là chẵn.

Ví dụ kết quả:

```
('x', 2)
('z', 4)
```

### 3.2 Yêu cầu 2

Viết chương trình đếm từ và in ra danh sách các từ có độ dài lớn hơn 1 và có số lần xuất hiện là lẻ.

Ví dụ kết quả:

```
('yy', 1)
('zz', 3)
('ttt', 1)
```
