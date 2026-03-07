## 1. Tạo thư mục và copy file lên hdfs

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -mkdir -p /user/spark/data/udf'
```

**Kiểm tra:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/'
```

**Copy file từ host vào trong container:**

```shell
docker cp 07-udf/data/survey.csv hadoop-namenode-1:/tmp/
```

**Đẩy file lên hdfs:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -put /tmp/survey.csv /user/spark/data/udf/ && hdfs dfs -chown -R spark:spark /user/spark'
```

**Kiểm tra**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/udf/'
```

## 2. Chạy chương trình

```shell
docker container stop udf || true &&
docker container rm udf || true &&
docker run --rm -ti --name udf \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/07-udf && zip -r /tmp/util.zip util/*) &&
(cd /spark/07-udf && zip -r /tmp/gender_util.zip gender_util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/util.zip,/tmp/gender_util.zip,/spark/07-udf/spark.conf \
--deploy-mode client \
--master yarn \
/spark/07-udf/udf.py"
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các bản ghi có số lượng employees lớn hơn hoặc bằng 500

Gợi ý: viết 1 hàm udf để xử lý dữ liệu trên cột `no_employees`

Ví dụ kết quả:

| Age | Gender | Country        | state | no_employees   |
|-----|--------|----------------|-------|----------------|
| 44  | Male   | United States  | IN    | More than 1000 |
| 36  | Male   | United States  | CT    | 500-1000       |
| 41  | Male   | United States  | IA    | More than 1000 |
| 35  | Male   | United States  | TN    | More than 1000 |
| 30  | Male   | United Kingdom | NA    | 500-1000       |
| 35  | Male   | United States  | TX    | More than 1000 |
| 35  | Male   | United States  | MI    | More than 1000 |
| 44  | Male   | United States  | IA    | More than 1000 |
