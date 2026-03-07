## 1. Tạo thư mục và copy file lên hdfs

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -mkdir -p /user/spark/data/spark-sql'
```

**Kiểm tra:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/'
```

**Copy file từ host vào trong namenode container:**

```shell
docker cp 02-spark-sql/data/survey.csv hadoop-namenode-1:/tmp/
```

**Đẩy file lên hdfs:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -put /tmp/survey.csv /user/spark/data/spark-sql/ && hdfs dfs -chown -R spark:spark /user/spark'
```

**Kiểm tra:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/spark-sql/'
```

## 2. Chạy chương trình

```shell
docker container stop spark-sql || true &&
docker container rm spark-sql || true &&
docker run --rm -ti --name spark-sql \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/02-spark-sql/ && zip -r /tmp/util.zip util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/util.zip,/spark/02-spark-sql/spark.conf \
--deploy-mode client \
--master yarn \
/spark/02-spark-sql/spark_sql.py"
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình sử dụng `Spark SQL` lấy ra danh sách các quốc gia và số người là nam có độ tuổi < 40.

Một người là nam thì trường `Gender` sẽ có giá trị là `male` hoặc `m` (lưu ý không phân biệt viết hoa/thường).

Dữ liệu sắp xếp theo số người tăng dần. Nếu số người bằng nhau thì sắp xếp theo tên quốc gia.

Ví dụ kết quả:

| Country | Count |
|---------|-------|
| France  | 11    |
| India   | 10    |    
| Italy   | 7     |
| Sweden  | 7     |

### 3.2 Yêu cầu 2

Viết chương trình sử dụng `Spark SQL` lấy ra danh sách quốc gia và số nam, nữ của từng quốc gia.

Một người là nam thì trường `Gender` sẽ có giá trị là `male` hoặc `man` hoặc `m` (lưu ý không phân biệt viết
hoa/thường).

Một người là nữ thì trường `Gender` sẽ có giá trị là `female` hoặc `woman` hoặc `w` (lưu ý không phân biệt viết
hoa/thường).

Dữ liệu sắp xếp theo tên quốc gia.

Ví dụ kết quả:

| Country | num_male | num_female |
|---------|----------|------------|
| France  | 8        | 3          |
| India   | 10       | 2          |    
| Italy   | 7        | 6          |
| Sweden  | 7        | 9          |