## 1. Tạo thư mục và copy file lên hdfs

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -mkdir -p /user/spark/data/window-function'
```

**Kiểm tra:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/'
```

**Copy file từ host vào trong container:**

```shell
docker cp 06-window-function/data/summary.parquet hadoop-namenode-1:/tmp/
```

**Đẩy file lên hdfs:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -put /tmp/summary.parquet /user/spark/data/window-function/ && hdfs dfs -chown -R spark:spark /user/spark'
```

**Kiểm tra**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/window-function/'
```

## 2. Chạy chương trình

```shell
docker container stop window-function || true &&
docker container rm window-function || true &&
docker run --rm -ti --name window-function \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/06-window-function && zip -r /tmp/util.zip util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/util.zip,/spark/06-window-function/spark.conf \
--deploy-mode client \
--master yarn \
/spark/06-window-function/window_function.py"
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các quốc gia, tuần, số hóa đơn, tổng số sản phẩm, tổng giá trị hóa đơn và xếp hạng
theo tiêu chí tổng số tiền nhiều nhất trên từng quốc gia

Dữ liệu sắp theo tên quốc gia và xếp hạng tăng dần.

Ví dụ kết quả:

| Country   | WeekNumber | NumInvoices | TotalQuantity | InvoiceValue | rank |
|-----------|------------|-------------|---------------|--------------|------|
| Australia | 50         | 2           | 133           | 387.95       | 1    |
| Australia | 48         | 1           | 107           | 358.25       | 2    |
| Australia | 49         | 1           | 214           | 258.9        | 3    |
| Austria   | 50         | 2           | 3             | 257.04       | 1    |
| Bahrain   | 51         | 1           | 54            | 205.74       | 1    |
| Belgium   | 51         | 2           | 942           | 838.65       | 1    |
| Belgium   | 50         | 2           | 285           | 625.16       | 2    |
| Belgium   | 48         | 1           | 528           | 346.1        | 3    |

### 3.2 Yêu cầu 2

Viết chương trình lấy ra danh sách các quốc gia, tuần, số hóa đơn, số sản phẩm, giá trị hóa đơn và tổng giá trị hóa đơn
tính tính đến tuần của bản ghi hiện tại, phần trăm tăng của giá trị hóa đơn so với tuần trước đó.

Dữ liệu sắp xếp theo tên quốc gia, tuần.

Ví dụ kết quả:

| Country   | WeekNumber | NumInvoices | TotalQuantity | InvoiceValue | PercentGrowth | AccumulateValue |
|-----------|------------|-------------|---------------|--------------|---------------|-----------------|
| Australia | 48         | 1           | 107           | 358.25       | 0.0           | 358.25          |
| Australia | 49         | 1           | 214           | 258.9        | -27.73        | 617.15          |
| Australia | 50         | 2           | 133           | 387.95       | 49.85         | 1005.1          |
| Austria   | 50         | 2           | 3             | 257.04       | 0.0           | 257.04          |
| Bahrain   | 51         | 1           | 54            | 205.74       | 0.0           | 205.74          |
| Belgium   | 48         | 1           | 528           | 346.1        | 0.0           | 346.1           |
| Belgium   | 50         | 2           | 285           | 625.16       | 80.63         | 971.26          |
| Belgium   | 51         | 2           | 942           | 838.65       | 34.15         | 1809.91         |
