## 1. Tạo thư mục và copy file lên hdfs

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -mkdir -p /user/spark/data/agg-group'
```

**Kiểm tra:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/'
```

**Copy file từ host vào trong container:**

```shell
docker cp 04-agg-group/data/invoices.csv hadoop-namenode-1:/tmp/
```

**Đẩy file lên hdfs:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -put /tmp/invoices.csv /user/spark/data/agg-group/ && hdfs dfs -chown -R spark:spark /user/spark'
```

**Kiểm tra**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/agg-group/'
```

## 2. Chạy chương trình

```shell
docker container stop agg-group || true &&
docker container rm agg-group || true &&
docker run --rm -ti --name agg-group \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/04-agg-group && zip -r /tmp/util.zip util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/util.zip,/spark/04-agg-group/spark.conf \
--deploy-mode client \
--master yarn \
/spark/04-agg-group/agg_group.py"
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các quốc gia, năm, số hóa đơn, số lượng sản phẩm, tổng sô tiền của từng quốc gia vào
năm đó

Dữ liệu sắp xếp theo tên quốc gia và theo năm.

Ví dụ kết quả:

| Country   | Year | num_invoices | total_quantity | invoice_value      |
|-----------|------|--------------|----------------|--------------------|
| Australia | 2010 | 4            | 454            | 1005.1000000000001 |
| Australia | 2011 | 65           | 83199          | 136072.16999999998 |
| Austria   | 2010 | 2            | 3              | 257.03999999999996 |
| Austria   | 2011 | 17           | 4824           | 9897.28            |

### 3.2 Yêu cầu 2

Viết chương trình lấy ra top 10 khách hàng có số tiền mua hàng nhiều nhất trong năm 2010

Dữ liệu sắp xếp theo số tiền giảm dần, nếu số tiền bằng nhau thì sắp xếp theo mã khách hàng tăng dần

Ví dụ kết quả:

| CustomerID | invoice_value |
|------------|---------------|
| 18102      | 27834.61      |
| 15061      | 19950.66      |
| 16029      | 13112.52      |
