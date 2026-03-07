## 1. Tạo thư mục và copy file lên hdfs

Tại thư mục `spark`, chạy các lệnh sau:

**Tạo thư mục:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -mkdir -p /user/spark/data/dataframe-join'
```

**Kiểm tra:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/'
```

**Copy file từ host vào trong container:**

```shell
docker cp 05-dataframe-join/data/. hadoop-namenode-1:/tmp/dataframe-join
```

**Đẩy file lên hdfs:**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -put /tmp/dataframe-join /user/spark/data/ && hdfs dfs -chown -R spark:spark /user/spark'
```

**Kiểm tra**

```shell
docker exec -ti hadoop-namenode-1 bash -c 'hdfs dfs -ls /user/spark/data/dataframe-join/'
```

## 2. Chạy chương trình

```shell
docker container stop dataframe-join || true &&
docker container rm dataframe-join || true &&
docker run --rm -ti --name dataframe-join \
--network=streaming-network \
-v ./:/spark \
-v spark_data:/data \
-v spark_lib:/home/spark/.ivy2 \
-e HADOOP_CONF_DIR=/spark/hadoop-conf/ \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "(cd /spark/05-dataframe-join && zip -r /tmp/util.zip util/*) &&
conda env create --file /spark/environment.yml &&
source ~/miniconda3/bin/activate &&
conda activate pyspark_conda_env &&
conda pack -f -o pyspark_conda_env.tar.gz &&
spark-submit \
--conf spark.yarn.dist.archives=pyspark_conda_env.tar.gz#environment \
--py-files /tmp/util.zip,/spark/05-dataframe-join/spark.conf \
--deploy-mode client \
--master yarn \
/spark/05-dataframe-join/dataframe_join.py"
```

## 3. Yêu cầu

### 3.1 Yêu cầu 1

Viết chương trình lấy ra danh sách các chuyến bay bị hủy tới thành phố Atlanta, GA trong năm 2000

Dữ liệu sắp theo theo ngày bay giảm dần.

Ví dụ kết quả:

| id         | DEST | DEST_CITY_NAME | FL_DATE    | ORIGIN | ORIGIN_CITY_NAME   | CANCELLED |
|------------|------|----------------|------------|--------|--------------------|-----------|
| 168686     | ATL  | Atlanta, GA    | 2000-12-01 | PHX    | Phoenix, AZ        | 1         |
| 165272     | ATL  | Atlanta, GA    | 2000-12-01 | BOS    | Boston, MA         | 1         |
| 8589938391 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 8589938541 | ATL  | Atlanta, GA    | 2000-12-01 | STL    | St. Louis, MO      | 1         |
| 8589938399 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 8589938520 | ATL  | Atlanta, GA    | 2000-12-01 | SLC    | Salt Lake City, UT | 1         |
| 8589938558 | ATL  | Atlanta, GA    | 2000-12-01 | TLH    | Tallahassee, FL    | 1         |
| 8589938397 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 168522     | ATL  | Atlanta, GA    | 2000-12-01 | BOS    | Boston, MA         | 1         |
| 165432     | ATL  | Atlanta, GA    | 2000-12-01 | DTW    | Detroit, MI        | 1         |
| 8589938393 | ATL  | Atlanta, GA    | 2000-12-01 | LGA    | New York, NY       | 1         |
| 8589938370 | ATL  | Atlanta, GA    | 2000-12-01 | LAS    | Las Vegas, NV      | 1         |

### 3.2 Yêu cầu 2

Viết chương trình lấy ra danh sách các destination, năm và tổng số chuyến bay bị hủy của năm đó.

Dữ liệu sắp xếp theo mã destination và theo năm.

Ví dụ kết quả:

| DEST | FL_YEAR | NUM_CANCELLED_FLIGHT |
|------|---------|----------------------|
| ABE  | 2000    | 5                    |
| ABQ  | 2000    | 15                   |
| AGS  | 2000    | 1                    |
| ALB  | 2000    | 12                   |
| AMA  | 2000    | 5                    |
| ANC  | 2000    | 36                   |
