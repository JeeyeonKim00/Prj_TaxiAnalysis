
![spark_prj](https://github.com/JeeyeonKim00/Prj_TaxiAnalysis/assets/127364024/6e1ac7bc-bbbd-4aba-ad0c-51343b477e13)
### 💻 사용 스킬

<aside>
<img src="/icons/checkmark_lightgray.svg" alt="/icons/checkmark_lightgray.svg" width="40px" /> `**Language` :** `Python` `SQL`

`**Tool` :** `Spark` `Airflow` `Pandas` `Matplotlib` `Seaborn` `Ubuntu` `Jupyter notebook`

</aside>

### 📂 프로젝트 개요

<aside>
<img src="/icons/checkmark_lightgray.svg" alt="/icons/checkmark_lightgray.svg" width="40px" /> **[프로젝트명]** 택시요금 예측 및 파이프라인 구축 프로젝트

**[기간]** 2023.11

**[인원]** 개인 프로젝트

**[프로젝트 목적]** 

- Spark ETL 파이프라인 설계: 데이터 수집부터 ML 모델 구축 및 저장.
- Airflow로 workflow 관리: airflow를 활용하여 pipeline 관리 및 스케쥴링.
- 뉴욕의 yellow taxi 데이터를 활용한 택시 요금의 분석 및 예측

**[진행 과정]**

- Spark
    - 데이터 수집 및 적재: spark 사용하여 parquet 형태로 수집 및 적재.
    - 데이터 전처리: Spark SQL 사용.
    - 모델 구축: spark MLlib 사용하여 파라미터 튜닝 후 best parameter로 모델 학습.
    - 최종 모델 저장: parquet 형태로 저장.
- Airflow: Spark pipeline 관리 및 스케줄링
    
    ![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/4f75b599-cbe9-42dc-aeb3-86035d164147/3db26510-fc63-4943-a3e3-cf85c48b8e0f/Untitled.png)
    

**[폴더 구조]**

```bash
**ubuntu**
	├── **airflow**  
	│    └── dags  # airflow의 dag를 저장
	└── **working**
	     └── **taxi_analysis2**
						├── **spark_pipeline** # dag에서 실행할 spark pipeline 저장
						│   ├── extract_data.py
						│   ├── preprocess.py
						│   ├── train_model.py
						│   └── tune_param.py
						└── **data** 
								├── raw_data # 1️⃣ 뉴욕 택시 데이터(raw data) 저장
						    ├── train_test 
						    │       ├── test # 2️⃣ 머신러닝 test data 저장
						    │       ├── train # 2️⃣ 머신러닝 train data 저장
						    │       └── param # 3️⃣ 파라미터 튜닝 결과 저장
                └── lr_model # 4️⃣ 최종 머신러닝 모델 저장 
```

</aside>

### 📈 EDA

<aside>
<img src="/icons/checkmark_lightgray.svg" alt="/icons/checkmark_lightgray.svg" width="40px" /> **[데이터 설명]**

총 19개 피처 中 6개만 사용.

- tpep_pickup_datetime : 탑승 시간
- passenger_count: 탑승 승객 수
- trip_distance: 이동 거리
- PULocationID: 탑승 위치, DOLocationID: 하차 위치
- total_amount: 총 요금 (회귀 분석에서 목표 변수)
    
    ```bash
    root
     |-- VendorID: long (nullable = true)
     |-- **tpep_pickup_datetime**: timestamp (nullable = true)
     |-- tpep_dropoff_datetime: timestamp (nullable = true)
     |-- **passenger_count**: double (nullable = true)
     |-- **trip_distance**: double (nullable = true)
     |-- RatecodeID: double (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- **PULocationID**: long (nullable = true)
     |-- **DOLocationID**: long (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- **total_amount**: double (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- airport_fee: double (nullable = true)
    
    ```
    

[EDA1. 데이터 확인](https://github.com/JeeyeonKim00/Toy_project/blob/dde1eb783a7c6c4967577ae70d3ee4c29ac6de81/taxi_analysis/working/taxi_analysis2/taxi_analysis.ipynb)

[EDA2. 회귀 분석](https://github.com/JeeyeonKim00/Toy_project/blob/dde1eb783a7c6c4967577ae70d3ee4c29ac6de81/taxi_analysis/working/taxi_analysis2/taxi_fare_prediction.ipynb)

</aside>

### ⚡ Spark process

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/4f75b599-cbe9-42dc-aeb3-86035d164147/a37a3af5-28bf-49d7-9eb4-b2cf701c51dd/Untitled.png)

<aside>
<img src="/icons/checkmark_lightgray.svg" alt="/icons/checkmark_lightgray.svg" width="40px" /> 1️⃣ **데이터 수집**

- 모듈명: **extract_data.py**
- Used library: `pyspark.sql`, `requests`
- 주요 기능:
    - requests → 뉴욕 택시 데이터 사이트에서 데이터 수집
    - spark→ 수집한 데이터 적재
- 결과물:  tripdata.parquet

2️⃣ **데이터 전처리**

- 모듈명: **preprocess.py**
- Used library: `pyspark.sql`
- 주요 기능 (아래의 ▶를 누르시면 자세한 코드가 보입니다.)
    - Spark SQL로 데이터 전처리
        
        ```python
        taxi_df.createOrReplaceTempView('trips')
        
        # 데이터 전처리
        preprocess_query = f"""
                    SELECT 
                        passenger_count,
                        PULocationID as pickup_location_id,
                        DOLocationID as dropoff_location_id,
                        trip_distance,
                        HOUR(tpep_pickup_datetime) as pickup_time,
                        DATE_FORMAT(TO_DATE(tpep_pickup_datetime), 'EEEE') AS day_of_week,
                        total_amount
                    FROM trips
                    WHERE (total_amount BETWEEN 0 AND 5000)
                    AND (trip_distance BETWEEN 0 AND 500)
                    AND passenger_count < 4
            """
        
        data_df = spark.sql(preprocess_query)
        ```
        
    - train, test data 분할 (8:2) 및 저장
        
        ```python
        train_df, test_df = data_df.randomSplit([0.8, 0.2], seed=5)
        ```
        
- 결과물: train_df, test_df

3️⃣ **파라미터 튜닝** 

- 모듈명: **tune_param.py**
- Used library: `pyspark.sql` `pyspark.ml`
- 주요 기능
    - 모델링 Pipeline 생성
        
        ```python
        stages = []
        
        cat_features = ['pickup_location_id','dropoff_location_id','day_of_week'] # 범주형 변수
        num_features = ['passenger_count','trip_distance','pickup_time'] # 숫자형 변수
        
        for col in cat_features: 
            cat_indexer = StringIndexer(inputCol= col, outputCol=col+'_idx').setHandleInvalid('keep')
            oh_encoder = OneHotEncoder(inputCols= [cat_indexer.getOutputCol()], outputCols=[col+'_oh'])
            stages += [cat_indexer, oh_encoder]
        
        for col in num_features:
            num_vec = VectorAssembler(inputCols= [col], outputCol= col+'_vec')
            num_scaler = StandardScaler(inputCol= num_vec.getOutputCol(), outputCol= col+'_scaled')
            stages += [num_vec, num_scaler]
        
        all_features = [cat+'_oh' for cat in cat_features] + [num+'_scaled' for num in num_features]
        vec_assembler = VectorAssembler(inputCols= all_features, outputCol='feature_vector')    
        stages += [vec_assembler]
        
        ## 전처리+모델링 pipeline 생성
        lr = LinearRegression(maxIter=30,
                              solver='normal',
                              labelCol='total_amount',
                              featuresCol='feature_vector')
        
        cv_stages = stages + [lr]
        **cv_pipeline** = Pipeline(stages = cv_stages)
        ```
        
    - 파라미터 튜닝 (cross validation) 및 최적 파라미터 추출
        
        ```python
        ## ParamGridBuilder 생성
        param_grid = ParamGridBuilder()\
                        .addGrid(lr.elasticNetParam, [0.1,0.2, 0.3, 0.4, 0.5])\
                        .addGrid(lr.regParam, [0.01, 0.02, 0.03, 0.04, 0.05])\
                        .build()
        
        ## cross validation
        **cross_val** = CrossValidator(
            estimator=cv_pipeline,
            estimatorParamMaps=param_grid,
            evaluator=RegressionEvaluator(labelCol='total_amount'),
            numFolds =5) 
        
        cv_model = cross_val.fit(toy_df)
        
        best_alpha = cv_model.bestModel.stages[-1]._java_obj.getElasticNetParam()
        best_reg_param = cv_model.bestModel.stages[-1]._java_obj.getRegParam()
        
        **hyper_param** = {'alpha': [best_alpha],
                       'reg_param': [best_reg_param]}
        
        hyper_df = pd.DataFrame(hyper_param).to_csv(f'{data_dir}/param/hyper_param_{two_month_ago.year}{two_month_ago.strftime("%m")}.csv')
        print(hyper_df)
        ```
        
- 결과물: hyper_param(최적 파라미터)

**4️⃣ 모델 학습 with best parameter** 

- 모듈명: **train_model.py**
- Used library: `pyspark.sql` `pyspark.ml`
- 주요 기능
    - 모델 학습 및 저장 (parquet)
        
        ```python
        # model 생성
        lr = LinearRegression(maxIter = 30,
                              solver = 'normal',
                              labelCol = 'total_amount',
                              featuresCol='feature_vector',
                              elasticNetParam=best_alpha,
                              regParam=best_reg_param)
        
        **final_model** = lr.fit(vec_train_df)
        predictions = final_model.transform(vec_test_df)
        # predictions.cache()
        predictions.select(['trip_distance','day_of_week','total_amount','prediction']).show()
        
        # 평가
        print(f'RMSE: {final_model.summary.rootMeanSquaredError:.4f}')
        print(f'R2: {final_model.summary.r2:.4f}')
        
        model_dir = '/home/ubuntu/working/taxi_analysis2/data/lr_model'
        **final_model**.write().overwrite().save(f'{model_dir}/lr_model_{two_month_ago.year}{two_month_ago.strftime("%m")}')
        ```
        
- 결과물: final_model (최종 모델 with best parameter)
</aside>

### 🎢 Airflow process

<aside>
<img src="/icons/checkmark_lightgray.svg" alt="/icons/checkmark_lightgray.svg" width="40px" /> [**Airflow의 Dag]**

- Dag을 통해 spark 각 단계를 연결.
- 스파크 파이프라인의 자동화, 스케쥴링.
- SparkSubmitOperator 사용.
- Dag
    
    ```python
    from datetime import datetime, timedelta
    from airflow import DAG
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    
    default_args ={'start_date': datetime(2021,1,1),
                    'parallelism': 10}
    
    with DAG(dag_id='taxi-price-pipeline2',
             schedule='0 0 10 * *', # 매월 10일 실행
             default_args=default_args,
             dagrun_timeout = timedelta(minutes=60),
             tags=['spark'],
             catchup=False,
             max_active_tasks=12) as dag:
        
        **extract** = SparkSubmitOperator(
            application = '/home/ubuntu/working/taxi_analysis2/spark_pipeline/extract_data.py',
            task_id = 'extract',
            conn_id = 'spark_local'
        )
    
        **preprocess** = SparkSubmitOperator(
            application='/home/ubuntu/working/taxi_analysis2/spark_pipeline/preprocess.py',
            task_id = 'preprocess',
            conn_id = 'spark_local')
    
        **tune_hyperparameter** = SparkSubmitOperator(
          application="/home/ubuntu/working/taxi_analysis2/spark_pipeline/tune_param.py", 
          task_id="tune_hyperparameter", conn_id="spark_local")
    
        **train_model** = SparkSubmitOperator(
          application="/home/ubuntu/working/taxi_analysis2/spark_pipeline/train_model.py", 
          task_id="train_model", conn_id="spark_local")
        
        **extract >> preprocess >> tune_hyperparameter >> train_model**
    ```
    

[**결과]** 

- 모든 단계가 성공적으로 연결, 마무리 됨.
    
    ![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/4f75b599-cbe9-42dc-aeb3-86035d164147/db635859-6715-460d-b3ed-5586421c3d85/Untitled.png)
    
</aside>

### 🎁 프로젝트를 통해 얻은 것, 느낀점

<aside>
<img src="/icons/checkmark_lightgray.svg" alt="/icons/checkmark_lightgray.svg" width="40px" /> - Spark 를 통해 대용량 데이터의 ETL 파이프라인을 구축할 수 있는 경험이었음.
- Airflow와 Spark를 연동하여 파이프라인을 관리하고 스케쥴링까지 할 수 있는 역량을 기를 수 있었음.
- 실시간 택시 데이터를 받아오지 못했다는 점이 아쉬움.
- 추후 실시간 데이터와 최종 모델을 연동하여 택시 요금을 예측할 수 있는 기회가 있었으면 좋겠음.

</aside>
