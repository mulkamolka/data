# 데이터 자동화 파이프라인 

- `수집` -> `전처리` -> `저장`

- 서울시 전통시장, 대형마트 소매 물가 데이터 - `feature/retail`
- 농수산물 도매 물가 데이터 - `feature/wholesale`
- 뉴스 데이터 - `feature/news`


---

## Cloud Compose

- Apache Airflow 기반의 Managed Workflow Service

- 자동으로 리소스 프로비저닝 해주므로 DAG 작성, 스케줄링, 모니터링에 집중할 수 있는 장점

- BigQuery, Dataflow, Dataproc, Cloud Storage, Pub/Sub 등 gcp 제품과 통합이 용이

### Cloud Composer 명령어

- dag 리스트 확인
    
    ```python
    gcloud composer environments storage dags list --environment 
    [airflow-project] --location [region]
    
    gcloud composer environments storage dags \
    --environment mulkamolka
    --location asia-northeast3
    ```
    
- dag파일 가져오기
    
    ```python
    cd [your directory]
    
    gcloud composer environments storage dags import \
    --environment mulkamolka \
    --location asia-northeast3 \
    --source ./test_connection.py
    
    ```
    
- task test
    
    ```python
    gcloud composer environments run \
    mulkamolka --location asia-northeast3 \
    tasks test hello_world print_hello 2022-07-07
    ```
    
- backfill
    
    ```python
    gcloud composer environments run 
    leo-stage-bi --location=europe-west1 
    dags backfill -- regulatory_spain_daily 
    -t "regulatory_spain_ru*" --start-date 20211201 --end-date 20220119 
    --reset_dagruns --ignore_dependencies
    ```
    
- dag list 확인
    
    ```python
    gcloud composer environments run \
    mulkamolka --location asia-northeast3 \
    dags list
    ```

---



## DAG 생성 중 테스트

#### 테스트 디렉터리 만들기

1. [Cloud Storage 버킷](https://cloud.google.com/composer/docs/how-to/using/managing-dags?hl=ko)에서 테스트 디렉터리를 만들고 여기에 DAG를 복사한다.

```
gsutil cp -r gs://asia-northeast3-mulkamolka-05396162-bucket/dags gs://asia-northeast3-mulkamolka-05396162-bucket/data/test
```

- `BUCKET_NAME`은 Cloud Composer 환경과 연결된 Cloud Storage 버킷의 이름이다..


#### 구문 오류 검사

1. [Cloud Storage 버킷](https://cloud.google.com/composer/docs/how-to/using/managing-dags?hl=ko)에서 [테스트 디렉터리를 만들고 여기에 DAG를 복사한다](https://cloud.google.com/composer/docs/how-to/using/testing-dags?hl=ko#test-directory).
2. 구문 오류를 확인하려면 아래의 `gcloud` 명령어를 입력한다.

```sql
gcloud composer environments run \
mulkamolka --location asia-northeast3 \
dags list -- --subdir /home/airflow/gcs/data/test
```

#### Dag의 Task 단위로 오류 확인

1. 환경의 [Cloud Storage 버킷](https://cloud.google.com/composer/docs/how-to/using/managing-dags?hl=ko)에서 [테스트 디렉터리를 만들고 여기에 DAG를 복사합니다](https://cloud.google.com/composer/docs/how-to/using/testing-dags?hl=ko#test-directory).
2. 작업 관련 오류를 확인하려면 다음 `gcloud` 명령어를 입력한다.

```
gcloud composer environments run \
mulkamolka \
--location asia-northeast3 \
tasks test -- --subdir /home/airflow/gcs/data/test \
conn_test2 conn_task 2022-07-07
```

### 테스트 커맨드 한번에 실행하기

```sql
# cmd.sh
#!/usr/bin/bash

gcloud composer environments storage dags import --environment mulkamolka --location asia-northeast3 --source ./[DAG 파일명]
gsutil cp -r gs://asia-northeast3-mulkamolka-05396162-bucket/dags gs://asia-northeast3-mulkamolka-05396162-bucket/data/test
gcloud composer environments run  mulkamolka  --location asia-northeast3   tasks test -- --subdir /home/airflow/gcs/data/test  [DAG_ID] [TASK_ID] [EXECUTION_TIME]
```

## DAG 업데이트 및 테스트 방법

변경된 DAG 테스트 순서

1. 업데이트하려는 배포된 DAG를 `data/test`로 복사한다.
2. DAG를 수정한다.
3. DAG를 테스트한다.
4. [구문 오류가 있는지 확인](https://cloud.google.com/composer/docs/how-to/using/testing-dags?hl=ko#syntax)한다.
5. [Task 관련 오류가 있는지 확인한다](https://cloud.google.com/composer/docs/how-to/using/testing-dags?hl=ko#task-error).
6. DAG가 정상적으로 실행되는지 확인한다.
7. 테스트 환경에서 DAG를 사용 중지한다.
8. Airflow UI > DAG 페이지로 이동합니다.
9. 수정 중인 DAG가 계속 실행되는 경우 DAG를 사용 중지한다.
10. 진행 중인 작업을 빠르게 처리하려면 작업 및 **성공 표시**를 클릭한다.
11. DAG를 프로덕션 환경에 배포한다.
12. 프로덕션 환경에서 DAG를 사용 중지한다.
13. 프로덕션 환경의 `dags/` 폴더에 [업데이트된 DAG를 업로드](https://cloud.google.com/composer/docs/how-to/using/managing-dags?hl=ko#adding_or_updating_a_dag)한다.

```jsx
gcloud composer environments storage dags import \
    --environment mulkamolka \
    --location asia-northeast3 \
    --source="example_dag.py"
```

- 설치된 패키지 리스트

```bash
gcloud beta composer environments list-packages \
    mulkamolka \
    --location asia-northeast3
```

- 패키지 추가 설치

```bash
gcloud composer environments update mulkamolka \
    --location asia-northeast3 \
    --update-pypi-package bs4
```

