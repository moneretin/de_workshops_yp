# Airflow

## 1. Запускаем MinIO с помощью docker-compose

Для этого перейдем в директорию 2.Airflow_and_S3:
```bash
cd 2.Airflow_and_S3
```

И выполним команду:
```bash
docker-compose -p minio -f minio/docker-compose.yaml up -d 
```

Далее необходимо пройти по http://127.0.0.1:9001 и залогиниться. Явки и пароли - minioadmin:minioadmin. 
API открывается на 9000 порту.

### Как общаться с minio?

Cкачать утилиту mc для СLI: https://docs.min.io/docs/minio-client-complete-guide.html .
Есть и другие утилиты: https://docs.min.io/docs/aws-cli-with-minio

Проверить все alias:
```bash
mc alias ls 
```

Установить alias **minio-local** можно следующим образом:
```bash
mc alias set minio-local http://127.0.0.1:9000 minioadmin minioadmin
```

Создать бакет **de-bucket** можно следующей командой:
```bash
mc mb minio-local/de-bucket/
```

Все доступные команды можно найти в документации: https://docs.min.io/docs/minio-client-complete-guide.html

Проверить созданный бакет можно в UI или с помощью команды:
```bash
mc ls minio-local/
```

Загрузить данные (файл) в S3 (MinIO) можно с помощью команды:
```bash
mc cp README.md minio-local/de-bucket/data/
```

```bash
mc cp docker-compose.yaml minio-local/de-bucket/data/
```

Удалить данные из S3:
```bash
mc rm -r --force minio-local/de-bucket/data/
```

Остановить docker-compose c MinIO можно следующим образом:
```bash
docker-compose -p minio -f minio/docker-compose.yaml down 
```

### Заметки

Для запуска MinIO в docker-compose можно воспользоваться следующей инструкцией:
https://docs.min.io/docs/deploy-minio-on-docker-compose.html

## 2. Запускаем Airflow c MinIO с помощью docker-compose

Для этого перейдем директорию 2.Airflow:
```bash
cd 2.Airflow_and_S3
```

И поменяем в docker-compose minIO volume на ./minio/nginx.conf в 42 строке

И выполним команду (флаг -d означает запуск в фоновом режиме):
```bash
docker-compose -f docker-compose.yaml -f minio/docker-compose.yaml -p practicum up -d
```

Пока Airflow запускается можно перейти в Jupyter и открыть тетрадку с названием Workshop№2.ipynb

Далее необходимо пройти по http://localhost:8080 и залогиниться. Явки и пароли - airflow:airflow

Создать бакет **practicum** через UI в minio

Создать коннекшен с названием **s3_conn**:

*Host* (по названию сервиса): http://nginx:9000; *Login* (access key): minioadmin; *Password* (secret key): minioadmin;
*Schema* (наш бакет): practicum

Создать виртуальную среду:
```bash
python -m venv .env
```

Активировать виртуальную среду:
```bash
source .env/bin/activate
```

Установить необходимые библиотеки:
```bash
pip install -r requirements.txt
```

Скопировать DAG можно следующей командой
```bash
cp -r future_dag/ airflow/dags/
```

Остановить docker-compose можно следующим образом:
```bash
docker-compose -f docker-compose.yaml -f minio/docker-compose.yaml -p practicum down
```

### Заметки:
Для запуска Airflow в docker-compose можно воспользоваться следующей инструкцией:
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
