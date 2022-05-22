# Airflow

## 1. Запускаем MinIO с помощью docker-compose

Для этого перейдем директорию 2.Airflow:
```bash
cd 2.Airflow_and_S3
```

И выполним команду:
```bash
docker-compose -p minio -f minio/docker-compose.yaml up -d 
```

Далее необходимо пройти по http://127.0.0.1:9001 и залогиниться. Явки и пароли - minioadmin:minioadmin

API открывается на 9000 порту

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

Создать бакет **practicum** можно следующей командой:
```bash
mc mb minio-local/practicum/
```

Все доступные команды можно найти в документации: https://docs.min.io/docs/minio-client-complete-guide.html

Проверить созданный бакет можно в UI или с помощью команды:
```bash
mc ls minio-local/
```

Загрузить данные (файл) в S3 (MinIO) можно с помощью команды:
```bash
mc cp README.md minio-local/practicum/data/
```

```bash
mc cp презентация minio-local/practicum/data/
```

Удалить данные из S3:
```bash
mc rm -r --force minio-local/practicum/data/
```

Остановить docker-compose c MinIO можно следующим образом:
```bash
docker-compose -p minio -f minio/docker-compose.yaml down 
```

### Заметки

Для запуска MinIO в docker-compose можно воспользоваться следующей инструкцией:
https://docs.min.io/docs/deploy-minio-on-docker-compose.html

## 2. Запускаем Airflow с помощью docker-compose

Для этого перейдем директорию 2.Airflow:
```bash
cd 2.Airflow_and_S3
```

И выполним команду (флаг -d означает запуск в фоновом режиме):
```bash
docker-compose -p airflow -f docker-compose.yaml up -d
```

Далее необходимо пройти по http://localhost:8080 и залогиниться. Явки и пароли - airflow:airflow

Скопировать DAG можно следующей командой
```bash
cp -r future_dag/ airflow/dags/
```

Создать виртуальную среду:
```bash
python -m venv .env/
```

Остановить docker-compose можно следующим образом:
```bash
docker-compose -f docker-compose.yaml down
```

### Заметки:

Для запуска Airflow в docker-compose можно воспользоваться следующей инструкцией:
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
