# Kafka

## 1. Запускаем Kafka с помощью docker-compose

Для этого перейдем в директорию 3.Kafka:
```bash
cd 3.Kafka
```

И выполним команду:
```bash
docker-compose -p kafka -f docker-compose.yaml up -d
```

Чтобы записать в Kafka данные, необходимо для начала создать топик:
```bash
docker exec broker \
kafka-topics --bootstrap-server broker:9092 \
             --create \
             --topic practicum_de
```

Теперь запишем данные в Kafka (флаги -i и -t подразумевают открытие CLI в интерактивном режиме):
```bash
docker exec -it broker \
kafka-console-producer --bootstrap-server broker:9092 \
                       --topic practicum_de
```

В открывшемся окне напишем:
```text
I am a Data Engineer!
```

Чтобы выйти из интерактивного режима, можно воспользоваться следующим сочетанием клавим **Ctrl-D**

Попробуем прочитать сообщение из Kafka:
```bash
docker exec -it broker \
kafka-console-consumer --bootstrap-server broker:9092 \
                       --topic practicum_de \
                       --from-beginning
```

Можно попробовать открыть две консоли, в одной будет продюсер,
в другой консьюмер и в интерактивном режиме пописать/почитать сообщения.

## 2. Работаем с Kafka с помощью Python

Pip install не сработает, так как библиотека работает поверх [librdkafka](https://github.com/edenhill/librdkafka)

Документация по установке [здесь](https://github.com/confluentinc/confluent-kafka-python/blob/master/INSTALL.md)

Создать виртуальную среду:
```bash
python -m venv .kafka-env
```

Активировать виртуальную среду:
```bash
source .kafka-env/bin/activate
```

Установить необходимые библиотеки:
```bash
pip install -r requirements.txt
```

Записать данные в Kafka:
```bash
python python_producer.py
```

Прочитать данные из Kafka:
```bash
python python_consumer.py
```

Записать данные в Kafka в Avro:
```bash
python python_avro_producer.py
```

Проверить схему в schema registry можно пройдя по ссылке http://0.0.0.0:8081/subjects/

Как путешествовать по schema registry можно найти [тут](https://docs.confluent.io/platform/current/schema-registry/develop/using.html)

Прочитать данные из Kafka в Avro:
```bash
python python_avro_consumer.py
```

Остановить docker-compose c Kafka можно следующим образом:
```bash
docker-compose -p kafka -f docker-compose.yaml down 
```

### Заметки

UI для работы с Kafka
https://www.kafkatool.com, https://akhq.io
