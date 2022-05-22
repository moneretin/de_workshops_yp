# Flyway with docker

## Работа с docker-compose

Для этого выполним команду (флаг -d означает запуск в фоновом режиме):
```bash
docker-compose -f 1.Flyway_with_Docker/docker-compose.yml up -d
```

Посмотреть список запушенных контейнеров:

```bash
docker ps 
```

Войти в интерактивный режим командной строки bash в уже запущенном контейнере 
(флаги -i и -t подразумевают открытие cli в интерактивном режиме):

```bash
docker exec -it {containder_id} /bin/bash
```

Выйти из контейнера:

```bash
exit 
```

Остановить docker-compose:

```bash
docker-compose -f 1.Flyway_with_Docker/docker-compose.yml down
```

Посмотреть volumes:

```bash
docker volume ls 
```

Удалить volume:
```bash
docker volume rm my-vol
```
