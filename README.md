# Тестовое задание для компании InboxSuite

## Запуск приложения
```sh
docker compose up --build
```
## Запустить миграции
```sh
docker compose run migrate up
```
## Удалить миграции
```sh
docker compose run migrate down
```
## Проверить логи
```sh
docker ps
```
```sh
docker logs <container-id-inbox-suite-test-task-app>
```
