Имплементация in-memory Redis кеша на языке Python.

Telnet клиент и однопоточный асинхронный сервер.
## Запуск
Сборка проекта: `docker-compose build`.

Запуск сервера: `docker-compose up -d server`.

Запуск клиента: `docker-compose run --rm client`.

Прогон тестов: `docker-compose up tests`.

Выключение всего: `docker-compose down`.
## Команды Redis

Поддерживаемые команды: GET, SET, DEL, KEY, LRANGE, LPUSH, RPUSH, LSET, LGET, HSET, HGET, EXPIRE, PERSIST.

Команды соответствуют оригинальным командам Redis, кроме LGET, которой там нет.

TTL: SET поддерживает опции EX, PX, EXAT, PXAT, для ключей-списков и ключей-словарей можно использовать EXPIRE.
Убрать TTL можно командой PERSIST.
## Сохранение на диск
Ключи сохраняются при выключении сервера SIGINT или SIGTERM, загружаются при запуске.
