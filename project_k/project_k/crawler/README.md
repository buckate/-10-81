# News Crawler Java Maven Project

## Задание

**1. Crawler:**
- Сбор публикаций с RSS-ленты (или сайта).
- Скачивание html/xml страниц.
- Обработка http-кодов ошибок.
- Парсинг публикации: заголовок, время публикации, автор, ссылка.

**2. Очереди (RabbitMQ):**
- Использование брокера RabbitMQ.
- Очередь задач (task_queue) — ссылки на публикации.
- Очередь результатов (result_queue) — публикация (заголовок, дата, текст, автор, ссылка).
- Защита от потери сообщений (Ack).
- Использование Basic.Consume (и пример Basic.Get).

**3. ElasticSearch:**
- Индекс с полями: заголовок, время публикации, текст, ссылка, автор.
- Идентификатор — хэш от заголовка и даты (или ссылки).
- Проверка наличия документа по id (не добавлять дубликаты).
- Сохранение/обновление документов из result_queue.
- Поиск по нескольким полям с логическими операторами (AND/OR).
- Сложные полнотекстовые запросы (fuzziness).
- Агрегации (по авторам, по датам публикаций).

## Быстрый старт
1. Остановите старые контейнеры (если есть):
   ```powershell
   docker-compose down -v
   ```
2. Соберите и запустите все сервисы:
   ```powershell
   docker-compose up --build
   ```
3. Если приложение стартовало раньше сервисов, перезапустите только приложение:
   ```powershell
   docker-compose restart app
   ```

## Проверка выполнения задания

### Проверка очередей RabbitMQ
- Откройте http://localhost:15672 (логин/пароль: guest/guest)
- Должны быть очереди `task_queue` и `result_queue`.
- Сообщения появляются и исчезают по мере обработки.

### Проверка данных в ElasticSearch
- Получить все публикации:
  ```powershell
  curl -X GET "http://localhost:9200/news/_search?pretty"
  ```
- Поиск по заголовку:
  ```powershell
  curl -X POST "http://localhost:9200/news/_search?pretty" -H "Content-Type: application/json" -d "{\"query\":{\"match\":{\"title\":\"AI\"}}}"
  ```
- Поиск по нескольким полям (AND):
  ```powershell
  curl -X POST "http://localhost:9200/news/_search?pretty" -H "Content-Type: application/json" -d "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"title\":\"AI\"}},{\"match\":{\"author\":\"Автор\"}}]}}}"
  ```
- Поиск по нескольким полям (OR):
  ```powershell
  curl -X POST "http://localhost:9200/news/_search?pretty" -H "Content-Type: application/json" -d "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"title\":\"AI\"}},{\"match\":{\"author\":\"Автор\"}}],\"minimum_should_match\":1}}}"
  ```
- Fuzzy-поиск по тексту:
  ```powershell
  curl -X POST "http://localhost:9200/news/_search?pretty" -H "Content-Type: application/json" -d "{\"query\":{\"match\":{\"text\":{\"query\":\"искусственный интелект\",\"fuzziness\":\"AUTO\"}}}}"
  ```
- Агрегация по авторам:
  ```powershell
  curl -X POST "http://localhost:9200/news/_search?pretty" -H "Content-Type: application/json" -d "{\"size\":0,\"aggs\":{\"by_author\":{\"terms\":{\"field\":\"author.keyword\"}}}}"
  ```
- Агрегация по датам публикаций:
  ```powershell
  curl -X POST "http://localhost:9200/news/_search?pretty" -H "Content-Type: application/json" -d "{\"size\":0,\"aggs\":{\"by_date\":{\"date_histogram\":{\"field\":\"pubDate\",\"calendar_interval\":\"day\"}}}}"
  ```

### Проверка работы Basic.Get
- В интерфейсе RabbitMQ выберите очередь, используйте кнопку Get Message (Basic.Get).

## Структура
- Dockerfile — сборка Java Maven приложения
- docker-compose.yml — запуск всех сервисов
- src/ — исходный код
- .gitignore — исключения для git
- README.md — это описание

## Требования
- Docker и Docker Compose
- Maven (для локальной разработки)

## Разработка
- Исходный код приложения размещайте в папке `src`.
- Для добавления зависимостей используйте pom.xml.
