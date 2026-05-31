# Study Log — Currency System Project

## Этап 0: Подготовка инфраструктуры в Docker — 31.05.2026

### 📖 Подробное объяснение: что сделано и зачем

---

## 1. Docker Compose — зачем он нужен?

**Проблема:** У нас 4 разных сервиса (PostgreSQL, Kafka, Kafka UI, pgAdmin). Каждый нужно установить, настроить, запустить. На разных машинах — разные версии, разные конфиги. "На моём компьютере работает" — классическая проблема.

**Решение:** Docker Compose — это YAML-файл, который описывает ВСЮ инфраструктуру как код. Один файл — и у любого разработчика поднимается точно такая же среда.

### 1.1 PostgreSQL контейнер

```yaml
postgres:
  image: postgres:16-alpine    # Образ: PostgreSQL 16 на базе Alpine Linux (минималистичный)
  container_name: currency-postgres
  environment:
    POSTGRES_USER: postgres      # Пользователь БД
    POSTGRES_PASSWORD: postgres  # Пароль
    POSTGRES_DB: currency_db     # Имя базы данных (создаётся автоматически)
  ports:
    - "5432:5432"                # Проброс портов: хост:контейнер
  volumes:
    - postgres-data:/var/lib/postgresql/data  # Постоянное хранилище
  networks:
    - currency-network           # Виртуальная сеть Docker
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U postgres"]  # Команда проверки здоровья
```

**Разбор ключевых концепций:**

#### Порты `"5432:5432"`
- **Первое число (5432)** — порт на ВАШЕМ компьютере (хосте)
- **Второе число (5432)** — порт внутри контейнера
- Формат: `"хост:контейнер"`
- Это значит: когда вы подключаетесь к `localhost:5432` на своём ПК, Docker перенаправляет запрос в контейнер

#### Volumes (тома)
```yaml
volumes:
  - postgres-data:/var/lib/postgresql/data
```
- **Без volume:** при удалении контейнера ВСЕ данные БД теряются
- **С volume:** данные хранятся в специальном хранилище Docker, переживают перезапуск контейнера
- `postgres-data` — имя тома (Docker сам создаёт и управляет им)
- `/var/lib/postgresql/data` — путь внутри контейнера, где PostgreSQL хранит файлы

#### Health check
```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U postgres"]
  interval: 10s    # Проверять каждые 10 секунд
  timeout: 5s      # Ждать ответ 5 секунд
  retries: 5       # После 5 неудачных попыток = unhealthy
```
- Docker сам проверяет, готова ли БД
- `pg_isready` — встроенная утилита PostgreSQL, проверяет готовность
- Другие сервисы могут ждать, пока PostgreSQL станет `healthy` перед своим стартом

---

### 1.2 Kafka в KRaft mode

**Что такое Kafka?** Apache Kafka — распределённая система обмена сообщениями (event streaming). Сервисы публикуют события в топики и подписываются на них. Пример:
- Парсер курсов → публикует событие `currency.rates.updated`
- Finance-сервис → подписывается на `currency.rates.updated`

**Что такое KRaft?** Раньше Kafka требовал Zookeeper — отдельный сервис для координации узлов. Это усложняло архитектуру. KRaft (Kafka Raft) — новый режим, где Kafka сама координирует себя без Zookeeper.

```yaml
kafka:
  image: apache/kafka:3.7.0    # Официальный образ Apache Kafka 3.7
  environment:
    KAFKA_NODE_ID: 1                        # Уникальный ID узла в кластере
    KAFKA_PROCESS_ROLES: broker,controller  # Роль: и брокер (обработка), и контроллер (управление)
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093  # Кто участвует в выборах контроллера
```

**KAFKA_PROCESS_ROLES: broker,controller**
- **broker** — обрабатывает запросы на публикацию/чтение сообщений
- **controller** — управляет кластером (ребалансировка партиций, лидерство)
- В production обычно разделяют роли, но для одного узла — обе в одном контейнере

#### Слушатели (Listeners) — самая сложная часть!

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_EXTERNAL://0.0.0.0:29092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_EXTERNAL://localhost:29092
```

**Зачем так сложно?** Kafka работает внутри Docker, но к ней подключаются:
1. **Другие контейнеры** (изнутри Docker network) → адрес `kafka:9092`
2. **Приложения на хосте** (наш .NET код при запуске локально) → адрес `localhost:29092`

- `KAFKA_LISTENERS` — на каких интерфейсах SЛУШАТЬ (0.0.0.0 = все интерфейсы)
- `KAFKA_ADVERTISED_LISTENERS` — какие адреса Kafka СООБЩАЕТ клиентам при подключении
  - Контейнер получает `kafka:9092` (внутренняя сеть Docker)
  - Хост получает `localhost:29092` (проброшенный порт)

**Без этого разделения:** контейнер внутри Docker не сможет подключиться к Kafka, потому что получит адрес `localhost`, а `localhost` для контейнера — это он сам, а не хост!

#### Health check для Kafka

```yaml
healthcheck:
  test: ["CMD", "bash", "-c", "echo | nc -z localhost 9092 || kafka-topics.sh --bootstrap-server localhost:9092 --list"]
  start_period: 30s    # Дать 30 секунд на запуск перед началом проверок
```
- `nc -z` — проверка, что порт 9092 открыт (netcat)
- `kafka-topics.sh --list` — если порт не доступен, пробуем список топиков
- `start_period: 30s` — Kafka запускается долго, даём ей время

---

### 1.3 Kafka UI

```yaml
kafka-ui:
  image: provectuslabs/kafka-ui:latest
  environment:
    KAFKA_CLUSTERS_0_NAME: local-kafka
    KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092  # Подключение ВНУТРИ Docker network
  ports:
    - "8080:8080"
```

**Зачем:** Веб-интерфейс для просмотра топиков, сообщений, потребителей Kafka. Открыл `http://localhost:8080` в браузере — и видишь всё, что происходит в Kafka.

**Важно:** Обратите внимание, что `BOOTSTRAPSERVERS: kafka:9092` — это имя контейнера в Docker network, НЕ `localhost`.

---

### 1.4 pgAdmin

```yaml
pgadmin:
  image: dpage/pgadmin4:latest
  environment:
    PGADMIN_DEFAULT_EMAIL: admin@admin.com
    PGADMIN_DEFAULT_PASSWORD: admin
    PGADMIN_CONFIG_SERVER_MODE: "False"  # Режим одного пользователя (не серверный)
  ports:
    - "5050:80"
  volumes:
    - pgadmin-data:/var/lib/pgadmin  # Сохраняем настройки и подключения
```

**Зачем:** Веб-интерфейс для управления PostgreSQL. Можно выполнять SQL-запросы, смотреть таблицы, создавать миграции. Доступ: `http://localhost:5050`

---

## 2. Docker Network — как сервисы находят друг друга

```yaml
networks:
  currency-network:
    driver: bridge
```

**Что это:** Виртуальная сеть Docker. Все сервисы в этой сети видят друг друга по именам.

**Как работает DNS в Docker:**
```
Сервис в docker-compose.yml  →  DNS-имя в сети
─────────────────────────────────────────────
postgres                     →  172.x.x.2 (IP в сети)
kafka                        →  172.x.x.3
kafka-ui                     →  172.x.x.4
```

Когда сервис пишет `Host=postgres`, Docker автоматически резолвит это в IP контейнера postgres.

**Схема соединений:**
```
┌─────────────────────────────────────────────────────┐
│               currency-network (Docker)              │
│                                                      │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐        │
│  │ postgres │   │  kafka   │   │ kafka-ui │        │
│  │ :5432    │   │ :9092    │   │ :8080    │        │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘        │
│       │              │              │               │
└───────┼──────────────┼──────────────┼───────────────┘
        │              │              │
   Порт 5432      Порт 29092     Порт 8080
   на хосте       на хосте       на хосте
        │              │              │
   ┌─────┴─────────────┴──────────────┴─────┐
   │        Ваш компьютер (хост)            │
   │  .NET приложения подключаются через    │
   │  localhost:5432 и localhost:29092      │
   └────────────────────────────────────────┘
```

---

## 3. Структура .NET 9 решения

### 3.1 Почему так много проектов?

Это **Clean Architecture** — подход к организации кода с чётким разделением ответственности:

```
┌──────────────────────────────────────────────────┐
│                    API (Outer)                    │  ← Веб-контроллеры, endpoints
├──────────────────────────────────────────────────┤
│              Infrastructure (Outer)               │  ← БД, Kafka, внешние API
├──────────────────────────────────────────────────┤
│               Application (Inner)                 │  ← Бизнес-логика, CQRS handlers
├──────────────────────────────────────────────────┤
│                 Domain (Core)                     │  ← Сущности, интерфейсы
└──────────────────────────────────────────────────┘
```

**Правило зависимостей:** Внешние слои знают о внутренних. Внутренние НЕ знают о внешних.

- Domain НЕ знает ни о чём (чистая бизнес-логика)
- Application знает только о Domain
- Infrastructure знает о Application и Domain
- API знает обо всех слоях

**Зачем:** Можно заменить базу данных, фреймворк или внешний API — и бизнес-логика не пострадает.

### 3.2 Описание проектов

| Проект | Назначение | Тип |
|--------|-----------|-----|
| `CurrencySystem.Contracts` | DTO и события Kafka, общие для всех сервисов | Class Library |
| `UserService.Domain` | Сущность User, интерфейсы репозиториев, доменные события | Class Library |
| `UserService.Application` | CQRS команды/запросы, хендлеры, валидация | Class Library |
| `UserService.Infrastructure` | EF Core DbContext, репозитории, миграции | Class Library |
| `UserService.API` | Minimal API endpoints, Program.cs, DI | Web API |
| `FinanceService.*` | Аналогично для финансов | (4 проекта) |
| `CurrencySystem.CurrencyParser` | Фоновый сервис парсинга cbr.ru | Worker Service |
| `CurrencySystem.DbMigrator` | Применение миграций EF Core | Console App |
| `CurrencySystem.ApiGateway` | YARP — маршрутизация запросов | Web API |
| `*.UnitTests` | Unit-тесты | xUnit |

### 3.3 Project References (зависимости)

```
API → Application → Domain
API → Infrastructure → Application → Domain
CurrencyParser → Contracts
FinanceService.Application → Contracts
```

---

## 4. Типы .NET проектов

| Шаблон | Назначение | Когда использовать |
|--------|-----------|-------------------|
| `classlib` | Библиотека классов | Слои Clean Architecture |
| `web` (ASP.NET Core) | HTTP API, веб-приложение | API-сервисы |
| `worker` | Фоновый сервис (IHostedService) | Периодические задачи, демоны |
| `console` | Консольное приложение | Миграции, утилиты |
| `xunit` | Библиотека тестов | Unit/Integration тесты |

---

## 🎤 Интервью-вопросы и ответы

**В: Чем отличается `localhost` в контейнере и на хосте?**
О: Внутри контейнера `localhost` ссылается на сам контейнер, а не на хост-машину. Для связи между контейнерами используется Docker network и имя сервиса (например, `postgres:5432`). Для доступа с хоста к контейнеру — проброс портов (`-p 5432:5432`).

**В: Как сервисы находят друг друга внутри Docker Compose?**
О: Docker Compose автоматически создает DNS-записи для каждого сервиса по его имени. Внутри сети `currency-network` сервис `postgres` доступен по адресу `postgres:5432`, а `kafka` — по `kafka:9092`.

**В: Что такое KRaft mode и зачем он нужен?**
О: KRaft (KRaft consensus protocol) — это режим работы Kafka без Zookeeper, введенный в Kafka 2.8+ и стабильный в 3.x+. Упрощает архитектуру (меньше компонентов), быстрее старт, проще конфигурация. Для production рекомендуется именно KRaft.

**В: Зачем нужны Advertised Listeners в Kafka?**
О: Advertised Listeners сообщают клиентам, по какому адресу подключаться к брокеру. Без правильного конфигурирования клиенты внутри Docker получат адрес `localhost` и не смогут подключиться к брокеру из другого контейнера.

### ⚠️ Типичные ошибки (красные флаги)
- Использование `localhost` внутри контейнера для связи с другим сервисом → нужно использовать имя сервиса
- Отсутствие health checks → контейнеры запускаются, но сервисы ещё не готовы → ошибки при старте
- Не использовать volumes → данные БД теряются при рестарте контейнера
- Один listener для внутренних и внешних клиентов → контейнеры не подключатся к Kafka

### 🔗 Ресурсы
- [Docker Compose networking](https://docs.docker.com/compose/networking/)
- [Kafka KRaft mode](https://developer.confluent.io/learn/kraft/)
- [Kafka listeners explained](https://www.confluent.io/blog/kafka-listeners-explained/)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
- [.NET 9 Docker images](https://hub.docker.com/_/microsoft-dotnet)

### ▶️ Следующий шаг
**Этап 1: Архитектура и паттерны** — Clean Architecture, CQRS, MediatR, Event-Driven Architecture. Настроим проект CurrencySystem.Contracts (Kafka события) и Domain-модели.

---

## Git — настройка репозитория

### Что сделано и зачем

**Git** — система контроля версий. Сохраняет историю изменений кода. Позволяет:
- Откатиться к любой версии
- Работать в команде (ветки, merge)
- Поделиться кодом (GitHub, GitLab)

### 1. .gitignore — что НЕ коммитить

```
.gitignore
```

Файл говорит Git, какие файлы **игнорировать**. Зачем:
- `[Bb]in/`, `[Oo]bj/` — скомпилированные файлы (генерируются автоматически)
- `*.dll`, `*.exe` — бинарники (не нужно в репозитории)
- `.vs/`, `.vscode/` — настройки IDE (у каждого свои)
- `*.env`, `*.pfx` — секреты и пароли (НИКОГДА не коммитить!)

**Интервью-вопрос:** Что будет, если закоммитить `.env` с паролями?
**Ответ:** Пароли попадут в историю Git и будут доступны всем, у кого есть доступ к репозиторию. Даже если потом удалить файл — он останется в истории коммитов. Решение: `.gitignore` + ротация секретов.

### 2. Инициализация репозитория

```bash
git init              # Создаёт локаный .git репозиторий
git add .             # Добавляет ВСЕ файлы в staging (подготовка к коммиту)
git status            # Показывает, что будет закоммичено
```

**Staging area** — промежуточная зона. `git add` добавляет файлы в staging, `git commit` сохраняет staging в историю.

### 3. Первый коммит

```bash
git commit -m "feat: initialize CurrencySystem project..."
```

**Convention Commits:**
- `feat:` — новая функциональность
- `fix:` — исправление бага
- `docs:` — документация
- `chore:` — обслуживание (зависимости, конфиги)

### 4. Подключение к GitHub

```bash
git remote add origin https://github.com/Disthere/currency-system.git
git branch -M main              # Переименовать master → main
git push -u origin main         # Отправить код на GitHub
```

**Что значит `-u` (upstream):** Связывает локальную ветку `main` с удалённой `origin/main`. После этого можно писать просто `git push` без указания ветки.

### ⚠️ Типичные ошибки (красные флаги)
- Коммитить `bin/`, `obj/` → репозиторий раздувается, конфликты на разных ОС
- Коммитить `.env` с секретами → утечка паролей
- Создавать репозиторий с README + локальный `git init` → конфликт историй (`non-fast-forward`)
- Не использовать `.gitignore` → в коммитах мусор, IDE-файлы, кэш

### 🔗 Ресурсы
- [Git Book (бесплатно)](https://git-scm.com/book/ru/v2)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [GitHub CLI](https://cli.github.com/) — управление репозиторием из терминала
