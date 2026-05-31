# Study Log — Currency System Project

---

## 📐 Этап 1: Архитектура и паттерны — 01.06.2026

### 📖 Подробное объяснение: что сделано и зачем

---

## 1. Clean Architecture — архитектура слоёв

### Что это такое?

**Clean Architecture** — это подход к организации кода, где проект разделяется на концентрические слои. Ключевое правило: **внешние слои зависят от внутренних, но НЕ наоборот**.

```
┌──────────────────────────────────────────────────────────┐
│                        API (Outer)                        │  ← Веб-контроллеры, Minimal API endpoints
├──────────────────────────────────────────────────────────┤
│                  Infrastructure (Outer)                   │  ← БД (EF Core), Kafka, внешние API, файловая система
├──────────────────────────────────────────────────────────┤
│                   Application (Inner)                     │  ← Бизнес-логика: CQRS команды/запросы, валидация
├──────────────────────────────────────────────────────────┤
│                     Domain (Core)                         │  ← Чистая бизнес-логика: сущности, интерфейсы
└──────────────────────────────────────────────────────────┘
```

### Почему так?

**Domain (ядро)** — НЕ зависит ни от чего. Это чистые классы/записи с бизнес-правилами. Если вы решите заменить PostgreSQL на MongoDB, или Kafka на RabbitMQ — Domain НЕ изменится.

**Application** — зависит только от Domain. Содержит бизнес-логику в виде use-cases (CQRS handlers). Не знает о базе данных, HTTP, Kafka.

**Infrastructure** — зависит от Application и Domain. Реализует интерфейсы: EF DbContext, репозитории, Kafka producer/consumer, HTTP клиенты.

**API** — зависит от всех слоёв. Тонкий слой: только endpoints, DI, middleware.

### Project References (зависимости)

```
UserService.API
  ├── UserService.Infrastructure
  │     └── UserService.Application
  │           ├── UserService.Domain
  │           └── CurrencySystem.Contracts (shared события)
  └── UserService.Application (прямая ссылка для DI)

FinanceService.API
  ├── FinanceService.Infrastructure
  │     └── FinanceService.Application
  │           ├── FinanceService.Domain
  │           └── CurrencySystem.Contracts (shared события)
  └── FinanceService.Application (прямая ссылка для DI)
```

**Интервью-вопрос:** Почему Domain НЕ должен зависеть от Application?
**Ответ:** Domain — это ядро бизнес-логики. Если Domain начнёт зависеть от Application, получится циклическая зависимость (цикл). Компилятор не позволит. Но главное — Domain должен быть независимым от фреймворков, БД, UI. Это позволяет легко тестировать Domain и переиспользовать его.

---

## 2. Domain-модели — что создали

### User (UserService.Domain)

```csharp
public class User
{
    public Guid Id { get; private set; }
    public string Name { get; private set; } = string.Empty;
    public string PasswordHash { get; private set; } = string.Empty;
    public List<string> FavoriteCurrencies { get; private set; } = new();
    public DateTime CreatedAt { get; private set; }
}
```

**Ключевые решения:**

| Решение | Почему |
|---------|--------|
| `private set` | Инкапсуляция — свойства можно изменить только через конструктор или методы |
| `PasswordHash`, а не `Password` | Храним хэш, а не пароль в открытом виде (безопасность) |
| `FavoriteCurrencies` как `List<string>` | Список ISO-кодов валют: ["USD", "EUR", "CNY"] |
| `private User() { }` | Конструктор для EF Core (требует пустой конструктор) |
| `SetFavoriteCurrencies()` | Метод для изменения избранных валют |

### Currency (FinanceService.Domain)

```csharp
public class Currency
{
    public Guid Id { get; private set; }
    public string Name { get; private set; } = string.Empty;
    public string Code { get; private set; } = string.Empty;  // USD, EUR, CNY
    public decimal Rate { get; private set; }                  // Курс к рублю
    public decimal Nominal { get; private set; } = 1;          // Номинал
    public DateTime UpdatedAt { get; private set; }
}
```

**Ключевые решения:**

| Решение | Почему |
|---------|--------|
| `decimal`, а не `double` | `decimal` для денег — точная арифметика (1.1 + 2.2 = 3.3, не 3.3000000000000003) |
| `Code` — ISO код | Стандарт: USD (доллар), EUR (евро), CNY (юань) |
| `Nominal` | CBR публикует курсы для разного номинала (например, 10 йен = X рублей) |
| `UpdateRate()` | Метод для обновления курса с фиксацией времени |

---

## 3. Kafka контракты (shared events)

### CurrencyRatesUpdatedEvent

```csharp
public record CurrencyRatesUpdatedEvent(
    Guid EventId,
    DateTime OccurredAt,
    IReadOnlyList<CurrencyRateItem> Rates,
    string Source = "cbr.ru"
);

public record CurrencyRateItem(
    string CurrencyCode,
    string CurrencyName,
    decimal RateToRub,
    decimal Nominal
);
```

**Почему `record`, а не `class`?**
- `record` — неизменяемый (immutable) по умолчанию
- Идеально для событий: создал → отправил → забыл
- Автоматическая реализация `Equals()`, `GetHashCode()` — удобно для тестов
- Компактный синтаксис (positional parameters)

**Почему `IReadOnlyList`?**
- Получатель события НЕ должен модифицировать список
- Явная контракта: "вот данные, они только для чтения"

### Зачем отдельный проект Contracts?

**Проблема:** Два сервиса (CurrencyParser и FinanceService) должны знать структуру одного события.

**Вариант А:** Каждый сервис определяет свою версию события.
- ❌ Риск рассинхронизации полей
- ❌ Сложность при изменении контракта

**Вариант Б:** Shared проект Contracts.
- ✅ Один источник правды
- ✅ Оба сервиса используют один тип
- ✅ Изменил контракт → оба сервиса обновились

**Интервью-вопрос:** Где хранить контракты событий в микросервисной архитектуре?
**Ответ:** Есть 3 подхода:
1. **Shared Kernel** (наш вариант) — один проект, который линкуют все сервисы. Просто, но создаёт耦合.
2. **Contract Repository** — отдельный репозиторий, подключаемый как NuGet-пакет. Чище, но сложнее CI/CD.
3. **Schema Registry** (Avro) — контракты хранятся в Kafka. Строгая типизация, но требует инфраструктуру.
Для тестового проекта Shared Kernel — оптимально. Для production — Schema Registry или NuGet-пакеты.

---

## 4. Почему `record` вместо `class`?

| Характеристика | `class` | `record` |
|---------------|---------|----------|
| Изменяемость | Mutable (по умолчанию) | Immutable (позиционные параметры) |
| Сравнение | По ссылке | По значению (value-based) |
| `Equals()` | Reference equality | Structural equality |
| `ToString()` | Имя типа | Имя типа + все свойства |
| `with` выражение | Нет | Да (копирование с изменением) |
| Идеально для | Сущности БД, сервисы | DTO, события, команды |

**Пример `with` для событий:**
```csharp
var event1 = new CurrencyRatesUpdatedEvent(Guid.NewGuid(), DateTime.UtcNow, rates);
var event2 = event1 with { Source = "ecb.eu" }; // Копия с изменением Source
```

---

## 5. Project References — как работают

### Команда добавления ссылки
```bash
dotnet add ProjectA.csproj reference ProjectB.csproj
```

### Что происходит в .csproj файле
```xml
<ItemGroup>
  <ProjectReference Include="..\ProjectB\ProjectB.csproj" />
</ItemGroup>
```

### Как компилятор использует ссылки
1. Компилирует ProjectB
2. Создаёт ProjectB.dll
3. Компилирует ProjectA, подключая ProjectB.dll
4. Если ProjectB не компилируется → ProjectA тоже не скомпилируется

### Порядок компиляции (зависимости)
```
1. Domain (нет зависимостей)
2. Contracts (нет зависимостей)
3. Application (зависит от Domain + Contracts)
4. Infrastructure (зависит от Application)
5. API (зависит от Infrastructure + Application)
```

---

## 6. Типы данных для денег

### Почему `decimal`, а не `double`/`float`?

```csharp
// double — проблемы с точностью
double a = 0.1 + 0.2;  // 0.30000000000000004 (!)
double b = 1.1 + 2.2;  // 3.3000000000000003 (!)

// decimal — точная арифметика
decimal c = 0.1m + 0.2m;  // 0.3
decimal d = 1.1m + 2.2m;  // 3.3
```

**Причина:** `double` использует двоичное представление (IEEE 754). Число 0.1 в двоичной системе — бесконечная дробь, поэтому округляется. `decimal` — десятичное представление, специально для финансов.

**Правило:** ВСЕГДА используйте `decimal` для денег и курсов валют. `double` — для научных вычислений, где допустима погрешность.

---

## 7. Сборка решения

### Команда
```bash
dotnet build CurrencySystem.sln
```

### Результат
```
Сборка успешно завершена.
    Предупреждений: 0
    Ошибок: 0
    Прошло времени: 00:00:02.12
```

Все 13 проектов скомпилированы без ошибок.

---

## 🎤 Интервью-вопросы и ответы

**В: Что такое Clean Architecture и зачем она нужна?**
О: Это подход к организации кода с разделением на слои (Domain, Application, Infrastructure, API). Внешние слои зависят от внутренних, но не наоборот. Позволяет легко тестировать бизнес-логику, заменить инфраструктуру (БД, брокер сообщений) без изменения ядра, соблюдать Single Responsibility.

**В: В чём разница между `class` и `record` в C#?**
O: `record` — immutable (неизменяемый) по умолчанию, сравнивается по значению (value-based equality), а не по ссылке. Имеет встроенную реализацию `Equals()`, `GetHashCode()`, `ToString()`. Поддерживает `with` выражение для копирования с изменениями. Идеально для DTO, событий, команд.

**В: Почему для денег используют `decimal`, а не `double`?**
О: `double` использует двоичное представление чисел (IEEE 754), что приводит к погрешностям (0.1 + 0.2 = 0.30000000000000004). `decimal` использует десятичное представление, обеспечивая точную арифметику. Для финансов это критично.

**В: Где хранить контракты событий в микросервисной архитектуре?**
О: 3 подхода: (1) Shared Kernel — один проект для всех сервисов, просто для небольших проектов. (2) Contract Repository — отдельный репозиторий как NuGet-пакет, чище для production. (3) Schema Registry — контракты в Kafka, строгая типизация, но требует инфраструктуру.

---

## ⚠️ Типичные ошибки (красные флаги)

| Ошибка | Почему плохо | Как исправить |
|--------|-------------|---------------|
| Domain зависит от Infrastructure | Циклическая зависимость, невозможно тестировать | Инвертировать: Infrastructure зависит от Domain |
| `double` для курсов валют | Потеря точности, ошибки в расчётах | Использовать `decimal` |
| Public set для всех свойств | Любая часть кода может изменить сущность | Private set, изменение через методы |
| Дублирование контрактов событий | Рассинхронизация при изменении | Shared проект или NuGet-пакет |
| God Object (один класс на всё) | Нарушение SRP, сложно тестировать | Разделять на маленькие классы |

---

## 🔗 Ресурсы

- [Clean Architecture by Uncle Bob](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)
- [Records in C# 9+](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/record)
- [Decimal vs Double in C#](https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/floating-point-numeric-types)
- [Domain-Driven Design](https://martinfowler.com/books/ddd.html)

---

## ▶️ Следующий шаг
**Этап 2: Базовая инфраструктура данных** — EF Core, DbContext, миграции, настройка подключения к PostgreSQL.

> ⚡ **Правило:** Не переходи к следующему этапу без согласия пользователя.

---

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
git init              # Создаёт локальный .git репозиторий
git add .             # Добавляет ВСЕ файлы в staging (подготовка к коммиту)
git status            # Показывает, что будет закоммичено
```

**Staging area** — промежуточная зона. `git add` добавляет файлы в staging, `git commit` сохраняет staging в историю.

### 3. Первый коммит

```bash
git commit -m "feat: initialize CurrencySystem project..."
```

**Conventional Commits:**
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