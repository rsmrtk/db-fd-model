# Income Management API Endpoints

## База даних має такі поля:
```
- income_id (string) - Primary Key
- income_name (string) - Назва/опис доходу
- income_amount (numeric) - Сума доходу
- income_type (enum) - Тип: salary, transfer, others
- income_date (timestamp) - Дата надходження
- created_at (timestamp) - Дата створення запису
```

## 1. Основні CRUD операції

### 1.1 Створити новий дохід
**POST** `/api/incomes`
```json
Request Body:
{
  "income_name": "Зарплата за грудень",
  "income_amount": 50000.00,
  "income_type": "salary",
  "income_date": "2024-12-25T10:00:00Z"
}

Response: 201 Created
{
  "income_id": "uuid-12345",
  "income_name": "Зарплата за грудень",
  "income_amount": 50000.00,
  "income_type": "salary",
  "income_date": "2024-12-25T10:00:00Z",
  "created_at": "2024-12-25T10:05:00Z"
}
```

### 1.2 Отримати конкретний дохід
**GET** `/api/incomes/{income_id}`
```json
Response: 200 OK
{
  "income_id": "uuid-12345",
  "income_name": "Зарплата за грудень",
  "income_amount": 50000.00,
  "income_type": "salary",
  "income_date": "2024-12-25T10:00:00Z",
  "created_at": "2024-12-25T10:05:00Z"
}
```

### 1.3 Оновити дохід
**PUT** `/api/incomes/{income_id}`
```json
Request Body:
{
  "income_name": "Зарплата за грудень (оновлено)",
  "income_amount": 55000.00
}

Response: 200 OK
{
  "income_id": "uuid-12345",
  "income_name": "Зарплата за грудень (оновлено)",
  "income_amount": 55000.00,
  "income_type": "salary",
  "income_date": "2024-12-25T10:00:00Z",
  "created_at": "2024-12-25T10:05:00Z"
}
```

### 1.4 Видалити дохід
**DELETE** `/api/incomes/{income_id}`
```json
Response: 204 No Content
```

## 2. Списки та фільтрація

### 2.1 Отримати всі доходи з фільтрацією
**GET** `/api/incomes`

Query параметри:
- `type` - фільтр за типом (salary, transfer, others)
- `from_date` - початкова дата
- `to_date` - кінцева дата
- `min_amount` - мінімальна сума
- `max_amount` - максимальна сума
- `limit` - кількість записів (default: 50)
- `offset` - зміщення для пагінації

Приклади:
```
GET /api/incomes?type=salary&from_date=2024-01-01&to_date=2024-12-31
GET /api/incomes?min_amount=10000&max_amount=100000&limit=10
```

```json
Response: 200 OK
{
  "data": [
    {
      "income_id": "uuid-12345",
      "income_name": "Зарплата за грудень",
      "income_amount": 50000.00,
      "income_type": "salary",
      "income_date": "2024-12-25T10:00:00Z",
      "created_at": "2024-12-25T10:05:00Z"
    },
    // ... more incomes
  ],
  "total": 150,
  "limit": 50,
  "offset": 0
}
```

## 3. Аналітичні ендпоінти

### 3.1 Отримати суму доходів за період
**GET** `/api/incomes/summary`

Query параметри:
- `from_date` - початкова дата (required)
- `to_date` - кінцева дата (required)
- `group_by` - групування (day, week, month, year, type)

```json
Response: 200 OK
{
  "total_amount": 250000.00,
  "count": 15,
  "from_date": "2024-01-01",
  "to_date": "2024-12-31",
  "by_type": {
    "salary": 200000.00,
    "transfer": 30000.00,
    "others": 20000.00
  }
}
```

### 3.2 Отримати статистику по типам доходів
**GET** `/api/incomes/statistics`

Query параметри:
- `from_date` - початкова дата
- `to_date` - кінцева дата

```json
Response: 200 OK
{
  "by_type": [
    {
      "type": "salary",
      "total_amount": 200000.00,
      "count": 12,
      "percentage": 80.0,
      "average": 16666.67
    },
    {
      "type": "transfer",
      "total_amount": 30000.00,
      "count": 5,
      "percentage": 12.0,
      "average": 6000.00
    },
    {
      "type": "others",
      "total_amount": 20000.00,
      "count": 8,
      "percentage": 8.0,
      "average": 2500.00
    }
  ],
  "total": 250000.00,
  "total_count": 25
}
```

### 3.3 Отримати доходи за місяцями
**GET** `/api/incomes/monthly`

Query параметри:
- `year` - рік (default: поточний рік)

```json
Response: 200 OK
{
  "year": 2024,
  "months": [
    {
      "month": 1,
      "month_name": "Січень",
      "total_amount": 45000.00,
      "count": 3
    },
    {
      "month": 2,
      "month_name": "Лютий",
      "total_amount": 52000.00,
      "count": 4
    },
    // ... for all 12 months
  ],
  "yearly_total": 580000.00
}
```

## 4. Пошук

### 4.1 Пошук за назвою
**GET** `/api/incomes/search`

Query параметри:
- `q` - пошуковий запит (required)
- `limit` - кількість результатів

```json
Response: 200 OK
{
  "results": [
    {
      "income_id": "uuid-12345",
      "income_name": "Зарплата за грудень",
      "income_amount": 50000.00,
      "income_type": "salary",
      "income_date": "2024-12-25T10:00:00Z"
    }
  ],
  "count": 1
}
```

## 5. Батч-операції

### 5.1 Створити декілька доходів
**POST** `/api/incomes/batch`
```json
Request Body:
{
  "incomes": [
    {
      "income_name": "Зарплата",
      "income_amount": 50000.00,
      "income_type": "salary",
      "income_date": "2024-12-25T10:00:00Z"
    },
    {
      "income_name": "Переказ від друга",
      "income_amount": 5000.00,
      "income_type": "transfer",
      "income_date": "2024-12-26T15:00:00Z"
    }
  ]
}

Response: 201 Created
{
  "created": 2,
  "incomes": [...]
}
```

## 6. Експорт даних

### 6.1 Експорт в CSV
**GET** `/api/incomes/export/csv`

Query параметри: ті ж самі, що і для фільтрації

Headers:
```
Content-Type: text/csv
Content-Disposition: attachment; filename="incomes_2024.csv"
```

### 6.2 Експорт в Excel
**GET** `/api/incomes/export/xlsx`

### 6.3 Експорт в PDF (звіт)
**GET** `/api/incomes/export/pdf`

## Приклади SQL запитів для кожного ендпоінту

### Створити дохід (INSERT):
```sql
INSERT INTO incomes (income_id, income_name, income_amount, income_type, income_date, created_at)
VALUES (@income_id, @income_name, @income_amount, @income_type, @income_date, CURRENT_TIMESTAMP());
```

### Отримати дохід за ID (SELECT):
```sql
SELECT income_id, income_name, income_amount, income_type, income_date, created_at
FROM incomes
WHERE income_id = @income_id;
```

### Оновити дохід (UPDATE):
```sql
UPDATE incomes
SET income_name = @income_name, income_amount = @income_amount
WHERE income_id = @income_id;
```

### Видалити дохід (DELETE):
```sql
DELETE FROM incomes
WHERE income_id = @income_id;
```

### Фільтрація доходів:
```sql
SELECT income_id, income_name, income_amount, income_type, income_date, created_at
FROM incomes
WHERE
  (@type IS NULL OR income_type = @type)
  AND (@from_date IS NULL OR income_date >= @from_date)
  AND (@to_date IS NULL OR income_date <= @to_date)
  AND (@min_amount IS NULL OR income_amount >= @min_amount)
  AND (@max_amount IS NULL OR income_amount <= @max_amount)
ORDER BY income_date DESC
LIMIT @limit OFFSET @offset;
```

### Отримати суму за період:
```sql
SELECT
  SUM(income_amount) as total_amount,
  COUNT(*) as count,
  income_type
FROM incomes
WHERE income_date BETWEEN @from_date AND @to_date
GROUP BY income_type;
```

### Статистика по місяцях:
```sql
SELECT
  EXTRACT(MONTH FROM income_date) as month,
  SUM(income_amount) as total_amount,
  COUNT(*) as count
FROM incomes
WHERE EXTRACT(YEAR FROM income_date) = @year
GROUP BY EXTRACT(MONTH FROM income_date)
ORDER BY month;
```

### Пошук за назвою:
```sql
SELECT income_id, income_name, income_amount, income_type, income_date
FROM incomes
WHERE LOWER(income_name) LIKE LOWER(CONCAT('%', @search_query, '%'))
LIMIT @limit;
```

## HTTP Status Codes

- **200 OK** - Успішний запит (GET, PUT)
- **201 Created** - Ресурс створено (POST)
- **204 No Content** - Успішне видалення (DELETE)
- **400 Bad Request** - Невірні дані в запиті
- **401 Unauthorized** - Не авторизований
- **403 Forbidden** - Заборонено
- **404 Not Found** - Ресурс не знайдено
- **422 Unprocessable Entity** - Помилка валідації
- **500 Internal Server Error** - Серверна помилка

## Валідація

### Правила валідації:
- `income_name` - обов'язкове, максимум 255 символів
- `income_amount` - обов'язкове, число > 0
- `income_type` - обов'язкове, один з: salary, transfer, others
- `income_date` - обов'язкове, валідна дата, не в майбутньому