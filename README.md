**Запуск:**

docker compose up -d

docker compose ps

**Создание таблиц:**

Get-Content .\sql\01_ddl.sql | docker compose exec -T clickhouse clickhouse-client -u click --password click --multiquery

docker compose exec -T clickhouse clickhouse-client -u click --password click -q "SHOW TABLES FROM teta"

**Запуск producer:**

python .\scripts\produce.py

**Проверка, что данные дошли:**

Start-Sleep -Seconds 30

docker compose exec -T clickhouse clickhouse-client -u click --password click -q "SELECT count() FROM teta.transactions"

**Сохранение:**

docker compose exec -T clickhouse clickhouse-client -u click --password click -q "SELECT state, argMax(category, amount) AS max_category, max(amount) AS max_amount FROM teta.transactions GROUP BY state ORDER BY state LIMIT 10"

New-Item -ItemType Directory -Force .\results | Out-Null

docker compose exec -T clickhouse clickhouse-client -u click --password click --query "SELECT state, argMax(category, amount) AS max_category, max(amount) AS max_amount FROM teta.transactions GROUP BY state ORDER BY state FORMAT CSVWithNames" | Out-File -Encoding utf8 .\results\result.csv

