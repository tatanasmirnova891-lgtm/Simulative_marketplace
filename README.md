# Marketplace
## Описание проекта
Сервис автоматизирует полный ETL-цикл: сбор данных через API, загрузку в PostgreSQL и визуализацию метрик в Metabase. Обеспечивает мониторинг клиентской активности, продаж и ассортимента в реальном времени.
## Архитектура проекта
```text
Final_project/
├── config/
│   ├── config.env  # Переменные окружения (API ключи, БД)         
├── basic/             
│   ├── client_api.py   # HTTP-клиент для работы с Marketplace API   
│   ├── client_db.py    # PostgreSQL коннектор для загрузки данных
│   ├── data_processor.py   # Парсинг, валидация, трансформация данных 
│   └── logger.py   # Настройка системы логирования
├── pipeline/          
│   ├── daiky_pipeline.py   # Ежедневная загрузка данных
│   ├── historical_pipeline.py  # Историческая загрузка    
│   └── orchestrator.py # Координатор ETL-пайплайна
├── Research_2023/
│   ├── 1_optimization_matrix.ipynb # Анализ ассортиментной матрицы
│   └── 2_customer_base.ipynb   # RFM/LTV/когортный анализ клиентской базы
├── requirements.txt    # Зависимости Python   
├── logs/   # Лог-файлы выполнения пайплайна
├── README.md   # Документация проекта        
└── main.py # Точка входа (запуск ETL)                
```
## Автоматизация
Исторические данные были загружены однократно с помощью скрипта `historical_pipeline.py`.
### Ежедневный автоматизированный процесс (cron, MSK):
1. *__06:50 — Очистка логов__*
  * Удаляются файлы из logs старше 21 дня
  * Предотвращение переполнения диска
```
50 6 * * * find /home/Simulative_marketplace/logs/ -type f -mtime +21 -delete
```
2. *__07:00 — ETL pipeline__*  
  * Сбор данных за вчерашний день
  * Загрузка в PostgreSQL marketplace
```
0 7 * * * /home/Simulative_marketplace/venv/bin/python main.py >> logs/cron.log 2>&1
```
## Ссылки
Metabase    
[Исследование по товарам за 2023 год](https://colab.research.google.com/github/tatanasmirnova891-lgtm/Simulative_marketplace/blob/master/Research_2023/1_optimization_matrix.ipynb)    
[Исследование по покупателям за 2023 год](https://colab.research.google.com/github/tatanasmirnova891-lgtm/Simulative_marketplace/blob/master/Research_2023/2_customer_base.ipynb)  
