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
```text
