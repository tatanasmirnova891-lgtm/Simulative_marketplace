import os
import psycopg2
from psycopg2.extras import execute_values
from basic.logger import get_logger
from typing import List, Dict, Any
from pathlib import Path
from dotenv import load_dotenv


class PostgreSQLStorage:
    def __init__(self, service_name: str = "ETL_Storage"):
        # ✅ КРИТИЧНО: Загрузка config ПЕРЕД подключением!
        config_path = Path("config/config.env")
        if config_path.exists():
            load_dotenv(config_path)
            self.logger = get_logger(service_name)
            self.logger.info(f"✅ Config загружен: {config_path}")
        else:
            raise FileNotFoundError(f"❌ config/config.env НЕ НАЙДЕН: {config_path}")
        
        self._init_connection()
        self.ensure_tables_exist()
    
    def _init_connection(self):
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('PG_HOST', 'localhost'),
                database=os.getenv('PG_DBNAME', 'marketplace'),
                user=os.getenv('PG_USER', 'postgres'),
                password=os.getenv('PG_PASSWORD', ''),
                port=os.getenv('PG_PORT', '5432')
            )
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            self.logger.info("PostgreSQL подключен")
        except Exception as e:
            self.logger.error(f"Ошибка подключения: {e}")
            raise
    
    def ensure_table_exists(self):
        table_sql = """
        CREATE TABLE IF NOT EXISTS purchase (
            purchase_id BIGSERIAL PRIMARY KEY,
            client_id BIGINT,
            gender VARCHAR(10),
            product_id BIGINT,
            quantity INTEGER,
            price_per_item NUMERIC(10,2),
            discount_per_item NUMERIC(10,2),
            total_price NUMERIC(12,2),
            purchase_datetime TIMESTAMP,
            purchase_time_as_seconds_from_midnight INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
        self.cursor.execute(table_sql)
        self.logger.info("Таблица 'purchase' создана")

    def ensure_tables_exist(self):
        self.ensure_table_exists()
    
    def store_sales_batch(self, sales_data: List[Dict[str, Any]]) -> int:
        if not sales_data:
            self.logger.warning("Нет данных")
            return 0
        
        self.logger.info(f"Сохраняем {len(sales_data):,} записей")
        
        try:
            purchase_values = []
            for sale in sales_data:
                if sale.get('quantity', 0) > 0 and sale.get('total_price', 0) > 0:
                    purchase_values.append((
                        sale.get("client_id"),
                        sale.get("gender"),
                        sale.get("product_id"),
                        sale.get("quantity"),
                        sale.get("price_per_item"),
                        sale.get("discount_per_item"),
                        sale.get("total_price"),
                        sale.get("purchase_datetime"),
                        sale.get("purchase_time_as_seconds_from_midnight", 0)
                    ))            
            if purchase_values:
                query = """
                    INSERT INTO purchase (
                        client_id, gender, product_id, quantity,
                        price_per_item, discount_per_item, total_price,
                        purchase_datetime, purchase_time_as_seconds_from_midnight
                    ) VALUES %s
                """
                execute_values(self.cursor, query, purchase_values)
                saved_count = len(purchase_values)
                self.logger.info(f"СОХРАНЕНО {saved_count:,} записей в таблицу purchase!")
                return saved_count
            else:
                self.logger.warning("Нет валидных записей")
                return 0
                
        except Exception as e:
            self.logger.error(f"Ошибка сохранения: {e}")
            return 0
    
    def get_total_records(self) -> int:
        try:
            self.cursor.execute("SELECT COUNT(*) FROM purchase")
            return self.cursor.fetchone()[0]
        except:
            return 0
    
    def disconnect(self):
        try:
            self.cursor.close()
            self.connection.close()
            self.logger.info("PostgreSQL отключен")
        except:
            pass
