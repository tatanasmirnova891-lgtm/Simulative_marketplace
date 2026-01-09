import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

# Импорты компонентов ETL пайплайна
from basic.client_api import MarketplaceAPI
from basic.data_processor import SalesDataTransformer
from basic.client_db import PostgreSQLStorage
from pipeline.daily_pipeline import YesterdaySalesProcessor, run_daily_etl
from pipeline.historical_pipeline import FullHistoryImporter, import_full_history
from basic.logger import get_logger


class MarketplaceETL:
    def __init__(self, mode: str = "daily", config_dir: str = "config"):
        self.mode = mode.lower()
        self.config_path = Path(config_dir) / "config.env"
        self.logger = get_logger("MarketplaceETL")
        self.start_timestamp = datetime.now()
        self._initialize_environment()
        self._setup_pipeline_components()
        self.logger.info(f"ETL запущен в режиме: {self.mode.upper()}")

    def _initialize_environment(self):
        if not self.config_path.exists():
            raise FileNotFoundError(f"Конфигурация не найдена: {self.config_path}")
        load_dotenv(self.config_path)
        self.project_root = Path.cwd()
        self.logger.info(f"Рабочая директория: {self.project_root.name}")
    
    def _setup_pipeline_components(self):
        self.api_client = MarketplaceAPI("ETL_API")
        self.data_processor = SalesDataTransformer("ETL_Processor")
        self.db_storage = PostgreSQLStorage("ETL_Storage")
        
        if self.mode == "history":
            self.pipeline_strategy = FullHistoryImporter(
                self.api_client, self.data_processor, self.db_storage
            )
        else:
            self.pipeline_strategy = YesterdaySalesProcessor(
                self.api_client, self.data_processor, self.db_storage
            )
    
    def execute(self) -> dict:
        self.logger.info("=" * 70)
        self.logger.info(f"МАРКЕТПЛЕЙС | РЕЖИМ: {self.mode.upper()}")
        self.logger.info("=" * 70)
        
        try:
            self.db_storage.ensure_tables_exist()
            self.logger.info("Схема БД готова")
            pipeline_stats = self.pipeline_strategy.execute()
            self._print_execution_summary(pipeline_stats)
            return pipeline_stats
            
        except KeyboardInterrupt:
            self.logger.warning("Остановлено пользователем")
            return {"status": "interrupted"}
        except Exception as critical_error:
            self.logger.error(f"Критическая ошибка: {critical_error}", exc_info=True)
            raise
        finally:
            self._cleanup()
    
    def _print_execution_summary(self, stats: dict):
        processed = stats.get('processed', 0)
        stored = stats.get('stored', 0)
        errors = stats.get('errors', 0)
        duration = (datetime.now() - self.start_timestamp).total_seconds()
        self.logger.info("РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ:")
        self.logger.info(f"Время работы: {duration:.1f}с")
        self.logger.info(f"Обработано записей: {processed:,}")
        self.logger.info(f"Сохранено в БД: {stored:,}")
        self.logger.info(f"Ошибок/отклонений: {errors:,}")
        
        success_rate = (stored / processed * 100) if processed > 0 else 0
        self.logger.info(f"Качество данных: {success_rate:.1f}%")
    
    def _cleanup(self):
        try:
            self.api_client.__del__()
            self.db_storage.disconnect()
            self.logger.info("Ресурсы освобождены")
        except:
            pass
    
    @classmethod
    def from_cli(cls) -> 'MarketplaceETL':
        import argparse    
        parser = argparse.ArgumentParser(description="ETL для маркетплейса")
        parser.add_argument('--mode', choices=['daily', 'history'], default='daily',
                          help="Режим работы (по умолчанию: daily)")
        parser.add_argument('--config', default='config', 
                          help="Папка с конфигурацией")   
        args = parser.parse_args()
        return cls(mode=args.mode, config_dir=args.config)


def main():
    try:
        config_path = Path("config") / "config.env"
        if not config_path.exists():
            raise FileNotFoundError(f"НЕ НАЙДЕН: {config_path}")
        
        load_dotenv(config_path)
        print(f"Конфигурация загружена: {config_path}")
        print("🔍 Проверяем наличие данных...")
        from basic.client_db import PostgreSQLStorage
        test_storage = PostgreSQLStorage("HistoryCheck")
        test_storage.ensure_tables_exist()
        test_storage.cursor.execute("SELECT COUNT(*) FROM purchase")
        total_records = test_storage.cursor.fetchone()[0]
        test_storage.disconnect()
        print(f"Всего записей в БД: {total_records:,}")
        if total_records < 1000:
            print("\nРЕЖИМ 1/1: ПОЛНАЯ ИСТОРИЧЕСКАЯ ЗАГРУЗКА")
            app = MarketplaceETL(mode="history", config_dir="config")
        else:
            print("\nРЕЖИМ ЕЖЕДНЕВНЫЙ: Только вчерашние данные")
            app = MarketplaceETL(mode="daily", config_dir="config")
    
        print(f"\n{'='*60}")
        results = app.execute()
        print(f"{'='*60}")
        sys.exit(0 if results.get('stored', 0) > 0 else 1)
        
    except KeyboardInterrupt:
        print("\nОстановлено пользователем")
        sys.exit(130)
    except FileNotFoundError as config_err:
        print(f"{config_err}")
        print("\nСоздайте config/config.env с PG_* параметрами")
        sys.exit(1)
    except Exception as fatal_error:
        print(f"Ошибка: {fatal_error}")
        print("\nПроверьте config/config.env и подключение к БД")
        sys.exit(1)


if __name__ == "__main__":
    main()
