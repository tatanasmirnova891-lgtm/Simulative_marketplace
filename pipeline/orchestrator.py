from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Optional
from basic.logger import get_logger


class DataPipelineCoordinator(ABC):
    def __init__(
        self, 
        api_service, 
        data_validator, 
        storage_layer,
        pipeline_name: str = "ETLCoordinator"
    ):
        self.api_fetcher = api_service
        self.data_processor = data_validator
        self.database_store = storage_layer
        self.logger = get_logger(pipeline_name)
        self._daily_metrics = {'processed': 0, 'stored': 0, 'errors': 0}
    
    def _process_single_day(self, target_date: date) -> bool:
        self.logger.info(f"Обработка данных за {target_date}")
        
        try:
            raw_sales = self.api_fetcher.fetch_sales_data(target_date.isoformat())
            if not raw_sales:
                self.logger.info(f"Нет продаж за {target_date}")
                return True
            clean_data, validation_errors = self.data_processor.validate_and_normalize(raw_sales)
            self._daily_metrics['processed'] += len(clean_data)
            self._daily_metrics['errors'] += len(validation_errors)
            
            if not clean_data:
                self.logger.warning(f"Все {len(raw_sales)} записей отклонены валидацией")
                return True
            stored_count = self.database_store.store_sales_batch(clean_data)
            self._daily_metrics['stored'] += stored_count
            self.logger.info(
                f"День {target_date}: обработано {len(clean_data)}, "
                f"сохранено {stored_count}"
            )
            return True
            
        except Exception as day_error:
            self.logger.error(f"Ошибка обработки {target_date}: {day_error}")
            return False
    
    def process_date_range(self, start_date: date, end_date: date) -> dict:
        self.logger.info(f"Запуск пайплайна с {start_date} по {end_date}")
        self._daily_metrics = {'processed': 0, 'stored': 0, 'errors': 0}     
        current_day = start_date
        failed_days = []  
        while current_day <= end_date:
            success = self._process_single_day(current_day)
            if not success:
                failed_days.append(current_day.isoformat())           
            current_day += timedelta(days=1)

        self.logger.info(
            f"Пайплайн завершен: "
            f"обработано {self._daily_metrics['processed']}, "
            f"сохранено {self._daily_metrics['stored']}, "
            f"ошибок {self._daily_metrics['errors']}"
        )
        
        if failed_days:
            self.logger.warning(f"Неудачные дни: {len(failed_days)}")
        
        return self._daily_metrics.copy()
    
    @abstractmethod
    def execute(self) -> dict:
        pass
    
    def get_pipeline_stats(self) -> dict:
        return self._daily_metrics.copy()
    
class DailyLoader(DataPipelineCoordinator): 
    def execute(self) -> dict:
        from datetime import date, timedelta
        yesterday = date.today() - timedelta(days=1)
        return self.process_date_range(yesterday, yesterday)


class FullHistoryLoader(DataPipelineCoordinator):  
    def __init__(self, history_start_date: date, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.history_start = history_start_date
    
    def execute(self) -> dict:
        return self.process_date_range(self.history_start, date.today())


class IncrementalLoader(DataPipelineCoordinator):
    
    def execute(self) -> dict:
        last_processed = self._get_last_processed_date()
        return self.process_date_range(last_processed, date.today())
    
    def _get_last_processed_date(self) -> date:
        return date.today() - timedelta(days=7)
