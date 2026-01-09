import os
from datetime import date, timedelta
from typing import Optional, Tuple
import requests
from pipeline.orchestrator import DataPipelineCoordinator
from basic.logger import get_logger


class FullHistoryImporter(DataPipelineCoordinator):
    def __init__(
        self,
        api_service,
        data_processor,
        database_storage,
        earliest_date: date = date(2020, 1, 1),
        service_name: str = "HistoryImporter"
    ):
        super().__init__(api_service, data_processor, database_storage, service_name)
        self.min_possible_date = earliest_date
        self.api_endpoint = os.getenv('API_URL', 'http://final-project.simulative.ru/data')
    
    def execute(self, custom_start: Optional[date] = None, custom_end: Optional[date] = None) -> dict:
        self.logger.info("Запуск импорта полной истории данных")
        start_date = custom_start or self._discover_first_available_date()
        end_date = custom_end or (date.today() - timedelta(days=1))
        total_days = (end_date - start_date).days + 1
        self.logger.info(f"Диапазон: {start_date} → {end_date} ({total_days} дней)")
        pipeline_results = self.process_date_range(start_date, end_date)
        self._generate_history_report(pipeline_results, total_days)
        return pipeline_results
    
    def _discover_first_available_date(self) -> date:
        self.logger.info("Автоматический поиск первой доступной даты...")
        
        left_bound = self.min_possible_date
        right_bound = date.today()
        while not self._check_date_has_data(right_bound):
            right_bound -= timedelta(days=365)
        while left_bound < right_bound:
            mid_date = left_bound + (right_bound - left_bound) // 2
            
            if self._check_date_has_data(mid_date):
                right_bound = mid_date
            else:
                left_bound = mid_date + timedelta(days=1)        
        self.logger.info(f"Первая дата с данными: {left_bound}")
        return left_bound
    
    def _check_date_has_data(self, check_date: date) -> bool:
        try:
            params = {'date': check_date.isoformat()}
            response = requests.get(self.api_endpoint, params=params, timeout=15)
            no_data_indicators = [
                'Информация за более ранние периоды отсутствует',
                'No data available',
                '[]',
                ''
            ]
            
            response_text = response.text.strip()
            has_sales_data = response_text not in no_data_indicators and response.status_code == 200
            if not has_sales_data:
                self.logger.debug(f"{check_date}: нет данных")
            
            return has_sales_data
            
        except requests.RequestException:
            return False
    
    def _generate_history_report(self, stats: dict, total_days: int):
        processed = stats.get('processed', 0)
        stored = stats.get('stored', 0)
        avg_daily_sales = processed / total_days if total_days > 0 else 0
        avg_daily_stored = stored / total_days if total_days > 0 else 0
        
        self.logger.info("ИТОГОВАЯ СВОДКА ПО ИСТОРИИ:")
        self.logger.info(f"Всего обработано продаж: {processed:,}")
        self.logger.info(f"Успешно сохранено: {stored:,}")
        self.logger.info(f"Дней с данными: {total_days}")
        self.logger.info(f"Средние дневные продажи: {avg_daily_sales:.0f}")
        self.logger.info(f"Качество данных: {stored/processed*100:.1f}%" if processed > 0 else "Нет данных")
    
    def dry_run(self, start_date: Optional[date] = None) -> Tuple[date, date, int]:
        start = start_date or self._discover_first_available_date()
        end = date.today() - timedelta(days=1)
        days_count = (end - start).days + 1
        
        self.logger.info(f"DRY-RUN: загрузка займет {days_count} дней")
        return start, end, days_count

def import_full_history(api_client, data_processor, db_storage, start_date=None):
    importer = FullHistoryImporter(api_client, data_processor, db_storage)
    return importer.execute(start_date)

if __name__ == "__main__":
    print("Запуск полной исторической загрузки...")
