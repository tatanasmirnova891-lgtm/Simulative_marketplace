from datetime import date, timedelta
from pipeline.orchestrator import DataPipelineCoordinator
from basic.logger import get_logger


class YesterdaySalesProcessor(DataPipelineCoordinator):
    def __init__(
        self, 
        api_service, 
        data_processor, 
        database_storage,
        service_name: str = "DailySalesProcessor"
    ):
        super().__init__(api_service, data_processor, database_storage, service_name)
        self.target_period = "yesterday"
    
    def execute(self) -> dict:
        processing_date = self._calculate_target_date()     
        self.logger.info(f"Ежедневная загрузка: {processing_date}")
        pipeline_stats = self.process_date_range(processing_date, processing_date)
        self._report_daily_summary(pipeline_stats)
        return pipeline_stats
    
    def _calculate_target_date(self) -> date:
        return date.today() - timedelta(days=1)
    
    def _report_daily_summary(self, stats: dict):
        processed = stats.get('processed', 0)
        stored = stats.get('stored', 0)
        errors = stats.get('errors', 0)
        
        self.logger.info(
            f"Дневная сводка | "
            f"Продаж: {processed:,} | "
            f"В БД: {stored:,} | "
            f"Отклонено: {errors:,}"
        )
    
    @property
    def is_cron_ready(self) -> bool:
        now = date.today()
        return True 

def run_daily_etl(api_client, data_processor, db_storage):
    logger = get_logger("QuickDailyETL")
    logger.info("Быстрый запуск ежедневной ETL")
    
    daily_processor = YesterdaySalesProcessor(api_client, data_processor, db_storage)
    return daily_processor.execute()


if __name__ == "__main__":
    print("Запуск ежедневной ETL...")
