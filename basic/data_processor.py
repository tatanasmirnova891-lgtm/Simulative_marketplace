# core/data_processor.py
import pandas as pd
from typing import List, Dict, Any, Tuple
from basic.logger import get_logger
from datetime import datetime


class SalesDataTransformer:
    REQUIRED_FIELDS = {
        'client_id', 'gender', 'purchase_datetime', 'purchase_time_as_seconds_from_midnight',
        'product_id', 'quantity', 'price_per_item', 'discount_per_item', 'total_price'
    }
    VALID_GENDERS = {'M', 'F', 'male', 'female'}
    def __init__(self, service_name: str = "DataTransformer"):
        self.logger = get_logger(service_name)
        self.stats = {'valid': 0, 'invalid': 0, 'errors': []}
    def validate_and_normalize(self, raw_sales: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        self.stats = {'valid': 0, 'invalid': 0, 'errors': []}
        validated_data = []
        self.logger.info(f"Начинается обработка {len(raw_sales)} записей о продажах")       
        for idx, record in enumerate(raw_sales):
            try:
                processed_record = self._sanitize_record(record) 
                if self._check_business_rules(processed_record):
                    validated_data.append(processed_record)
                    self.stats['valid'] += 1
                else:
                    self.stats['invalid'] += 1                
            except Exception as proc_error:
                self.stats['invalid'] += 1
                self.stats['errors'].append({
                    'record_index': idx, 
                    'error': str(proc_error),
                    'raw_data': record
                })       
        self._log_processing_results(len(raw_sales), validated_data)
        return validated_data, self.stats['errors']
    
    def _sanitize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        missing_fields = self.REQUIRED_FIELDS - set(record.keys())
        if missing_fields:
            raise ValueError(f"Отсутствуют поля: {missing_fields}")
        
        sanitized = {}
        sanitized['client_id'] = int(record['client_id'])
        sanitized['gender'] = str(record['gender']).upper()
        sanitized['product_id'] = int(record['product_id'])
        
        numeric_fields = ['quantity', 'price_per_item', 'discount_per_item', 'total_price']
        for field in numeric_fields:
            sanitized[field] = float(record[field])
        sanitized['purchase_datetime'] = record['purchase_datetime']
        sanitized['purchase_time_as_seconds_from_midnight'] = int(record['purchase_time_as_seconds_from_midnight']) 
        return sanitized
    
    def _check_business_rules(self, record: Dict[str, Any]) -> bool:
        # Гендер
        if record['gender'] not in self.VALID_GENDERS:
            self.logger.warning(f"Некорректный пол: {record['gender']} (record_id: {record['client_id']})")
            return False
        
        # Нулевые продажи
        if record['quantity'] <= 0 or record['total_price'] <= 0:
            return False
        
        # Отрицательные значения
        if any(record[field] < 0 for field in ['quantity', 'price_per_item', 'discount_per_item']):
            self.logger.warning(f"Отрицательные значения в записи клиента {record['client_id']}")
            return False
        
        # Логическая проверка цены
        expected_total = record['quantity'] * (record['price_per_item'] - record['discount_per_item'])
        if abs(record['total_price'] - expected_total) > 0.01:
            self.logger.warning(f"Несоответствие total_price (client: {record['client_id']})")
            return False
        
        # Преобразование времени
        try:
            full_timestamp = self._build_timestamp(record)
            record['purchase_datetime'] = full_timestamp
            del record['purchase_time_as_seconds_from_midnight']
        except ValueError as time_error:
            self.logger.warning(f"Ошибка времени для клиента {record['client_id']}: {time_error}")
            return False
        
        return True
    
    def _build_timestamp(self, record: Dict[str, Any]) -> pd.Timestamp:
        base_date = pd.to_datetime(record['purchase_datetime'])
        seconds_offset = pd.Timedelta(seconds=record['purchase_time_as_seconds_from_midnight'])
        return base_date + seconds_offset
    
    def _log_processing_results(self, total_count: int, valid_data: List):
        success_rate = (self.stats['valid'] / total_count * 100) if total_count > 0 else 0
        self.logger.info(
            f"Обработка завершена: {self.stats['valid']}/{total_count} "
            f"({success_rate:.1f}%) валидных записей"
        )
        
        if self.stats['errors']:
            self.logger.warning(f"Найдено {len(self.stats['errors'])} проблемных записей")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        return self.stats.copy()


# Быстрый способ использования
def process_sales_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    processor = SalesDataTransformer()
    valid_data, errors = processor.validate_and_normalize(raw_data)
    return valid_data
