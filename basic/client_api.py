import os
import requests
from typing import List, Dict, Any
from basic.logger import Logger


class MarketplaceAPI:
    def __init__(self, client_name: str = "MarketplaceAPI"):
        self.api_endpoint = os.getenv('API_URL', 'http://final-project.simulative.ru/data')
        self.logger = Logger(client_name).get_logger()
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MarketplaceDataCollector/1.0'})

    def fetch_sales_data(self, target_date: str) -> List[Dict[str, Any]]:
        query_params = {'date': target_date}       
        self.logger.info(f"Начинаем загрузку продаж за дату: {target_date}")    
        try:
            response = self.session.get(
                self.api_endpoint, 
                params=query_params, 
                timeout=45
            )       
            response.raise_for_status()   
            sales_data = response.json()           
            records_count = len(sales_data) if isinstance(sales_data, list) else 0         
            if records_count > 0:
                self.logger.info(f"Получено {records_count} продаж за {target_date}")
                return sales_data
            else:
                self.logger.warning(f"Нет продажных данных за {target_date}")
                return []                
        except requests.exceptions.Timeout:
            self.logger.error(f"Таймаут запроса к API для {target_date}")
            return []
        except requests.exceptions.ConnectionError:
            self.logger.error(f"Ошибка сети при запросе {target_date}")
            return []
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f"HTTP {response.status_code}: {response.text[:200]}...")
            return []
        except ValueError as json_err:
            self.logger.error(f"Некорректный JSON ответ для {target_date}: {json_err}")
            return []
        except Exception as unexpected:
            self.logger.error(f"Неожиданная ошибка для {target_date}: {unexpected}")
            return []

    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()
