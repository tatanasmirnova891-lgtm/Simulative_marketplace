import logging
import os
from datetime import datetime
from pathlib import Path

class Logger:
    def __init__(self, component: str, log_dir: str = "logs"):
        self.component = component
        self.log_directory = Path(log_dir)
        self._setup_logging()
    
    def _setup_logging(self):
        self.log_directory.mkdir(parents=True, exist_ok=True)

        log_filename = self.log_directory / f"{datetime.now():%Y-%m-%d}.log"
        
        self.logger = logging.getLogger(self.component)
        self.logger.setLevel(logging.INFO)
        
        # Избегаем дублирования
        if self.logger.handlers:
            return
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s [%(name)s] %(message)s',
            datefmt='%H:%M:%S'
        )
        
        file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def debug(self, message: str, *args, **kwargs):
        self.logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        self.logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        self.logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)
    
    def get_logger(self) -> logging.Logger:
        return self.logger

def get_logger(component_name: str) -> Logger:
    return Logger(component_name)
