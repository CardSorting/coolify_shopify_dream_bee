import logging
import os
import json
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Dict, Optional


class JsonFormatter(logging.Formatter):
    """Custom formatter to output logs in JSON format."""

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "name": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
            "pathname": record.pathname,
            "lineno": record.lineno,
            "funcName": record.funcName,
            "process": record.process,
            "thread": record.thread,
        }
        return json.dumps(log_record)


class Logger:
    _instances: Dict[str, logging.Logger] = {}

    def __init__(self, name: str, level: int = logging.INFO, log_dir: str = 'logs',
                 max_file_size: int = 10 * 1024 * 1024, backup_count: int = 5,
                 use_timed_rotation: bool = False):
        self.name = name
        self.level = level
        self.log_dir = Path(log_dir)
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.use_timed_rotation = use_timed_rotation
        self._logger = self._get_or_create_logger()

    def _get_or_create_logger(self) -> logging.Logger:
        """Retrieve an existing logger or create a new one if it doesn't exist."""
        if self.name not in self._instances:
            self._instances[self.name] = self._configure_logger()
        return self._instances[self.name]

    def _configure_logger(self) -> logging.Logger:
        """Configure the logger with handlers for console and file output."""
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        logger.addHandler(self._create_console_handler())
        logger.addHandler(self._create_file_handler())
        logger.propagate = False
        return logger

    def _create_console_handler(self) -> logging.StreamHandler:
        """Create a console handler with JSON formatting."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self._create_json_formatter())  # Use JSON formatter
        return console_handler

    def _create_file_handler(self) -> logging.Handler:
        """Create a file handler with rotation support and JSON formatting."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = self.log_dir / f"{self.name}.json"  # Save logs in JSON format

        if self.use_timed_rotation:
            file_handler = TimedRotatingFileHandler(
                log_file_path, when="midnight", interval=1,
                backupCount=self.backup_count, encoding="utf-8"
            )
        else:
            file_handler = RotatingFileHandler(
                log_file_path, maxBytes=self.max_file_size,
                backupCount=self.backup_count, encoding="utf-8"
            )

        file_handler.setFormatter(self._create_json_formatter())  # Use JSON formatter
        return file_handler

    @staticmethod
    def _create_json_formatter() -> logging.Formatter:
        """Create a JSON formatter instance."""
        return JsonFormatter()  # Use the custom JSON formatter

    def get_logger(self) -> logging.Logger:
        """Return the configured logger instance."""
        return self._logger

    @classmethod
    def get_instance(cls, name: str, **kwargs) -> logging.Logger:
        """Get or create a singleton instance of a logger."""
        if name not in cls._instances:
            cls(name, **kwargs)
        return cls._instances[name]

    @staticmethod
    def set_global_level(level: int) -> None:
        """Set the logging level for all loggers."""
        logging.getLogger().setLevel(level)

    @classmethod
    def close_all_loggers(cls) -> None:
        """Close all loggers and remove their handlers."""
        for logger in cls._instances.values():
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)
        cls._instances.clear()

    # Logging methods for convenience
    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs) -> None:
        self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        self._logger.exception(msg, *args, **kwargs)