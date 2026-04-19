# src/config.py
"""Конфигурация для подключения к ClickHouse"""

import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class ClickHouseConfig:
    """Конфигурация подключения к ClickHouse"""
    
    # Основные параметры
    host: str
    port: str
    username: str
    password: str
    
    # Дополнительные параметры
    protocol: str = "https"  # http или https
    driver: str = "com.clickhouse.jdbc.ClickHouseDriver"
    socket_timeout: int = 600000
    secure: bool = True
    
    # Параметры подключения
    connection_timeout: int = 30000
    socket_timeout_ms: int = 600000
    
    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Загружает конфигурацию из переменных окружения"""
        return cls(
            host=os.environ.get('CH_HOST', 'localhost'),
            port=os.environ.get('CH_PORT', '8443'),
            username=os.environ.get('CH_USERNAME', ''),
            password=os.environ.get('CH_PASSWORD', ''),
            protocol=os.environ.get('CH_PROTOCOL', 'https'),
            secure=os.environ.get('CH_SECURE', 'https').lower() == 'https'
        )
    
    @classmethod
    def from_creds_file(cls, creds_path: str = "creds.py") -> "ClickHouseConfig":
        """Загружает конфигурацию из creds.py файла"""
        import importlib.util
        import sys
        
        # Проверяем существование файла
        if not os.path.exists(creds_path):
            raise FileNotFoundError(f"Creds file not found: {creds_path}")
        
        # Создаем spec с проверкой
        spec = importlib.util.spec_from_file_location("creds", creds_path)
        if spec is None:
            raise ImportError(f"Cannot load module from {creds_path}")
        
        # Загружаем модуль
        creds = importlib.util.module_from_spec(spec)
        sys.modules["creds"] = creds
        spec.loader.exec_module(creds) if spec.loader else None
        
        return cls(
            host=creds.CH_HOST,
            port=creds.CH_PORT,
            username=creds.CH_USERNAME,
            password=creds.CH_PASSWORD,
            protocol=getattr(creds, 'CH_PROTOCOL', 'https'),
            secure=getattr(creds, 'CH_SECURE', 'https').lower() == 'https'
        )
    
    def jdbc_url(self) -> str:
        """Формирует JDBC URL для ClickHouse"""
        protocol = "jdbc:clickhouse" if self.protocol == "http" else "jdbc:clickhouse"
        return f"{protocol}://{self.host}:{self.port}"
    
    def http_url(self) -> str:
        """Формирует HTTP URL для REST API"""
        return f"{self.protocol}://{self.host}:{self.port}"
    
    def properties(self) -> dict:
        """Возвращает свойства для JDBC подключения"""
        return {
            "user": self.username,
            "password": self.password,
            "driver": self.driver,
            "socket_timeout": str(self.socket_timeout)
        }
    
    def to_dict(self) -> dict:
        """Возвращает конфигурацию в виде словаря (без пароля)"""
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "protocol": self.protocol,
            "secure": self.secure
        }