"""
Production-grade logging utility for the E-commerce Analytics Engine.
Supports multi-client logging with rotation, file handlers, and console output.
"""
import json
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import sys
from functools import wraps


class AnalyticsLogger:
    """Centralized logger factory with client-specific configuration."""
    
    _loggers: Dict[str, logging.Logger] = {}
    
    @classmethod
    def get_logger(cls, name: str, config: Optional[Dict[str, Any]] = None) -> logging.Logger:
        """
        Get or create logger instance with client-specific configuration.
        
        Args:
            name: Logger name (module/client)
            config: Client config with logging settings
            
        Returns:
            Configured logger instance
        """
        if name not in cls._loggers:
            logger = logging.getLogger(name)
            logger.setLevel(logging.INFO)
            cls._loggers[name] = logger
            
            # Prevent duplicate handlers
            if not logger.handlers:
                cls._setup_handlers(logger, config)
                
        return cls._loggers[name]
    
    @classmethod
    def _setup_handlers(cls, logger: logging.Logger, config: Optional[Dict[str, Any]]) -> None:
        """Setup file, console, and rotating handlers."""
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # Client-specific file handler
        if config and 'paths' in config and 'logs' in config['paths']:
            log_path = Path(config['paths']['logs'])
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s | %(name)s | %(client_id)s | %(levelname)s | %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        # Root file handler for all clients
        root_log_path = Path("logs/analytics_engine.log")
        root_log_path.parent.mkdir(parents=True, exist_ok=True)
        
        root_handler = logging.handlers.RotatingFileHandler(
            root_log_path,
            maxBytes=50*1024*1024,  # 50MB
            backupCount=10,
            encoding='utf-8'
        )
        root_handler.setLevel(logging.DEBUG)
        root_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s'
        )
        root_handler.setFormatter(root_formatter)
        logger.addHandler(root_handler)


def setup_logger(name: str, config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Factory function to setup logger with client context.
    
    Args:
        name: Logger name
        config: Client configuration
        
    Returns:
        Configured logger instance
    """
    logger = AnalyticsLogger.get_logger(name, config)
    
    # Add client_id to extra fields if available
    if config and 'client_id' in config:
        logger.client_id = config['client_id']
    else:
        logger.client_id = 'system'
    
    return logger


def log_pipeline_stage(logger: logging.Logger, stage: str, status: str = "START") -> None:
    """Log pipeline stage transitions."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    emoji = {"START": "🚀", "SUCCESS": "✅", "FAIL": "❌", "SKIP": "⏭️"}.get(status, "⚠️")
    logger.info(f"{emoji} [{stage}] {status} - {timestamp}")


def safe_log_dataframe(logger: logging.Logger, df_name: str, df, max_rows: int = 5) -> None:
    """
    Safely log DataFrame info without exposing sensitive data.
    
    Args:
        logger: Logger instance
        df_name: DataFrame name
        df: DataFrame to log
        max_rows: Max rows to show in sample
    """
    if df is None or df.empty:
        logger.info(f"📊 {df_name}: Empty DataFrame")
        return
    
    rows, cols = len(df), len(df.columns)
    logger.info(f"📊 {df_name}: {rows} rows, {cols} cols")
    logger.debug(f"📋 {df_name} columns: {list(df.columns)}")
    
    # Safe sample without sensitive columns
    safe_cols = [col for col in df.columns if 'id' not in col.lower() and 'email' not in col.lower()]
    if safe_cols:
        sample_df = df[safe_cols].head(max_rows)
        logger.debug(f"👀 {df_name} sample:\n{sample_df.to_string()}")


def log_error_with_context(logger: logging.Logger, error: Exception, context: Dict[str, Any]) -> None:
    """Log error with structured context."""
    logger.error(f"💥 Error: {str(error)}", exc_info=True)
    logger.error(f"📍 Context: {json.dumps(context, default=str, indent=2)}")


# Decorator for safe DataFrame operations
def safe_dataframe_operation(func):
    """Decorator to safely handle DataFrame operations with logging."""
    @wraps(func)
    def wrapper(*args, logger=None, config=None, **kwargs):
        func_logger = logger or setup_logger(f"{func.__module__}.{func.__name__}", config)
        
        try:
            result = func(*args, **kwargs)
            safe_log_dataframe(func_logger, f"{func.__name__}_result", result)
            return result
        except Exception as e:
            func_logger.error(f"❌ {func.__name__} failed: {str(e)}", exc_info=True)
            log_error_with_context(func_logger, e, {"func": func.__name__, "args": str(args)})
            return None
    return wrapper