# import os as _os
import colorlog as _colorlog
import logging as _logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

__all__ = ["logger", "get_logger"]


_logger_instance = None


def get_logger(log_file_path: str) -> _logging.Logger:
    global _logger_instance

    path = Path(log_file_path)
    # log_path = Path(log_file_path)
    # log_dir = log_path.parent
    # log_fname = log_path.name

    # 로그 파일 저장 폴더 생성
    # if not _os.path.exists(log_dir):
    #     _os.mkdir(log_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 로그가 저장되는 위치
    # log_path = _os.path.join(log_dir, log_fname)

    # 로그 포맷 설정
    formatter = _logging.Formatter(
        # "%(asctime)s [%(levelname)s] %(message)s [%(filename)s:%(lineno)d]",
        "%(asctime)s [%(levelname)s] %(message)s [%(filename)s:%(lineno)d]",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    coloredFormatter = _colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )

    # 로거 세팅
    logger = _logging.getLogger(__name__)
    logger.setLevel(_logging.DEBUG)

    if not logger.handlers:
        # 파일 핸들러 세팅
        # file_handler = _logging.FileHandler(log_file, encoding="utf-8")
        file_handler = RotatingFileHandler(
            path, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(_logging.DEBUG)

        # 콘솔 핸들러 세팅
        console_handler = _logging.StreamHandler()
        console_handler.setFormatter(coloredFormatter)
        console_handler.setLevel(_logging.INFO)

        # 로거에 헨들러 추가
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        _logger_instance = logger

    return _logger_instance


# logger = get_logger("logs/app.log")
