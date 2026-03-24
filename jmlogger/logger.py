import __main__
import os as _os
import colorlog as _colorlog
import logging as _logging

log_fname = _os.path.basename(__main__.__file__) + ".log"
__all__ = ["logger"]


def _init_logger() -> _logging.Logger:
    # global _fname

    # 로그 파일 저장 폴더 생성
    log_dir = "logs"
    if not _os.path.exists(log_dir):
        _os.mkdir(log_dir)

    # 로그가 저장되는 위치
    log_file = _os.path.join(log_dir, log_fname)

    # 로그 포맷 설정
    formatter = _logging.Formatter(
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
        file_handler = _logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(_logging.DEBUG)

        # 콘솔 핸들러 세팅
        console_handler = _logging.StreamHandler()
        console_handler.setFormatter(coloredFormatter)
        console_handler.setLevel(_logging.INFO)

        # 로거에 헨들러 추가
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


logger: _logging.Logger | None = _init_logger()
