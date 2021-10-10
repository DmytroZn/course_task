from loguru import logger
import sys

logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} {level} {message}", filter="my_module", level="INFO")
logger.add("log.log", rotation="60 min", retention="1 day")
