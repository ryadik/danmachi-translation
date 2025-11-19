import logging
import sys

def setup_logger():
    """
    Настраивает и возвращает универсальный логгер для проекта.
    """
    # 1. Создаем логгер
    logger = logging.getLogger('TranslatorApp')
    logger.setLevel(logging.DEBUG)  # Устанавливаем самый низкий уровень для захвата всех сообщений

    # Предотвращаем дублирование сообщений, если функция вызывается повторно
    if logger.hasHandlers():
        logger.handlers.clear()

    # 2. Создаем обработчики (хендлеры)
    # Обработчик для вывода в консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # В консоль выводим только INFO и выше

    # Обработчик для вывода в файл
    # Файл будет перезаписываться при каждом запуске (mode='w')
    file_handler = logging.FileHandler('translator.log', mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # В файл пишем всё, включая отладочную информацию

    # 3. Создаем форматирование для сообщений
    # Формат для консоли: простое сообщение
    console_format = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_format)

    # Формат для файла: подробная информация
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_format)

    # 4. Добавляем обработчики к логгеру
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

# Создаем и экспортируем один экземпляр логгера для всего приложения
logger = setup_logger()
