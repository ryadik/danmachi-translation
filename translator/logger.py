import logging
import sys
import os

# Создаем "пустые" логгеры-заглушки
system_logger = logging.getLogger('system')
input_logger = logging.getLogger('worker_input')
output_logger = logging.getLogger('worker_output')

# По умолчанию весь вывод отключен с помощью NullHandler
# Это предотвращает ошибки "No handler found" если логгер используется до конфигурации
for logger_instance in [system_logger, input_logger, output_logger]:
    logger_instance.addHandler(logging.NullHandler())
    logger_instance.propagate = False

def setup_loggers(log_dir: str, debug_mode: bool):
    """
    Настраивает все логгеры проекта.
    Если debug_mode=False, настраивает только вывод в консоль для system_logger.
    Если debug_mode=True, настраивает вывод в консоль и в 3 разных файла.
    """
    # Очищаем все предыдущие хендлеры, чтобы избежать дублирования
    for logger_instance in [system_logger, input_logger, output_logger]:
        if logger_instance.hasHandlers():
            logger_instance.handlers.clear()

    # --- ОБЩИЕ НАСТРОЙКИ ---
    # Устанавливаем минимальный уровень захвата сообщений.
    # Реальный вывод будет определяться уровнем хендлеров.
    system_logger.setLevel(logging.DEBUG)
    input_logger.setLevel(logging.DEBUG)
    output_logger.setLevel(logging.DEBUG)

    # Консольный хендлер для системного логгера (работает всегда)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    system_logger.addHandler(console_handler)

    if not debug_mode:
        # Если не дебаг-режим, добавляем NullHandler-ы для файловых логгеров
        # чтобы они "молчали", и выходим
        input_logger.addHandler(logging.NullHandler())
        output_logger.addHandler(logging.NullHandler())
        return

    # --- НАСТРОЙКИ ДЛЯ DEBUG-РЕЖИМА ---
    
    # Убедимся, что директория для логов существует
    os.makedirs(log_dir, exist_ok=True)

    # Общий формат для всех файлов логов
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # 1. Файловый хендлер для system_logger
    system_log_path = os.path.join(log_dir, 'system_output.log')
    system_file_handler = logging.FileHandler(system_log_path, mode='w', encoding='utf-8')
    system_file_handler.setLevel(logging.DEBUG)
    system_file_handler.setFormatter(file_formatter)
    system_logger.addHandler(system_file_handler)
    
    # 2. Файловый хендлер для input_logger
    input_log_path = os.path.join(log_dir, 'workers_input.log')
    input_file_handler = logging.FileHandler(input_log_path, mode='w', encoding='utf-8')
    input_file_handler.setLevel(logging.DEBUG)
    input_file_handler.setFormatter(file_formatter)
    input_logger.addHandler(input_file_handler)

    # 3. Файловый хендлер для output_logger
    output_log_path = os.path.join(log_dir, 'workers_output.log')
    output_file_handler = logging.FileHandler(output_log_path, mode='w', encoding='utf-8')
    output_file_handler.setLevel(logging.DEBUG)
    output_file_handler.setFormatter(file_formatter)
    output_logger.addHandler(output_file_handler)

    system_logger.debug("Логгеры успешно настроены в debug-режиме.")
