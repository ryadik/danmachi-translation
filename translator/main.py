import argparse
import os
import sys
import logging

# Добавляем корневую директорию проекта (родителя 'translator') в путь,
# чтобы можно было использовать абсолютные импорты от 'translator'.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from translator.logger import setup_loggers, system_logger
from translator import orchestrator

def main():
    parser = argparse.ArgumentParser(description='Запускает процесс перевода для указанной главы.', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('chapter_file', type=str, help='Путь к .txt файлу главы, которую нужно перевести.')
    parser.add_argument('--debug', action='store_true', help='Если указан, рабочая директория не будет удалена после успешного завершения для отладки.')
    parser.add_argument('--resume', action='store_true', help='Возобновить прерванный процесс. Не запускает разделение главы.')
    parser.add_argument('--force-split', action='store_true', help='Принудительно удалить существующую рабочую директорию и начать с этапа разделения.')
    args = parser.parse_args()

    if not os.path.exists(args.chapter_file) or not os.path.isfile(args.chapter_file):
        print(f"Ошибка: Файл не найден или путь не является файлом: {args.chapter_file}")
        sys.exit(1)

    try:
        # По умолчанию очистка включена. Флаг --debug отключает ее.
        cleanup_enabled = not args.debug

        orchestrator.run_translation_process(
            chapter_file_path=args.chapter_file,
            cleanup=cleanup_enabled,
            resume=args.resume,
            force_split=args.force_split
        )
    except Exception as e:
        # Используем system_logger, который к этому моменту уже должен быть настроен
        system_logger.critical(f"--- НЕПЕРЕХВАЧЕННАЯ КРИТИЧЕСКАЯ ОШИБКА ---")
        system_logger.critical(f"Тип ошибки: {type(e).__name__}")
        system_logger.critical(f"Сообщение: {e}", exc_info=True)
        system_logger.critical("-------------------------------------------------")
        sys.exit(1)

if __name__ == '__main__':
    main()