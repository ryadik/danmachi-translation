import argparse
import os
import sys
import logging

# Добавляем корневую директорию проекта в путь
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from translator import orchestrator
from translator.logger import logger

def main():
    """
    Главная функция, обрабатывающая запуск из командной строки.
    """
    parser = argparse.ArgumentParser(
        description='Запускает процесс перевода для указанной главы.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'chapter_file',
        type=str,
        help='Путь к .txt файлу главы, которую нужно перевести.'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Если указан, рабочая директория не будет удалена после успешного завершения для отладки.'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Возобновить прерванный процесс. Не запускает разделение главы.'
    )
    parser.add_argument(
        '--force-split',
        action='store_true',
        help='Принудительно удалить существующую рабочую директорию и начать с этапа разделения.'
    )

    args = parser.parse_args()

    if not os.path.exists(args.chapter_file):
        logger.error(f"Файл не найден по указанному пути: {args.chapter_file}")
        sys.exit(1)
        
    if not os.path.isfile(args.chapter_file):
        logger.error(f"Указанный путь не является файлом: {args.chapter_file}")
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
        logger.critical(f"--- КРИТИЧЕСКАЯ ОШИБКА В ПРОЦЕССЕ ВЫПОЛНЕНИЯ ---")
        logger.critical(f"Тип ошибки: {type(e).__name__}")
        logger.critical(f"Сообщение: {e}", exc_info=True)
        logger.critical("-------------------------------------------------")
        sys.exit(1)

if __name__ == '__main__':
    main()
