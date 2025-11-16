import argparse
import os
import sys

# Добавляем корневую директорию проекта в путь
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from translator import orchestrator

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
        '--cleanup',
        action='store_true',
        help='Удалить рабочую директорию главы после успешного завершения.'
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
        print(f"Ошибка: Файл не найден по указанному пути: {args.chapter_file}")
        sys.exit(1)
        
    if not os.path.isfile(args.chapter_file):
        print(f"Ошибка: Указанный путь не является файлом: {args.chapter_file}")
        sys.exit(1)

    try:
        # Передаем все аргументы в оркестратор
        orchestrator.run_translation_process(
            chapter_file_path=args.chapter_file,
            cleanup=args.cleanup,
            resume=args.resume,
            force_split=args.force_split
        )
    except Exception as e:
        print(f"\n--- КРИТИЧЕСКАЯ ОШИБКА В ПРОЦЕССЕ ВЫПОЛНЕНИЯ ---")
        print(f"Тип ошибки: {type(e).__name__}")
        print(f"Сообщение: {e}")
        print("-------------------------------------------------")
        sys.exit(1)

if __name__ == '__main__':
    main()