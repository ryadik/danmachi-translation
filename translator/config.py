import json
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')

def load_config():
    """
    Загружает конфигурационный файл config.json.
    
    Возвращает:
        dict: Словарь с настройками.
    
    Вызывает:
        FileNotFoundError: Если файл config.json не найден.
        json.JSONDecodeError: Если файл имеет неверный формат JSON.
    """
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Файл конфигурации не найден по пути: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# Пример использования (можно закомментировать или удалить)
if __name__ == '__main__':
    try:
        config = load_config()
        print("Конфигурация успешно загружена:")
        print(json.dumps(config, indent=2, ensure_ascii=False))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Ошибка: {e}")
