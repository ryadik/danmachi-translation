import json
import os
from typing import List, Dict, Any, Optional

def collect_terms(workspace_paths: dict) -> List[Dict[str, Any]]:
    """
    Собирает и объединяет термины из всех JSON-файлов в директории 'terms'.
    
    Args:
        workspace_paths (dict): Словарь с путями к рабочим директориям.

    Returns:
        list: Единый, дедуплицированный список словарей с терминами.
    """
    terms_dir = workspace_paths.get("terms")
    if not terms_dir or not os.path.exists(terms_dir):
        print(f"[TermCollector] Директория для терминов не найдена: {terms_dir}")
        return []

    all_terms = []
    seen_originals = set()

    for filename in os.listdir(terms_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(terms_dir, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    terms_from_file = json.load(f)
                    if isinstance(terms_from_file, list):
                        for term in terms_from_file:
                            original = term.get("original")
                            if original and original not in seen_originals:
                                all_terms.append(term)
                                seen_originals.add(original)
            except (json.JSONDecodeError, IOError) as e:
                print(f"[TermCollector] Ошибка чтения или парсинга файла '{filename}': {e}")
    
    # Сортируем для консистентного вывода
    all_terms.sort(key=lambda x: x.get('original', ''))
    return all_terms

def present_for_confirmation(terms: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """
    Отображает список терминов в консоли и запускает интерактивный режим
    для подтверждения, редактирования или удаления.

    Returns:
        list | None: Финальный список терминов, если пользователь его подтвердил,
                      иначе None.
    """
    if not terms:
        print("\n[TermCollector] Новых терминов для добавления не найдено.")
        return []

    print("\n" + "="*30)
    print("  Найдены новые термины")
    print("="*30)
    
    current_terms = list(terms) # Создаем рабочую копию

    while True:
        for i, term in enumerate(current_terms):
            print(f"  {i+1:02d}. {term.get('original', '')} -> {term.get('translation', '')} ({term.get('note', '')})")
        
        print("\n--- Команды ---")
        print("  - 'ok' или 'yes': Принять все и продолжить.")
        print("  - 'del 1 3': Удалить термины №1 и №3.")
        print("  - 'edit 2': Редактировать термин №2.")
        print("  - 'quit' или 'exit': Отменить и выйти.")
        
        try:
            command = input("\nВведите команду: ").strip().lower()
        except EOFError:
            print("\n[TermCollector] Ввод отменен. Операция прервана.")
            return None

        if command in ['ok', 'yes', 'y']:
            return current_terms
        
        if command in ['quit', 'exit', 'q']:
            print("[TermCollector] Операция отменена пользователем.")
            return None
            
        parts = command.split()
        action = parts[0]
        
        try:
            indices = [int(p) - 1 for p in parts[1:]]
            if not all(0 <= i < len(current_terms) for i in indices):
                print("Ошибка: Неверный номер термина.")
                continue

            if action == 'del':
                # Сортируем индексы в обратном порядке, чтобы удаление не сбивало нумерацию
                for i in sorted(indices, reverse=True):
                    del current_terms[i]
                print(f"Удалено {len(indices)} терминов.")

            elif action == 'edit':
                if len(indices) != 1:
                    print("Ошибка: Редактировать можно только один термин за раз.")
                    continue
                
                idx_to_edit = indices[0]
                term_to_edit = current_terms[idx_to_edit]
                
                print(f"\n--- Редактирование термина №{idx_to_edit + 1} ---")
                print(f"Оригинал: {term_to_edit['original']}")
                
                new_trans = input(f"  Новый перевод (Enter, чтобы оставить '{term_to_edit['translation']}'): ").strip()
                if new_trans: term_to_edit['translation'] = new_trans
                
                new_note = input(f"  Новое примечание (Enter, чтобы оставить '{term_to_edit['note']}'): ").strip()
                if new_note: term_to_edit['note'] = new_note
                
                print("Термин обновлен.")

            else:
                print(f"Неизвестная команда: '{action}'")

        except (ValueError, IndexError):
            print("Ошибка: Неверный формат команды. Укажите 'команда номер'.")

def update_glossary_file(new_terms: List[Dict[str, Any]], glossary_path: str):
    """
    Загружает основной файл глоссария, добавляет в него новые термины
    и сохраняет обратно.
    """
    if not new_terms:
        return

    try:
        with open(glossary_path, 'r', encoding='utf-8') as f:
            glossary_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Если файл не найден или пуст, создаем новую структуру
        glossary_data = {"characters": {}, "terminology": {}}

    print(f"\n[TermCollector] Обновление основного глоссария: {glossary_path}")
    added_count = 0
    for term in new_terms:
        original = term.get("original")
        if not original:
            continue

        # Простое правило: если в примечании есть "персонаж", добавляем в characters, иначе в terminology
        note = term.get("note", "").lower()
        category = "characters" if "персонаж" in note else "terminology"
        
        # Создаем ID для термина
        term_id = original.lower().replace(" ", "_").replace("・", "_")

        if term_id not in glossary_data[category]:
            glossary_data[category][term_id] = {
                "name": {
                    "ru": term.get("translation", ""),
                    "jp": original
                },
                "description": term.get("note", ""),
                "aliases": []
            }
            added_count += 1
    
    if added_count > 0:
        try:
            with open(glossary_path, 'w', encoding='utf-8') as f:
                json.dump(glossary_data, f, ensure_ascii=False, indent=2, sort_keys=True)
            print(f"[TermCollector] Успешно добавлено {added_count} новых терминов в глоссарий.")
        except IOError as e:
            print(f"[TermCollector] КРИТИЧЕСКАЯ ОШИБКА: Не удалось сохранить обновленный глоссарий: {e}")
    else:
        print("[TermCollector] Нет новых уникальных терминов для добавления в глоссарий.")
