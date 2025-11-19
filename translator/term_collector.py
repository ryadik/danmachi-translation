import json
import os
import re
from typing import Dict, Any, Optional
from .logger import system_logger

def collect_and_deduplicate_terms(workspace_paths: dict) -> Dict[str, Any]:
    terms_dir = workspace_paths.get("terms")
    if not terms_dir or not os.path.exists(terms_dir):
        return {}
    unique_terms = {}
    for filename in os.listdir(terms_dir):
        if not filename.endswith(".json"): continue
        file_path = os.path.join(terms_dir, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as f: cli_output = json.load(f)
            if not (isinstance(cli_output, dict) and "response" in cli_output): continue
            response_str = cli_output["response"]
            match = re.search(r'```json\s*\n(.*?)\s*\n```', response_str, re.DOTALL)
            json_str = match.group(1) if match else response_str
            data = json.loads(json_str)
            for category, items in data.items():
                if not isinstance(items, dict): continue
                for term_id, term_data in items.items():
                    if term_id not in unique_terms:
                        unique_terms[term_id] = {"category": category, "data": term_data}
        except (json.JSONDecodeError, IOError, TypeError) as e:
            system_logger.error(f"[TermCollector] Ошибка обработки файла '{filename}': {e}")
    final_structure = {"characters": {}, "terminology": {}, "expressions": {}}
    for term_id, term_info in unique_terms.items():
        cat = term_info["category"]
        if cat in final_structure:
            final_structure[cat][term_id] = term_info["data"]
    return final_structure

def _edit_term(term_data: Dict[str, Any]) -> Dict[str, Any]:
    system_logger.info("\n--- Редактирование термина ---")
    for key in ["ru", "jp", "romaji"]:
        new_val = input(f"  name.{key} (Enter, чтобы оставить '{term_data['name'].get(key, '')}'): ").strip()
        if new_val: term_data['name'][key] = new_val
    new_desc = input(f"  description (Enter, чтобы оставить '{term_data.get('description', '')}'): ").strip()
    if new_desc: term_data['description'] = new_desc
    new_context = input(f"  context (Enter, чтобы оставить '{term_data.get('context', '')}'): ").strip()
    if new_context: term_data['context'] = new_context
    system_logger.info(f"  Текущие псевдонимы: {[a.get('ru', '') for a in term_data.get('aliases', [])]}")
    if input("  Редактировать псевдонимы? (y/n): ").lower() == 'y':
        term_data['aliases'] = []
        while True:
            alias_ru = input("    Добавить псевдоним (RU) (Enter для завершения): ").strip()
            if not alias_ru: break
            term_data['aliases'].append({"ru": alias_ru})
    if "characteristics" in term_data: # Персонаж
        for key, val in term_data["characteristics"].items():
            new_val = input(f"  characteristics.{key} (Enter, чтобы оставить '{val}'): ").strip()
            if new_val: term_data["characteristics"][key] = new_val
    elif "type" in term_data: # Термин
        new_type = input(f"  type (Enter, чтобы оставить '{term_data['type']}'): ").strip()
        if new_type: term_data['type'] = new_type
    system_logger.info("--- Редактирование завершено ---")
    return term_data

def present_for_confirmation(new_terms: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not any(new_terms.values()):
        system_logger.info("\n[TermCollector] Новых терминов для добавления не найдено.")
        return {}
    term_list = []
    for category, items in new_terms.items():
        for term_id, data in items.items():
            term_list.append({"id": term_id, "category": category, "data": data})
    while True:
        system_logger.info("\n" + "="*40 + "\n  Найдены новые термины для подтверждения\n" + "="*40)
        for i, term in enumerate(term_list):
            system_logger.info(f"\n--- Термин #{i+1} ---\n  ID: {term['id']} (Категория: {term['category']})\n  JP: {term['data']['name'].get('jp', 'N/A')}\n  RU: {term['data']['name'].get('ru', 'N/A')}\n  Описание: {term['data'].get('description', 'N/A')}\n  Контекст: {term['data'].get('context', 'N/A')}")
        system_logger.info("\n" + "-"*40 + "\n  Команды: ok, del <номера>, edit <номер>, quit\n" + "-"*40)
        try: command = input("\nВведите команду: ").strip().lower()
        except EOFError: return None
        if command in ['ok', 'yes', 'y']:
            final_terms = {"characters": {}, "terminology": {}, "expressions": {}}
            for term in term_list:
                if term["category"] in final_terms: final_terms[term["category"]][term["id"]] = term["data"]
            return final_terms
        if command in ['quit', 'exit', 'q']: return None
        parts = command.split()
        action = parts[0]
        try:
            indices = [int(p) - 1 for p in parts[1:]]
            if not all(0 <= i < len(term_list) for i in indices):
                system_logger.warning("Ошибка: Неверный номер термина.")
                continue
            if action == 'del':
                for i in sorted(indices, reverse=True): del term_list[i]
                system_logger.info(f"Удалено {len(indices)} терминов.")
            elif action == 'edit':
                if len(indices) != 1:
                    system_logger.warning("Ошибка: Редактировать можно только один термин за раз.")
                    continue
                idx_to_edit = indices[0]
                term_list[idx_to_edit]["data"] = _edit_term(term_list[idx_to_edit]["data"])
            else: system_logger.warning(f"Неизвестная команда: '{action}'")
        except (ValueError, IndexError): system_logger.error("Ошибка: Неверный формат команды.")

def update_glossary_file(new_terms: Dict[str, Any], glossary_path: str):
    if not any(new_terms.values()): return
    try:
        with open(glossary_path, 'r', encoding='utf-8') as f:
            content = f.read()
            glossary_data = json.loads(content) if content else {"characters": {}, "terminology": {}, "expressions": {}}
    except (FileNotFoundError, json.JSONDecodeError):
        glossary_data = {"characters": {}, "terminology": {}, "expressions": {}}
    system_logger.info(f"\n[TermCollector] Обновление основного глоссария: {glossary_path}")
    for cat in ["characters", "terminology", "expressions"]:
        if cat not in glossary_data: glossary_data[cat] = {}
    glossary_data["characters"].update(new_terms.get("characters", {}))
    glossary_data["terminology"].update(new_terms.get("terminology", {}))
    glossary_data["expressions"].update(new_terms.get("expressions", {}))
    try:
        with open(glossary_path, 'w', encoding='utf-8') as f: json.dump(glossary_data, f, ensure_ascii=False, indent=2)
        system_logger.info(f"[TermCollector] Глоссарий успешно обновлен.")
    except IOError as e:
        system_logger.critical(f"[TermCollector] КРИТИЧЕСКАЯ ОШИБКА: Не удалось сохранить обновленный глоссарий: {e}")