import os
import subprocess
import time
import json
import re
import shutil
import sys
from typing import Dict, Any, List

# Импортируем наши собственные модули
from . import config
from . import chapter_splitter
from . import task_manager
from . import term_collector

def _run_workers_pooled(max_workers: int, tasks: List[str], prompt_template: str, workspace_paths: Dict[str, str], output_dir_key: str, cli_args: Dict[str, Any], glossary_str: str = ""):
    """
    Универсальная функция для запуска воркеров в параллельном режиме с ограничением
    количества одновременных процессов.
    """
    active_processes = []
    task_queue = list(tasks)
    all_successful = True
    total_tasks = len(tasks)
    completed_tasks_count = 0

    while task_queue or active_processes:
        while len(active_processes) < max_workers and task_queue:
            task_path = task_queue.pop(0)
            try:
                in_progress_path = task_manager.move_task(task_path, workspace_paths["in_progress"])
                
                with open(in_progress_path, 'r', encoding='utf-8') as f:
                    chunk_content = f.read()

                final_prompt = prompt_template.format(text=chunk_content, glossary=glossary_str)

                output_filename = os.path.basename(in_progress_path).replace('.txt', f'_{output_dir_key}.json' if output_dir_key == 'terms' else '_translated.txt')
                output_path = os.path.join(workspace_paths[output_dir_key], output_filename)
                
                command = ['gemini', '-p', final_prompt, '--output-format', cli_args.get('output_format', 'text')]

                with open(output_path, 'w', encoding='utf-8') as out_f:
                    proc = subprocess.Popen(command, stdout=out_f, stderr=subprocess.PIPE, text=True, encoding='utf-8')
                    active_processes.append({
                        "process": proc,
                        "in_progress_path": in_progress_path,
                        "output_path": output_path
                    })
                    print(f"[Orchestrator] Запущен воркер для: {os.path.basename(in_progress_path)}")

            except Exception as e:
                print(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА при запуске воркера для {task_path}: {e}")
                all_successful = False
                if 'in_progress_path' in locals() and os.path.exists(in_progress_path):
                    task_manager.move_task(in_progress_path, workspace_paths["failed"])
                continue

        remaining_processes = []
        for p_info in active_processes:
            proc = p_info["process"]
            if proc.poll() is not None:
                stderr_output = proc.communicate()[1]
                if proc.returncode != 0:
                    all_successful = False
                    print(f"[Orchestrator] Воркер для {os.path.basename(p_info['in_progress_path'])} завершился с ошибкой (код: {proc.returncode}).")
                    print(f"  Stderr: {stderr_output.strip()}")
                    task_manager.move_task(p_info['in_progress_path'], workspace_paths["failed"])
                    if os.path.exists(p_info['output_path']): os.remove(p_info['output_path'])
                else:
                    completed_tasks_count += 1
                    print(f"[Orchestrator] Воркер для {os.path.basename(p_info['in_progress_path'])} успешно завершен. ({completed_tasks_count}/{total_tasks})")
                    if output_dir_key != "done":
                        task_manager.move_task(p_info['in_progress_path'], workspace_paths["done"])
            else:
                remaining_processes.append(p_info)
        
        active_processes = remaining_processes
        time.sleep(1)

    return all_successful

def run_translation_process(chapter_file_path: str, cleanup: bool, resume: bool, force_split: bool):
    """
    Главная функция-оркестратор. Управляет всем процессом перевода главы.
    """
    try:
        cfg = config.load_config()
        print("[Orchestrator] Конфигурация успешно загружена.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА: Не удалось загрузить config.json. {e}")
        return

    # --- 1. НАСТРОЙКА --- 
    max_workers = cfg.get("max_concurrent_workers", 3)
    chapter_name = os.path.basename(os.path.dirname(chapter_file_path))
    workspace_paths = task_manager.setup_task_workspace(cfg['workspace_dir'], chapter_name)
    
    print(f"[Orchestrator] Рабочая директория для главы '{chapter_name}' создана.")

    # --- 2. РАЗДЕЛЕНИЕ --- 
    print("[Orchestrator] Запуск разделения главы на части...")
    try:
        chapter_splitter.split_chapter_intelligently(
            chapter_file_path=chapter_file_path,
            output_dir=workspace_paths["pending"], # ИСПРАВЛЕНО
            target_chars=cfg['chapter_splitter']['target_chunk_size'],
            max_part_chars=cfg['chapter_splitter']['max_part_chars']
        )
        print("[Orchestrator] Глава успешно разделена на части.")
    except Exception as e:
        print(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА: Не удалось разделить главу. {e}")
        return
    
    # --- 3. ЭТАП 1: ПОИСК ТЕРМИНОВ --- 
    print("\n--- ЭТАП 1: Поиск новых терминов ---")
    try:
        with open("translator/prompts/term_discovery.txt", 'r', encoding='utf-8') as f:
            term_prompt_template = f.read()
    except FileNotFoundError:
        print("[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА: Файл промпта 'term_discovery.txt' не найден.")
        return

    pending_tasks_discovery = task_manager.get_pending_tasks(workspace_paths)
    if not pending_tasks_discovery:
        print("[Orchestrator] Не найдено задач для обработки. Завершение.")
        return

    discovery_cli_args = {"output_format": "json"}
    discovery_success = _run_workers_pooled(max_workers, pending_tasks_discovery, term_prompt_template, workspace_paths, "terms", discovery_cli_args)

    if not discovery_success:
        print("[Orchestrator] Этап поиска терминов завершился с ошибками. Процесс прерван.")
        return

    print("\n--- Сбор и подтверждение терминов ---")
    newly_found_terms = term_collector.collect_terms(workspace_paths)
    approved_terms = term_collector.present_for_confirmation(newly_found_terms)
    
    if approved_terms is None:
        print("[Orchestrator] Пользователь отменил операцию. Выход.")
        return
        
    if approved_terms:
        main_glossary_path = "data/glossary.json"
        term_collector.update_glossary_file(approved_terms, main_glossary_path)
    else:
        print("[Orchestrator] Нет новых терминов для добавления в глоссарий.")
    
    # --- 4. ЭТАП 2: ПЕРЕВОД --- 
    print("\n--- ЭТАП 2: Перевод чанков ---")
    task_manager.requeue_completed_tasks(workspace_paths)
    
    try:
        with open("translator/prompts/translation.txt", 'r', encoding='utf-8') as f:
            translation_prompt_template = f.read()
        with open("data/glossary.json", 'r', encoding='utf-8') as f:
            glossary_content = f.read()
    except FileNotFoundError as e:
        print(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА: Не найден файл промпта или глоссария: {e}")
        return

    pending_tasks_for_translation = task_manager.get_pending_tasks(workspace_paths)
    if not pending_tasks_for_translation:
        print("[Orchestrator] Не найдено задач для перевода. Завершение.")
        return

    translation_cli_args = {"output_format": "text"}
    
    done_dir = workspace_paths["done"]
    for f in os.listdir(done_dir):
        if f.endswith("_translated.txt"):
            os.remove(os.path.join(done_dir, f))

    translation_success = _run_workers_pooled(
        max_workers=max_workers,
        tasks=pending_tasks_for_translation,
        prompt_template=translation_prompt_template,
        workspace_paths=workspace_paths,
        output_dir_key="done",
        cli_args=translation_cli_args,
        glossary_str=glossary_content
    )

    if not translation_success:
        print("[Orchestrator] Этап перевода завершился с ошибками. Процесс прерван.")
        return

    # --- 5. СБОРКА И ОЧИСТКА --- 
    print("\n--- Сборка итогового файла ---")
    try:
        done_dir = workspace_paths["done"]
        translated_chunks = [os.path.join(done_dir, f) for f in os.listdir(done_dir) if f.endswith("_translated.txt")]
        
        def get_part_number(filename):
            match = re.search(r'part_(\d+)_translated.txt', filename)
            return int(match.group(1)) if match else -1

        translated_chunks.sort(key=lambda f: get_part_number(os.path.basename(f)))
        
        final_output_path = f"{chapter_name}_translated.txt"
        
        with open(final_output_path, 'w', encoding='utf-8') as final_file:
            for chunk_path in translated_chunks:
                with open(chunk_path, 'r', encoding='utf-8') as chunk_file:
                    final_file.write(chunk_file.read())
                final_file.write("\n\n")

        print(f"[Orchestrator] ✅ Глава успешно переведена и собрана в файл: {final_output_path}")

    except Exception as e:
        print(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА: Не удалось собрать финальный файл. {e}")
        return

    print("\n[Orchestrator] Процесс завершен.")

if __name__ == '__main__':
    test_chapter_path = "text/chapters/prologue/jp.txt"
    if os.path.exists(test_chapter_path):
        run_translation_process(test_chapter_path, cleanup=False, resume=False, force_split=True)
    else:
        print(f"Тестовый файл не найден: {test_chapter_path}")
        print("Пожалуйста, создайте его или измените путь в orchestrator.py")