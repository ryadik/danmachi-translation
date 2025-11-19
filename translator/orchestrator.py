import os
import subprocess
import time
import json
import re
import shutil
import sys
import uuid
from typing import Dict, Any, List

from .logger import setup_loggers, system_logger, input_logger, output_logger
from . import config
from . import chapter_splitter
from . import task_manager
from . import term_collector

def _run_workers_pooled(max_workers: int, tasks: List[str], prompt_template: str, workspace_paths: Dict[str, str], output_dir_key: str, cli_args: Dict[str, Any], glossary_str: str = "", style_guide_str: str = ""):
    active_processes = []
    task_queue = list(tasks)
    all_successful = True
    total_tasks = len(tasks)
    completed_tasks_count = 0

    while task_queue or active_processes:
        while len(active_processes) < max_workers and task_queue:
            task_path = task_queue.pop(0)
            worker_id = uuid.uuid4().hex[:6]
            try:
                in_progress_path = task_manager.move_task(task_path, workspace_paths["in_progress"])
                
                with open(in_progress_path, 'r', encoding='utf-8') as f:
                    chunk_content = f.read()

                final_prompt = prompt_template.replace('{text}', chunk_content)
                final_prompt = final_prompt.replace('{glossary}', glossary_str)
                final_prompt = final_prompt.replace('{style_guide}', style_guide_str)
                
                input_logger.info(f"[id: {worker_id}] --- PROMPT FOR: {os.path.basename(in_progress_path)} ---\n{final_prompt}\n")

                output_filename = os.path.basename(in_progress_path).replace('.txt', f'_{output_dir_key}.json' if output_dir_key == 'terms' else '_translated.txt')
                output_path = os.path.join(workspace_paths[output_dir_key], output_filename)
                
                command = ['gemini', '-p', final_prompt, '--output-format', cli_args.get('output_format', 'text')]

                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
                active_processes.append({"process": proc, "in_progress_path": in_progress_path, "output_path": output_path, "id": worker_id})
                system_logger.info(f"[Orchestrator] Запущен воркер [id: {worker_id}] для: {os.path.basename(in_progress_path)}")

            except Exception as e:
                system_logger.critical(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА при запуске воркера [id: {worker_id}] для {task_path}: {e}", exc_info=True)
                all_successful = False
                if 'in_progress_path' in locals() and os.path.exists(in_progress_path):
                    task_manager.move_task(in_progress_path, workspace_paths["failed"])
                continue

        remaining_processes = []
        for p_info in active_processes:
            proc = p_info["process"]
            worker_id = p_info["id"]
            if proc.poll() is not None:
                worker_name = os.path.basename(p_info['in_progress_path'])
                stdout_output, stderr_output = proc.communicate()
                
                if proc.returncode != 0:
                    all_successful = False
                    system_logger.error(f"[Orchestrator] Воркер [id: {worker_id}] для {worker_name} завершился с ошибкой (код: {proc.returncode}).")
                    output_logger.error(f"[id: {worker_id}] --- FAILED OUTPUT FROM: {worker_name} ---\n{stderr_output.strip()}\n")
                    task_manager.move_task(p_info['in_progress_path'], workspace_paths["failed"])
                    if os.path.exists(p_info['output_path']): os.remove(p_info['output_path'])
                else:
                    completed_tasks_count += 1
                    system_logger.info(f"[Orchestrator] Воркер [id: {worker_id}] для {worker_name} успешно завершен. ({completed_tasks_count}/{total_tasks})")
                    
                    try:
                        with open(p_info['output_path'], 'w', encoding='utf-8') as f_out:
                            f_out.write(stdout_output)
                        output_logger.info(f"[id: {worker_id}] --- SUCCESSFUL OUTPUT FROM: {worker_name} ---\n{stdout_output}\n")
                    except Exception as e:
                        system_logger.error(f"[Orchestrator] Ошибка при записи вывода от воркера [id: {worker_id}]: {e}")

                    if output_dir_key != "done":
                        task_manager.move_task(p_info['in_progress_path'], workspace_paths["done"])
                    else:
                        os.remove(p_info['in_progress_path'])
            else:
                remaining_processes.append(p_info)
        
        active_processes = remaining_processes
        time.sleep(1)

    return all_successful

def run_translation_process(chapter_file_path: str, cleanup: bool, resume: bool, force_split: bool):
    debug_mode = not cleanup
    try:
        cfg = config.load_config()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось загрузить config.json. {e}")
        return

    max_workers = cfg.get("max_concurrent_workers", 3)
    chapter_name = os.path.basename(os.path.dirname(chapter_file_path))
    workspace_paths = task_manager.setup_task_workspace(cfg['workspace_dir'], chapter_name)
    
    setup_loggers(workspace_paths["logs"], debug_mode)
    system_logger.info("--- Запуск нового процесса перевода ---")
    system_logger.debug(f"Режим отладки: {debug_mode}")

    lock_file = os.path.join(workspace_paths["base"], ".lock")
    discovery_checkpoint = os.path.join(workspace_paths["base"], ".stage_discovery_complete")
    translation_checkpoint = os.path.join(workspace_paths["base"], ".stage_translation_complete")

    if os.path.exists(lock_file) and not resume:
        system_logger.warning(f"[Orchestrator] ОБНАРУЖЕНА БЛОКИРОВКА...")
        sys.exit(1)

    if force_split and os.path.exists(workspace_paths["base"]):
        system_logger.info("[Orchestrator] Обнаружен флаг --force-split...")
        task_manager.cleanup_workspace(workspace_paths)
        workspace_paths = task_manager.setup_task_workspace(cfg['workspace_dir'], chapter_name)
        setup_loggers(workspace_paths["logs"], debug_mode)

    try:
        with open(lock_file, 'w') as f: f.write(str(os.getpid()))
        system_logger.info(f"[Orchestrator] Рабочая директория для главы '{chapter_name}' создана и заблокирована.")

        if resume:
            task_manager.requeue_stalled_and_failed(workspace_paths)

        if not task_manager.get_pending_tasks(workspace_paths) and not resume:
            system_logger.info("[Orchestrator] Запуск разделения главы на части...")
            chapter_splitter.split_chapter_intelligently(chapter_file_path, workspace_paths["pending"], cfg['chapter_splitter']['target_chunk_size'], cfg['chapter_splitter']['max_part_chars'])
            system_logger.info("[Orchestrator] Глава успешно разделена на части.")
        
        if not os.path.exists(discovery_checkpoint):
            system_logger.info("\n--- ЭТАП 1: Поиск новых терминов ---")
            glossary_content = "{}"
            try:
                with open("data/glossary.json", 'r', encoding='utf-8') as f:
                    glossary_content = f.read() or "{}"
            except FileNotFoundError:
                system_logger.warning("Файл 'data/glossary.json' не найден.")
                choice = input("Продолжить с пустым глоссарием? (y/n): ").lower()
                if choice == 'y':
                    with open("data/glossary.json", 'w', encoding='utf-8') as f: f.write(glossary_content)
                    system_logger.info("Создан пустой 'data/glossary.json'.")
                else:
                    system_logger.info("Операция прервана пользователем.")
                    return

            with open("translator/prompts/term_discovery.txt", 'r', encoding='utf-8') as f:
                term_prompt_template = f.read()

            pending_tasks_discovery = task_manager.get_pending_tasks(workspace_paths)
            if pending_tasks_discovery:
                discovery_success = _run_workers_pooled(max_workers, pending_tasks_discovery, term_prompt_template, workspace_paths, "terms", {"output_format": "json"}, glossary_str=glossary_content)
                if not discovery_success:
                    system_logger.error("[Orchestrator] Этап поиска терминов завершился с ошибками.")
                    return

            system_logger.info("\n--- Сбор и подтверждение терминов ---")
            newly_found_terms = term_collector.collect_and_deduplicate_terms(workspace_paths)
            approved_terms = term_collector.present_for_confirmation(newly_found_terms)
            
            if approved_terms is None:
                system_logger.info("[Orchestrator] Пользователь отменил операцию. Выход.")
                return
            if approved_terms:
                term_collector.update_glossary_file(approved_terms, "data/glossary.json")
            else:
                system_logger.info("[Orchestrator] Нет новых терминов для добавления в глоссарий.")
            
            with open(discovery_checkpoint, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
            system_logger.info("[Orchestrator] Чекпоинт 'discovery_complete' создан.")
        else:
            system_logger.info("\n[Orchestrator] Обнаружен чекпоинт. Пропуск этапа поиска терминов.")
        
        if not os.path.exists(translation_checkpoint):
            system_logger.info("\n--- ЭТАП 2: Перевод чанков ---")
            task_manager.requeue_completed_tasks(workspace_paths)
            
            try:
                with open("translator/prompts/translation.txt", 'r', encoding='utf-8') as f:
                    translation_prompt_template = f.read()
                with open("data/glossary.json", 'r', encoding='utf-8') as f:
                    glossary_content = f.read()
                with open("data/style_guide.md", 'r', encoding='utf-8') as f:
                    style_guide_content = f.read()
            except FileNotFoundError as e:
                system_logger.error(f"[Orchestrator] Не найден обязательный файл (глоссарий, стайлгайд или промпт): {e}")
                return

            pending_tasks_for_translation = task_manager.get_pending_tasks(workspace_paths)
            if pending_tasks_for_translation:
                translation_success = _run_workers_pooled(max_workers, pending_tasks_for_translation, translation_prompt_template, workspace_paths, "done", {"output_format": "text"}, glossary_str=glossary_content, style_guide_str=style_guide_content)
                if not translation_success:
                    system_logger.error("[Orchestrator] Этап перевода завершился с ошибками.")
                    return
            
            with open(translation_checkpoint, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
            system_logger.info("[Orchestrator] Чекпоинт 'translation_complete' создан.")
        else:
            system_logger.info("\n[Orchestrator] Обнаружен чекпоинт. Пропуск этапа перевода.")

        system_logger.info("\n--- Сборка итогового файла ---")
        done_dir = workspace_paths["done"]
        translated_chunks = [os.path.join(done_dir, f) for f in os.listdir(done_dir) if f.endswith("_translated.txt")]
        
        def get_part_number(filename):
            match = re.search(r'part_(\d+)_translated.txt', filename)
            return int(match.group(1)) if match else -1

        translated_chunks.sort(key=lambda f: get_part_number(os.path.basename(f)))
        
        input_dir = os.path.dirname(chapter_file_path)
        final_output_path = os.path.join(input_dir, "ru.txt")
        
        with open(final_output_path, 'w', encoding='utf-8') as final_file:
            for chunk_path in translated_chunks:
                with open(chunk_path, 'r', encoding='utf-8') as chunk_file:
                    final_file.write(chunk_file.read() + "\n\n")

        system_logger.info(f"✅ Глава успешно переведена и собрана в файл: {final_output_path}")

    except Exception as e:
        system_logger.critical(f"[Orchestrator] НЕПЕРЕХВАЧЕННАЯ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
            system_logger.info("[Orchestrator] Блокировка снята.")
        
        if not debug_mode:
            system_logger.info("\n[Orchestrator] Запрошена очистка рабочей директории...")
            task_manager.cleanup_workspace(workspace_paths)
        else:
            system_logger.info("\n[Orchestrator] Процесс завершен. Рабочая директория сохранена для отладки.")
