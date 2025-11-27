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

def _run_workers_pooled(max_workers: int, tasks: List[str], prompt_template: str, step_paths: Dict[str, str], output_suffix: str, cli_args: Dict[str, Any], workspace_paths: Dict[str, Any], glossary_str: str = "", style_guide_str: str = ""):
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
                in_progress_path = task_manager.move_task(task_path, step_paths["in_progress"])
                with open(in_progress_path, 'r', encoding='utf-8') as f:
                    chunk_content = f.read()

                final_prompt = prompt_template.replace('{text}', chunk_content).replace('{glossary}', glossary_str).replace('{style_guide}', style_guide_str)
                input_logger.info(f"[{worker_id}] --- PROMPT FOR: {os.path.basename(in_progress_path)} ---\n{final_prompt}\n")

                output_filename = os.path.basename(in_progress_path)
                
                if output_suffix == ".json":
                    output_path = os.path.join(workspace_paths["terms"], f"{output_filename}{output_suffix}")
                else:
                    output_path = os.path.join(step_paths["done"], output_filename)
                
                command = ['gemini', '-p', final_prompt, '--output-format', cli_args.get('output_format', 'text')]
                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
                active_processes.append({"process": proc, "in_progress_path": in_progress_path, "output_path": output_path, "id": worker_id, "is_term_discovery": output_suffix == ".json"})
                system_logger.info(f"[Orchestrator] Запущен воркер [id: {worker_id}] для: {os.path.basename(in_progress_path)}")

            except Exception as e:
                system_logger.critical(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА при запуске воркера [id: {worker_id}] для {task_path}: {e}", exc_info=True)
                all_successful = False
                if 'in_progress_path' in locals() and os.path.exists(in_progress_path):
                    task_manager.move_task(in_progress_path, step_paths["failed"])
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
                    output_logger.error(f"[{worker_id}] --- FAILED OUTPUT FROM: {worker_name} ---\n{stderr_output.strip()}\n")
                    task_manager.move_task(p_info['in_progress_path'], step_paths["failed"])
                else:
                    completed_tasks_count += 1
                    system_logger.info(f"[Orchestrator] Воркер [id: {worker_id}] для {worker_name} успешно завершен. ({completed_tasks_count}/{total_tasks})")
                    
                    try:
                        with open(p_info['output_path'], 'w', encoding='utf-8') as f_out:
                            f_out.write(stdout_output)
                        output_logger.info(f"[{worker_id}] --- SUCCESSFUL OUTPUT FROM: {worker_name} ---\n{stdout_output}\n")
                    except Exception as e:
                        system_logger.error(f"[Orchestrator] Ошибка при записи вывода от воркера [id: {worker_id}]: {e}")
                    
                    if p_info["is_term_discovery"]:
                        task_manager.move_task(p_info['in_progress_path'], step_paths["done"])
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
    
    lock_file = os.path.join(workspace_paths["base"], ".lock")
    discovery_checkpoint = os.path.join(workspace_paths["base"], ".stage_discovery_complete")
    translation_checkpoint = os.path.join(workspace_paths["base"], ".stage_translation_complete")
    reading_checkpoint = os.path.join(workspace_paths["base"], ".stage_reading_complete")

    if os.path.exists(lock_file) and not resume:
        system_logger.warning(f"[Orchestrator] ОБНАРУЖЕНА БЛОКИРОВКА для главы '{chapter_name}'. Используйте --resume или --force-split.")
        sys.exit(1)

    if force_split and os.path.exists(workspace_paths["base"]):
        system_logger.info("[Orchestrator] Обнаружен флаг --force-split. Полная очистка рабочей директории...")
        task_manager.cleanup_workspace(workspace_paths)
        workspace_paths = task_manager.setup_task_workspace(cfg['workspace_dir'], chapter_name)
        setup_loggers(workspace_paths["logs"], debug_mode)

    try:
        with open(lock_file, 'w') as f: f.write(str(os.getpid()))
        system_logger.info(f"[Orchestrator] Рабочая директория для главы '{chapter_name}' создана и заблокирована.")

        if resume:
            task_manager.requeue_stalled_and_failed(workspace_paths["steps"])

        # --- Этап 0: Разделение на чанки ---
        discovery_paths = workspace_paths["steps"]["discovery"]
        if not task_manager.get_pending_tasks(discovery_paths) and not resume:
            system_logger.info("[Orchestrator] Запуск разделения главы на чанки...")
            chapter_splitter.split_chapter_intelligently(chapter_file_path, discovery_paths["pending"], cfg['chapter_splitter']['target_chunk_size'], cfg['chapter_splitter']['max_part_chars'])
        
        # --- Этап 1: Поиск терминов ---
        if not os.path.exists(discovery_checkpoint):
            system_logger.info("\n--- ЭТАП 1: Поиск новых терминов ---")
            glossary_content = "{}"
            try:
                with open("data/glossary.json", 'r', encoding='utf-8') as f: glossary_content = f.read() or "{}"
            except FileNotFoundError:
                system_logger.warning("Файл 'data/glossary.json' не найден.")
                if input("Продолжить с пустым глоссарием? (y/n): ").lower() == 'y':
                    with open("data/glossary.json", 'w', encoding='utf-8') as f: f.write(glossary_content)
                else: system_logger.info("Операция прервана пользователем."); return
            with open("translator/prompts/term_discovery.txt", 'r', encoding='utf-8') as f: term_prompt_template = f.read()
            
            pending_tasks = task_manager.get_pending_tasks(discovery_paths)
            if pending_tasks:
                success = _run_workers_pooled(max_workers, pending_tasks, term_prompt_template, discovery_paths, ".json", {"output_format": "json"}, workspace_paths, glossary_str=glossary_content)
                if not success: system_logger.error("[Orchestrator] Этап поиска терминов завершился с ошибками."); return

            system_logger.info("\n--- Сбор и подтверждение терминов ---")
            new_terms = term_collector.collect_and_deduplicate_terms(workspace_paths)
            approved_terms = term_collector.present_for_confirmation(new_terms)
            if approved_terms is None: system_logger.info("[Orchestrator] Пользователь отменил операцию."); return
            if approved_terms: term_collector.update_glossary_file(approved_terms, "data/glossary.json")
            
            with open(discovery_checkpoint, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
            system_logger.info("[Orchestrator] Чекпоинт 'discovery_complete' создан.")
            task_manager.copy_tasks_to_next_step(discovery_paths["done"], workspace_paths["steps"]["translation"]["pending"])
        else:
            system_logger.info("\n[Orchestrator] Обнаружен чекпоинт. Пропуск этапа поиска терминов.")
        
        # --- Этап 2: Перевод ---
        translation_paths = workspace_paths["steps"]["translation"]
        if not os.path.exists(translation_checkpoint):
            system_logger.info("\n--- ЭТАП 2: Перевод чанков ---")
            try:
                with open("translator/prompts/translation.txt", 'r', encoding='utf-8') as f: translation_prompt_template = f.read()
                with open("data/glossary.json", 'r', encoding='utf-8') as f: glossary_content = f.read()
                with open("data/style_guide.md", 'r', encoding='utf-8') as f: style_guide_content = f.read()
            except FileNotFoundError as e:
                system_logger.error(f"[Orchestrator] Не найден обязательный файл: {e}"); return
            
            pending_tasks = task_manager.get_pending_tasks(translation_paths)
            if pending_tasks:
                success = _run_workers_pooled(max_workers, pending_tasks, translation_prompt_template, translation_paths, ".txt", {"output_format": "text"}, workspace_paths, glossary_str=glossary_content, style_guide_str=style_guide_content)
                if not success: system_logger.error("[Orchestrator] Этап перевода завершился с ошибками."); return
            
            with open(translation_checkpoint, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
            system_logger.info("[Orchestrator] Чекпоинт 'translation_complete' создан.")
            task_manager.copy_tasks_to_next_step(translation_paths["done"], workspace_paths["steps"]["reading"]["pending"])
        else:
            system_logger.info("\n[Orchestrator] Обнаружен чекпоинт. Пропуск этапа перевода.")

        # --- Этап 3: Вычитка ---
        reading_paths = workspace_paths["steps"]["reading"]
        if not os.path.exists(reading_checkpoint):
            system_logger.info("\n--- ЭТАП 3: Вычитка текста ---")
            try:
                with open("translator/prompts/proofreading.txt", 'r', encoding='utf-8') as f: proofreading_prompt_template = f.read()
                with open("data/glossary.json", 'r', encoding='utf-8') as f: glossary_content = f.read()
                with open("data/style_guide.md", 'r', encoding='utf-8') as f: style_guide_content = f.read()
            except FileNotFoundError as e:
                system_logger.error(f"[Orchestrator] Не найден обязательный файл: {e}"); return
            
            pending_tasks = task_manager.get_pending_tasks(reading_paths)
            if pending_tasks:
                success = _run_workers_pooled(max_workers, pending_tasks, proofreading_prompt_template, reading_paths, ".txt", {"output_format": "text"}, workspace_paths, glossary_str=glossary_content, style_guide_str=style_guide_content)
                if not success: system_logger.error("[Orchestrator] Этап вычитки завершился с ошибками."); return

            with open(reading_checkpoint, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
            system_logger.info("[Orchestrator] Чекпоинт 'reading_complete' создан.")
        else:
            system_logger.info("\n[Orchestrator] Обнаружен чекпоинт. Пропуск этапа вычитки.")

        # --- ФИНАЛЬНАЯ СБОРКА ---
        system_logger.info("\n--- Сборка итогового файла ---")
        final_chunks_dir = workspace_paths["steps"]["reading"]["done"]
        final_chunks = [os.path.join(final_chunks_dir, f) for f in sorted(os.listdir(final_chunks_dir))]
        final_chunks.sort(key=lambda f: int(re.search(r'chunk_(\d+).txt', os.path.basename(f)).group(1)))
        
        input_dir = os.path.dirname(chapter_file_path)
        final_output_path = os.path.join(input_dir, "ru.txt")
        
        with open(final_output_path, 'w', encoding='utf-8') as final_file:
            for i, chunk_path in enumerate(final_chunks):
                with open(chunk_path, 'r', encoding='utf-8') as chunk_file:
                    final_file.write(chunk_file.read())
                if i < len(final_chunks) - 1: final_file.write("\n\n")

        system_logger.info(f"✅ Глава успешно переведена и собрана в файл: {final_output_path}")

    except Exception as e:
        system_logger.critical(f"[Orchestrator] НЕПЕРЕХВАЧЕННАЯ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
            system_logger.info("[Orchestrator] Блокировка снята.")
        if not debug_mode:
            task_manager.cleanup_workspace(workspace_paths)
        else:
            system_logger.info("\n[Orchestrator] Процесс завершен. Рабочая директория сохранена для отладки.")