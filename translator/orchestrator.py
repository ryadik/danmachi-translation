import os
import subprocess
import time
import json
import re
import shutil
import sys
from typing import Dict, Any, List
from .logger import logger
from . import config
from . import chapter_splitter
from . import task_manager
from . import term_collector

def _run_workers_pooled(max_workers: int, tasks: List[str], prompt_template: str, workspace_paths: Dict[str, str], output_dir_key: str, cli_args: Dict[str, Any], glossary_str: ""):
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

                if output_dir_key == 'terms':
                    log_file_path = os.path.join(workspace_paths["base"], "debug_prompts.log")
                    with open(log_file_path, 'a', encoding='utf-8') as log_f:
                        log_f.write(f"--- PROMPT FOR: {os.path.basename(in_progress_path)} ---\n{final_prompt}\n\n")

                output_filename = os.path.basename(in_progress_path).replace('.txt', f'_{output_dir_key}.json' if output_dir_key == 'terms' else '_translated.txt')
                output_path = os.path.join(workspace_paths[output_dir_key], output_filename)
                
                command = ['gemini', '-p', final_prompt, '--output-format', cli_args.get('output_format', 'text')]

                with open(output_path, 'w', encoding='utf-8') as out_f:
                    proc = subprocess.Popen(command, stdout=out_f, stderr=subprocess.PIPE, text=True, encoding='utf-8')
                    active_processes.append({"process": proc, "in_progress_path": in_progress_path, "output_path": output_path})
                    logger.info(f"[Orchestrator] Запущен воркер для: {os.path.basename(in_progress_path)}")

            except Exception as e:
                logger.critical(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА при запуске воркера для {task_path}: {e}", exc_info=True)
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
                    logger.error(f"[Orchestrator] Воркер для {os.path.basename(p_info['in_progress_path'])} завершился с ошибкой (код: {proc.returncode}).")
                    logger.error(f"  Stderr: {stderr_output.strip()}")
                    task_manager.move_task(p_info['in_progress_path'], workspace_paths["failed"])
                    if os.path.exists(p_info['output_path']): os.remove(p_info['output_path'])
                else:
                    completed_tasks_count += 1
                    logger.info(f"[Orchestrator] Воркер для {os.path.basename(p_info['in_progress_path'])} успешно завершен. ({completed_tasks_count}/{total_tasks})")
                    
                    if output_dir_key == 'terms':
                        try:
                            with open(p_info['output_path'], 'r', encoding='utf-8') as f_out:
                                worker_output = f_out.read()
                            log_file_path = os.path.join(workspace_paths["base"], "debug_output.log")
                            with open(log_file_path, 'a', encoding='utf-8') as log_f:
                                log_f.write(f"--- OUTPUT FROM: {os.path.basename(p_info['in_progress_path'])} ---\n{worker_output}\n\n")
                        except Exception as e:
                            logger.error(f"[Orchestrator] Ошибка при логировании вывода: {e}")

                    if output_dir_key != "done":
                        task_manager.move_task(p_info['in_progress_path'], workspace_paths["done"])
            else:
                remaining_processes.append(p_info)
        
        active_processes = remaining_processes
        time.sleep(1)

    return all_successful

def run_translation_process(chapter_file_path: str, cleanup: bool, resume: bool, force_split: bool):
    logger.info("--- Запуск нового процесса перевода ---")
    try:
        cfg = config.load_config()
        logger.info("[Orchestrator] Конфигурация успешно загружена.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.critical(f"[Orchestrator] КРИТИЧЕСКАЯ ОШИБКА: Не удалось загрузить config.json. {e}")
        return

    max_workers = cfg.get("max_concurrent_workers", 3)
    chapter_name = os.path.basename(os.path.dirname(chapter_file_path))
    workspace_paths = task_manager.setup_task_workspace(cfg['workspace_dir'], chapter_name)
    
    lock_file = os.path.join(workspace_paths["base"], ".lock")
    discovery_checkpoint = os.path.join(workspace_paths["base"], ".stage_discovery_complete")
    translation_checkpoint = os.path.join(workspace_paths["base"], ".stage_translation_complete")

    if os.path.exists(lock_file) and not resume:
        logger.warning(f"[Orchestrator] ОБНАРУЖЕНА БЛОКИРОВКА: Процесс для главы '{chapter_name}' уже запущен или был завершен некорректно.")
        logger.warning(f"  Используйте флаг --resume для возобновления или --force-split для принудительного перезапуска.")
        sys.exit(1)

    if force_split and os.path.exists(workspace_paths["base"]):
        logger.info("[Orchestrator] Обнаружен флаг --force-split. Полная очистка рабочей директории...")
        task_manager.cleanup_workspace(workspace_paths)
        workspace_paths = task_manager.setup_task_workspace(cfg['workspace_dir'], chapter_name)

    try:
        with open(lock_file, 'w') as f: f.write(str(os.getpid()))
        logger.info(f"[Orchestrator] Рабочая директория для главы '{chapter_name}' создана и заблокирована.")

        if resume:
            task_manager.requeue_stalled_and_failed(workspace_paths)

        if not task_manager.get_pending_tasks(workspace_paths) and not resume:
            logger.info("[Orchestrator] Запуск разделения главы на части...")
            chapter_splitter.split_chapter_intelligently(
                chapter_file_path=chapter_file_path,
                output_dir=workspace_paths["pending"],
                target_chars=cfg['chapter_splitter']['target_chunk_size'],
                max_part_chars=cfg['chapter_splitter']['max_part_chars']
            )
            logger.info("[Orchestrator] Глава успешно разделена на части.")
        
        if not os.path.exists(discovery_checkpoint):
            logger.info("\n--- ЭТАП 1: Поиск новых терминов ---")
            glossary_content = "{}"
            try:
                with open("data/glossary.json", 'r', encoding='utf-8') as f:
                    glossary_content = f.read()
            except FileNotFoundError:
                logger.warning("Файл 'data/glossary.json' не найден.")
                choice = input("Продолжить с пустым глоссарием? (y/n): ").lower()
                if choice == 'y':
                    with open("data/glossary.json", 'w', encoding='utf-8') as f: f.write(glossary_content)
                    logger.info("Создан пустой 'data/glossary.json'.")
                else:
                    logger.info("Операция прервана пользователем.")
                    return

            with open("translator/prompts/term_discovery.txt", 'r', encoding='utf-8') as f:
                term_prompt_template = f.read()

            pending_tasks_discovery = task_manager.get_pending_tasks(workspace_paths)
            if pending_tasks_discovery:
                discovery_success = _run_workers_pooled(max_workers, pending_tasks_discovery, term_prompt_template, workspace_paths, "terms", {"output_format": "json"}, glossary_str=glossary_content)
                if not discovery_success:
                    logger.error("[Orchestrator] Этап поиска терминов завершился с ошибками. Процесс прерван.")
                    return

            logger.info("\n--- Сбор и подтверждение терминов ---")
            newly_found_terms = term_collector.collect_and_deduplicate_terms(workspace_paths)
            approved_terms = term_collector.present_for_confirmation(newly_found_terms)
            
            if approved_terms is None:
                logger.info("[Orchestrator] Пользователь отменил операцию. Выход.")
                return
                
            if approved_terms:
                term_collector.update_glossary_file(approved_terms, "data/glossary.json")
            else:
                logger.info("[Orchestrator] Нет новых терминов для добавления в глоссарий.")
            
            with open(discovery_checkpoint, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("[Orchestrator] Чекпоинт 'discovery_complete' создан.")
        else:
            logger.info("\n[Orchestrator] Обнаружен чекпоинт. Пропуск этапа поиска терминов.")
        
        if not os.path.exists(translation_checkpoint):
            logger.info("\n--- ЭТАП 2: Перевод чанков ---")
            task_manager.requeue_completed_tasks(workspace_paths)
            
            with open("translator/prompts/translation.txt", 'r', encoding='utf-8') as f:
                translation_prompt_template = f.read()
            with open("data/glossary.json", 'r', encoding='utf-8') as f:
                glossary_content = f.read()

            pending_tasks_for_translation = task_manager.get_pending_tasks(workspace_paths)
            if pending_tasks_for_translation:
                translation_success = _run_workers_pooled(max_workers, pending_tasks_for_translation, translation_prompt_template, workspace_paths, "done", {"output_format": "text"}, glossary_str=glossary_content)
                if not translation_success:
                    logger.error("[Orchestrator] Этап перевода завершился с ошибками. Процесс прерван.")
                    return
            
            with open(translation_checkpoint, 'w') as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("[Orchestrator] Чекпоинт 'translation_complete' создан.")
        else:
            logger.info("\n[Orchestrator] Обнаружен чекпоинт. Пропуск этапа перевода.")

        logger.info("\n--- Сборка итогового файла ---")
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
                    final_file.write(chunk_file.read() + "\n\n")

        logger.info(f"✅ Глава успешно переведена и собрана в файл: {final_output_path}")

    except Exception as e:
        logger.critical(f"[Orchestrator] НЕПЕРЕХВАЧЕННАЯ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)
            logger.info("[Orchestrator] Блокировка снята.")
        
        if cleanup:
            logger.info("\n[Orchestrator] Запрошена очистка рабочей директории...")
            task_manager.cleanup_workspace(workspace_paths)
        else:
            logger.info("\n[Orchestrator] Процесс завершен. Рабочая директория сохранена для отладки.")