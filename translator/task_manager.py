import os
import shutil
from .logger import system_logger

def setup_task_workspace(workspace_root: str, chapter_name: str) -> dict:
    base_dir = os.path.join(workspace_root, chapter_name)
    paths = {
        "base": base_dir,
        "logs": os.path.join(base_dir, "logs"),
        "terms": os.path.join(base_dir, "terms"),
        "steps": {}
    }
    steps = ["discovery", "translation", "reading"]
    process_dirs = ["pending", "in_progress", "failed", "done"]
    for step in steps:
        step_path = os.path.join(base_dir, "steps", step)
        paths["steps"][step] = {"base": step_path}
        for process_dir in process_dirs:
            full_path = os.path.join(step_path, process_dir)
            os.makedirs(full_path, exist_ok=True)
            paths["steps"][step][process_dir] = full_path
    os.makedirs(paths["logs"], exist_ok=True)
    os.makedirs(paths["terms"], exist_ok=True)
    return paths

def get_pending_tasks(step_paths: dict) -> list:
    pending_dir = step_paths["pending"]
    if not os.path.exists(pending_dir): return []
    return [os.path.join(pending_dir, f) for f in os.listdir(pending_dir) if os.path.isfile(os.path.join(pending_dir, f))]

def move_task(source_path: str, dest_dir: str) -> str:
    if not os.path.exists(source_path):
        system_logger.error(f"Попытка переместить несуществующий файл: {source_path}")
        raise FileNotFoundError(f"Исходный файл задачи не найден: {source_path}")
    dest_path = os.path.join(dest_dir, os.path.basename(source_path))
    shutil.move(source_path, dest_path)
    return dest_path

def copy_tasks_to_next_step(source_done_dir: str, dest_pending_dir: str):
    if not os.path.exists(source_done_dir): return
    copied_count = 0
    # ИСПРАВЛЕНО: Копируем любые .txt файлы, а не только с префиксом part_
    for filename in os.listdir(source_done_dir):
        if filename.endswith(".txt"):
            source_file = os.path.join(source_done_dir, filename)
            dest_file = os.path.join(dest_pending_dir, filename)
            shutil.copy2(source_file, dest_file)
            copied_count += 1
    if copied_count > 0:
        system_logger.info(f"[TaskManager] Скопировано {copied_count} чанков на следующий этап.")

def requeue_stalled_and_failed(all_step_paths: dict):
    for step_name, paths in all_step_paths.items():
        stalled_tasks = [os.path.join(paths["in_progress"], f) for f in os.listdir(paths["in_progress"])]
        if stalled_tasks:
            system_logger.info(f"[TaskManager] ({step_name}) Обнаружено {len(stalled_tasks)} 'зависших' задач. Возвращаем в очередь.")
            for task in stalled_tasks: move_task(task, paths["pending"])
        failed_tasks = [os.path.join(paths["failed"], f) for f in os.listdir(paths["failed"])]
        if failed_tasks:
            system_logger.info(f"[TaskManager] ({step_name}) Обнаружено {len(failed_tasks)} 'упавших' задач. Возвращаем в очередь.")
            for task in failed_tasks: move_task(task, paths["pending"])

def cleanup_workspace(workspace_paths: dict):
    base_dir = workspace_paths["base"]
    system_logger.info(f"Очистка рабочей директории: {base_dir}")
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)