import os
import shutil
from .logger import logger

def setup_task_workspace(workspace_root: str, chapter_name: str):
    """
    Создает необходимую структуру директорий для обработки одной главы.
    Возвращает словарь с путями к созданным папкам.
    """
    chapter_dir = os.path.join(workspace_root, chapter_name)
    paths = {
        "base": chapter_dir,
        "pending": os.path.join(chapter_dir, "pending"),
        "in_progress": os.path.join(chapter_dir, "in_progress"),
        "terms": os.path.join(chapter_dir, "terms"),
        "done": os.path.join(chapter_dir, "done"),
        "failed": os.path.join(chapter_dir, "failed"),
    }
    
    for path in paths.values():
        os.makedirs(path, exist_ok=True)
        
    return paths

def get_pending_tasks(workspace_paths: dict) -> list:
    """Возвращает список путей к задачам в очереди 'pending'."""
    pending_dir = workspace_paths["pending"]
    return [os.path.join(pending_dir, f) for f in os.listdir(pending_dir) if os.path.isfile(os.path.join(pending_dir, f))]

def move_task(source_path: str, dest_dir: str) -> str:
    """
    Атомарно перемещает файл задачи в указанную директорию состояния.
    Возвращает новый путь к файлу.
    """
    if not os.path.exists(source_path):
        # Логируем ошибку, но также выбрасываем исключение, т.к. это серьезная проблема
        logger.error(f"Попытка переместить несуществующий файл: {source_path}")
        raise FileNotFoundError(f"Исходный файл задачи не найден: {source_path}")
    
    dest_path = os.path.join(dest_dir, os.path.basename(source_path))
    shutil.move(source_path, dest_path)
    return dest_path

def requeue_stalled_and_failed(workspace_paths: dict):
    """
    Собирает задачи из 'in_progress' и 'failed' и перемещает их в 'pending'.
    """
    stalled_tasks = [os.path.join(workspace_paths["in_progress"], f) for f in os.listdir(workspace_paths["in_progress"])]
    if stalled_tasks:
        logger.info(f"[TaskManager] Обнаружено {len(stalled_tasks)} 'зависших' задач. Возвращаем в очередь.")
        for task in stalled_tasks:
            move_task(task, workspace_paths["pending"])
            
    failed_tasks = [os.path.join(workspace_paths["failed"], f) for f in os.listdir(workspace_paths["failed"])]
    if failed_tasks:
        logger.info(f"[TaskManager] Обнаружено {len(failed_tasks)} 'упавших' задач. Возвращаем в очередь.")
        for task in failed_tasks:
            move_task(task, workspace_paths["pending"])

def requeue_completed_tasks(workspace_paths: dict):
    """
    Перемещает исходные чанки из 'done' обратно в 'pending' для следующего этапа.
    Игнорирует уже переведенные файлы.
    """
    done_dir = workspace_paths["done"]
    pending_dir = workspace_paths["pending"]
    
    tasks_in_done = [
        os.path.join(done_dir, f) 
        for f in os.listdir(done_dir) 
        if f.endswith(".txt") and not f.endswith("_translated.txt")
    ]
    
    if tasks_in_done:
        for task_path in tasks_in_done:
            move_task(task_path, pending_dir)
        logger.info(f"[TaskManager] Перемещено {len(tasks_in_done)} исходных чанков из 'done' в 'pending'.")

def cleanup_workspace(workspace_paths: dict):
    """Полностью удаляет рабочую директорию главы."""
    base_dir = workspace_paths["base"]
    logger.info(f"Очистка рабочей директории: {base_dir}")
    shutil.rmtree(base_dir)
