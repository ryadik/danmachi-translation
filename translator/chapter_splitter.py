import os
import re
import sys
from .logger import system_logger

def split_chapter_intelligently(chapter_file_path, output_dir, target_chars=3000, max_part_chars=5000):
    """
    Splits a chapter file into smaller parts and saves them directly into output_dir.
    """
    system_logger.info(f"Processing chapter file: {chapter_file_path}")
    
    os.makedirs(output_dir, exist_ok=True)

    with open(chapter_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    current_chunk_lines = []
    current_chunk_chars = 0
    chunk_num = 1

    def write_part():
        nonlocal current_chunk_lines, current_chunk_chars, chunk_num
        if not current_chunk_lines:
            return

        chunk_output_file = os.path.join(output_dir, f"chunk_{chunk_num}.txt")

        with open(chunk_output_file, 'w', encoding='utf-8') as out_f:
            out_f.writelines(current_chunk_lines)
        system_logger.info(f"  Saved chunk {chunk_num} to {chunk_output_file} ({current_chunk_chars} chars)")
        current_chunk_lines = []
        current_chunk_chars = 0
        chunk_num += 1

    def is_scene_marker(line):
        return re.match(r'^(\s*\[\]\s*|\s*---\s*)$', line)

    def is_dialogue_start(line):
        stripped_line = line.strip()
        return stripped_line.startswith('「') or stripped_line.startswith('『')

    def is_blank_line(line):
        return not line.strip()

    i = 0
    while i < len(lines):
        line = lines[i]
        current_chunk_lines.append(line)
        current_chunk_chars += len(line)

        if current_chunk_chars >= target_chars:
            best_break_index = -1
            
            for j in range(len(current_chunk_lines) - 1, -1, -1):
                current_line_in_buffer = current_chunk_lines[j]

                if is_scene_marker(current_line_in_buffer):
                    best_break_index = j
                    break

                if is_blank_line(current_line_in_buffer):
                    next_non_blank_line_is_dialogue = False
                    for k in range(j + 1, len(current_chunk_lines)):
                        if not is_blank_line(current_chunk_lines[k]):
                            if is_dialogue_start(current_chunk_lines[k]):
                                next_non_blank_line_is_dialogue = True
                            break
                    
                    if not next_non_blank_line_is_dialogue:
                        best_break_index = j
                        break
                
            if best_break_index != -1:
                temp_lines = current_chunk_lines[best_break_index + 1:]
                current_chunk_lines = current_chunk_lines[:best_break_index + 1]
                write_part()
                current_chunk_lines = temp_lines
                current_chunk_chars = sum(len(l) for l in current_chunk_lines)
            elif current_chunk_chars >= max_part_chars:
                force_break_index = -1
                for j in range(len(current_chunk_lines) - 1, -1, -1):
                    if is_blank_line(current_chunk_lines[j]):
                        force_break_index = j
                        break
                
                if force_break_index != -1:
                    temp_lines = current_chunk_lines[force_break_index + 1:]
                    current_chunk_lines = current_chunk_lines[:force_break_index + 1]
                    write_part()
                    current_chunk_lines = temp_lines
                    current_chunk_chars = sum(len(l) for l in current_chunk_lines)
                else:
                    write_part()

        i += 1

    write_part()