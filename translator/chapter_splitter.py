import os
import re
import sys

def split_chapter_intelligently(chapter_file_path, output_dir, target_chars=3000, max_part_chars=5000):
    """
    Splits a chapter file into smaller parts and saves them directly into output_dir.
    """
    print(f"Processing chapter file: {chapter_file_path}")
    
    os.makedirs(output_dir, exist_ok=True)

    with open(chapter_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    current_part_lines = []
    current_part_chars = 0
    part_num = 1

    def write_part():
        nonlocal current_part_lines, current_part_chars, part_num
        if not current_part_lines:
            return

        part_output_file = os.path.join(output_dir, f"part_{part_num}.txt")

        with open(part_output_file, 'w', encoding='utf-8') as out_f:
            out_f.writelines(current_part_lines)
        print(f"  Saved part {part_num} to {part_output_file} ({current_part_chars} chars)")
        current_part_lines = []
        current_part_chars = 0
        part_num += 1

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
        current_part_lines.append(line)
        current_part_chars += len(line)

        if current_part_chars >= target_chars:
            best_break_index = -1
            
            for j in range(len(current_part_lines) - 1, -1, -1):
                current_line_in_buffer = current_part_lines[j]

                if is_scene_marker(current_line_in_buffer):
                    best_break_index = j
                    break

                if is_blank_line(current_line_in_buffer):
                    next_non_blank_line_is_dialogue = False
                    for k in range(j + 1, len(current_part_lines)):
                        if not is_blank_line(current_part_lines[k]):
                            if is_dialogue_start(current_part_lines[k]):
                                next_non_blank_line_is_dialogue = True
                            break
                    
                    if not next_non_blank_line_is_dialogue:
                        best_break_index = j
                        break
                
            if best_break_index != -1:
                temp_lines = current_part_lines[best_break_index + 1:]
                current_part_lines = current_part_lines[:best_break_index + 1]
                write_part()
                current_part_lines = temp_lines
                current_part_chars = sum(len(l) for l in current_part_lines)
            elif current_part_chars >= max_part_chars:
                force_break_index = -1
                for j in range(len(current_part_lines) - 1, -1, -1):
                    if is_blank_line(current_part_lines[j]):
                        force_break_index = j
                        break
                
                if force_break_index != -1:
                    temp_lines = current_part_lines[force_break_index + 1:]
                    current_part_lines = current_part_lines[:force_break_index + 1]
                    write_part()
                    current_part_lines = temp_lines
                    current_part_chars = sum(len(l) for l in current_part_lines)
                else:
                    write_part()

        i += 1

    write_part()
