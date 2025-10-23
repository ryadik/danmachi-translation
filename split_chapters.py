import os
import re
import sys

def split_chapter_intelligently(chapter_file_path, output_base_dir, target_chars=3000, max_part_chars=5000):
    """
    Splits a chapter file into smaller parts based on semantic breaks.
    """
    print(f"Processing chapter file: {chapter_file_path}")

    chapter_name = os.path.basename(os.path.dirname(chapter_file_path))
    parts_dir = os.path.join(output_base_dir, chapter_name, "parts")
    os.makedirs(parts_dir, exist_ok=True)

    with open(chapter_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    current_part_lines = []
    current_part_chars = 0
    part_num = 1

    def write_part():
        nonlocal current_part_lines, current_part_chars, part_num
        if not current_part_lines:
            return

        part_output_dir = os.path.join(parts_dir, str(part_num))
        os.makedirs(part_output_dir, exist_ok=True)
        part_output_file = os.path.join(part_output_dir, "jp.txt")

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

        # Check for split condition
        if current_part_chars >= target_chars:
            best_break_index = -1
            
            # Look for a good break point in the current accumulated lines
            # Iterate backwards to find the latest possible good break
            for j in range(len(current_part_lines) - 1, -1, -1):
                current_line_in_buffer = current_part_lines[j]

                # Priority 1: Scene markers
                if is_scene_marker(current_line_in_buffer):
                    best_break_index = j
                    break

                # Priority 2: Blank lines (not within dialogue)
                if is_blank_line(current_line_in_buffer):
                    # Check if the next non-blank line is not a dialogue start
                    next_non_blank_line_is_dialogue = False
                    for k in range(j + 1, len(current_part_lines)):
                        if not is_blank_line(current_part_lines[k]):
                            if is_dialogue_start(current_part_lines[k]):
                                next_non_blank_line_is_dialogue = True
                            break
                    
                    if not next_non_blank_line_is_dialogue:
                        best_break_index = j
                        break
                
                # Priority 3: End of paragraph (non-blank followed by blank)
                # This check is tricky with current_part_lines as it's a buffer.
                # A simpler approach for paragraph end is to split after a blank line.
                # The blank line check above covers this for non-dialogue cases.

            # If a good break point was found, split there
            if best_break_index != -1:
                temp_lines = current_part_lines[best_break_index + 1:]
                current_part_lines = current_part_lines[:best_break_index + 1]
                write_part()
                current_part_lines = temp_lines
                current_part_chars = sum(len(l) for l in current_part_lines)
            elif current_part_chars >= max_part_chars:
                # Force split at nearest paragraph end if no good break found and part is too large
                # Look for the last blank line to split after
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
                else: # No blank line found, force split at current line (last resort)
                    write_part()

        i += 1

    # Write any remaining lines
    write_part()

# --- Main execution logic ---
root_dir = "/Users/ryadik/personal/translations/danmachi_vol21/text/"
chapters_dir = os.path.join(root_dir, "chapters")

if not os.path.exists(chapters_dir):
    print(f"Error: '{chapters_dir}' not found. Please ensure chapter directories are under 'text/chapters/'.")
    sys.exit(1)

available_chapters = [d for d in os.listdir(chapters_dir) if os.path.isdir(os.path.join(chapters_dir, d))]
if not available_chapters:
    print(f"No chapter directories found in '{chapters_dir}'.")
    sys.exit(1)

print("Available chapters:")
for idx, chapter_name in enumerate(available_chapters):
    print(f"  {idx + 1}. {chapter_name}")

while True:
    try:
        choice = input("Enter the number or name of the chapter to split (e.g., '1' or 'prologue'): ").strip()
        if choice.isdigit():
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(available_chapters):
                selected_chapter_name = available_chapters[choice_idx]
                break
            else:
                print("Invalid number. Please try again.")
        elif choice in available_chapters:
            selected_chapter_name = choice
            break
        else:
            print("Invalid chapter name. Please try again.")
    except EOFError:
        print("\nInput cancelled. Exiting.")
        sys.exit(0)

selected_chapter_path = os.path.join(chapters_dir, selected_chapter_name, "jp.txt")

if not os.path.exists(selected_chapter_path):
    print(f"Error: '{selected_chapter_path}' not found. Please ensure 'jp.txt' exists in the selected chapter directory.")
    sys.exit(1)

print(f"\nSplitting chapter '{selected_chapter_name}'...")
split_chapter_intelligently(selected_chapter_path, chapters_dir)
print(f"Finished splitting chapter '{selected_chapter_name}'.")
