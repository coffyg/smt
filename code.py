import os

# List of Go source files to include
files = [
    "./task_priority.go",
    "./task.go",
    "./task_manager_simple.go",
    "./command_queue.go",
    "./task_command.go"
]

output_path = "code.md"

with open(output_path, "w", encoding="utf-8") as out:
    out.write("## Current Code\n\n")
    for filepath in files:
        filename = os.path.basename(filepath)
        out.write(f"### {filename}\n")
        out.write("```go\n")
        try:
            with open(filepath, "r", encoding="utf-8") as src:
                out.write(src.read())
        except FileNotFoundError:
            out.write(f"// ERROR: File not found: {filepath}\n")
        out.write("\n```\n\n")

print(f"Generated {output_path}")
