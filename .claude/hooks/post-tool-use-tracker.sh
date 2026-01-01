#!/bin/bash
set -e

# Post-tool-use hook that tracks edited Rust files
# Runs after Edit, MultiEdit, or Write tools complete successfully

# Read tool information from stdin
tool_info=$(cat)

# Extract relevant data
tool_name=$(echo "$tool_info" | jq -r '.tool_name // empty')
file_path=$(echo "$tool_info" | jq -r '.tool_input.file_path // empty')
session_id=$(echo "$tool_info" | jq -r '.session_id // empty')

# Skip if not an edit tool or no file path
if [[ ! "$tool_name" =~ ^(Edit|MultiEdit|Write)$ ]] || [[ -z "$file_path" ]]; then
    exit 0
fi

# Skip markdown files
if [[ "$file_path" =~ \.(md|markdown)$ ]]; then
    exit 0
fi

# Create cache directory in project
cache_dir="$CLAUDE_PROJECT_DIR/.claude/cache/${session_id:-default}"
mkdir -p "$cache_dir"

# Detect file type and module
detect_module() {
    local file="$1"
    local project_root="$CLAUDE_PROJECT_DIR"

    # Remove project root from path
    local relative_path="${file#$project_root/}"

    # For Rust projects, detect based on src structure
    if [[ "$relative_path" =~ ^src/ ]]; then
        # Extract the module name (first directory under src or file name)
        local module=$(echo "$relative_path" | sed 's|^src/||' | cut -d'/' -f1 | sed 's|\.rs$||')
        echo "$module"
    else
        echo "root"
    fi
}

# Get cargo check command for Rust files
get_check_command() {
    local file="$1"
    local project_root="$CLAUDE_PROJECT_DIR"

    if [[ "$file" =~ \.rs$ ]] && [[ -f "$project_root/Cargo.toml" ]]; then
        echo "cargo check"
    fi
}

# Detect module
module=$(detect_module "$file_path")

# Log edited file
echo "$(date +%s):$file_path:$module" >> "$cache_dir/edited-files.log"

# Update affected modules list
if ! grep -q "^$module$" "$cache_dir/affected-modules.txt" 2>/dev/null; then
    echo "$module" >> "$cache_dir/affected-modules.txt"
fi

# Store check commands for Rust files
check_cmd=$(get_check_command "$file_path")
if [[ -n "$check_cmd" ]]; then
    echo "rust:check:$check_cmd" >> "$cache_dir/commands.txt.tmp"
    sort -u "$cache_dir/commands.txt.tmp" > "$cache_dir/commands.txt" 2>/dev/null || true
    rm -f "$cache_dir/commands.txt.tmp"
fi

exit 0
