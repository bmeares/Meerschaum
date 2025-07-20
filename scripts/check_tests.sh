#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"

TEST_DIR="$PARENT/tests/"

if [ ! -d "$TEST_DIR" ]; then
    echo "Error: The directory '$TEST_DIR' was not found."
    exit 1
fi

# Create a temporary file to store all extracted function names along with their source file.
# This allows us to track where each function definition came from.
TEMP_FUNCTION_LIST=$(mktemp)

# Find all Python files (.py) in the TEST_DIR and its subdirectories.
# The -print0 and read -r -d $'\0' ensure correct handling of filenames with spaces or special characters.
find "$TEST_DIR" -name "*.py" -print0 | while IFS= read -r -d $'\0' file; do
    # Extract function names using grep and sed.
    # grep -oP 'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(' :
    #   -oP: Only print matching parts, and use Perl-compatible regex.
    #   'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(': Matches 'def ', then captures a valid Python function name,
    #                                         followed by any whitespace and an opening parenthesis.
    # sed -E 's/def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/\1/':
    #   -E: Use extended regex.
    #   This substitutes the entire matched string with just the captured group (the function name).
    grep -oP 'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(' "$file" | \
    sed -E 's/def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/\1/' | \
    while read -r func_name; do
        # Append the function name and its file path to the temporary list, separated by a colon.
        echo "$func_name:$file" >> "$TEMP_FUNCTION_LIST"
    done
done

# Extract only the function names from the temporary list, sort them, and find duplicates.
# cut -d':' -f1: Extracts the first field (function name) using ':' as a delimiter.
# sort: Sorts the function names alphabetically.
# uniq -d: Filters out unique lines, showing only lines that appeared more than once (duplicates).
DUPLICATE_FUNCTION_NAMES=$(cat "$TEMP_FUNCTION_LIST" | cut -d':' -f1 | sort | uniq -d)

# Check if any duplicate function names were found.
if [ -z "$DUPLICATE_FUNCTION_NAMES" ]; then
    echo "No duplicate function names found across the Python test files."
else
    echo "The following function names are defined in multiple files:"
    # Iterate through each unique duplicate function name.
    for func in $DUPLICATE_FUNCTION_NAMES; do
        echo "Function: '$func'"
        echo "  Found in:"
        # For each duplicate function, find all entries in the temporary list that match it.
        # Then, extract the filenames, sort them uniquely, and format the output.
        grep "^$func:" "$TEMP_FUNCTION_LIST" | cut -d':' -f2 | sort -u | sed 's/^/    - /'
        echo "" # Add a blank line for better readability between functions.
    done
fi

rm "$TEMP_FUNCTION_LIST"
