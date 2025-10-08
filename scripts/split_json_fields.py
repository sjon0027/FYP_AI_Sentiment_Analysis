import json
import os
import sys

def split_json_fields(input_path, output_dir):
    # Ensure output folder exists
    os.makedirs(output_dir, exist_ok=True)

    # Load input JSON (expecting a list of entries)
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Dictionary to collect all values for each field
    collected = {}

    for entry in data:
        for key, value in entry.items():
            if key not in collected:
                collected[key] = []
            collected[key].append(value)

    # Save each field to its own JSON file
    for field, values in collected.items():
        # Clean filename: replace unsafe characters
        safe_name = field.replace("?", "").replace(" ", "_").replace("/", "_")
        safe_name = safe_name.replace(":", "_").replace("*", "_")
        path = os.path.join(output_dir, f"{safe_name}.json")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(values, f, indent=2, ensure_ascii=False)

    print(f"✅ Split complete! Created {len(collected)} files in {output_dir}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python split_json_fields.py <input_json> <output_folder>")
        sys.exit(1)
    input_path, output_dir = sys.argv[1], sys.argv[2]
    split_json_fields(input_path, output_dir)
