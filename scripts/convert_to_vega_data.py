import json
import sys
import os
from collections import Counter

# Define the possible categories in order (for consistent output)
CATEGORIES = [
    "Strongly disagree",
    "Disagree",
    "Neither agree nor disagree",
    "Agree",
    "Strongly agree",
]

# Map lowercase variants and short forms to standardized categories
NORMALIZATION_MAP = {
    "strongly disagree": "Strongly disagree",
    "disagree": "Disagree",
    "neutral": "Neither agree nor disagree",
    "neither agree nor disagree": "Neither agree nor disagree",
    "agree": "Agree",
    "strongly agree": "Strongly agree",
}


def normalize_response(resp: str):
    """Convert response variants to one of the standardized categories."""
    r = resp.strip().lower()
    return NORMALIZATION_MAP.get(r, None)


def convert(input_file: str, output_folder: str):
    """Convert raw response data into Vega-Lite formatted JSON."""
    # Validate input file
    if not os.path.isfile(input_file):
        print(f"❌ Error: Input file not found: {input_file}")
        sys.exit(1)

    # Validate output folder
    os.makedirs(output_folder, exist_ok=True)

    # Determine output file name
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(output_folder, f"{base_name}_vega.json")

    # Read input JSON
    with open(input_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    output = []

    for question, responses in raw_data.items():
        normalized = [normalize_response(r) for r in responses if normalize_response(r)]
        total = len(normalized)
        counts = Counter(normalized)

        for category in CATEGORIES:
            value = counts.get(category, 0)
            percentage = round((value / total) * 100, 1) if total > 0 else 0
            output.append({
                "question": question,
                "type": category,
                "value": value,
                "percentage": percentage
            })

    # Write output JSON
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({"values": output}, f, indent=2, ensure_ascii=False)

    print(f"✅ Converted data written to: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_to_vega_data.py <input_file.json> <output_folder>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_folder = sys.argv[2]
    convert(input_file, output_folder)
