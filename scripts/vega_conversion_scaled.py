import json
import os
import sys

def convert_survey_to_scatter(input_file: str, output_file):
    # Load the raw survey data
    with open(input_file, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Extract each list
    q1_list = raw_data.get(
        "Overall, how do you feel about the use of AI technologies in Australian law enforcement and national security?", []
    )
    q2_list = raw_data.get(
        "How familiar are you with how AI is currently used by Australian law enforcement?", []
    )
    q3_list = raw_data.get(
        "How much do you trust Australian law enforcement agencies to use AI technologies responsibly?", []
    )
    q4_list = raw_data.get(
        "In your own words how do you feel about the use of AI by law enforcement (police, security) in Australia?", []
    )

    # Check that all lists have the same length
    n = len(q1_list)
    if not all(len(lst) == n for lst in [q2_list, q3_list, q4_list]):
        print("❌ Error: All question lists must have the same length.")
        sys.exit(1)

    # Build output
    output = []
    for i in range(n):
        row = {
            "q1": q1_list[i],
            "q2": q2_list[i],
            "q3": q3_list[i],
            "q4": q4_list[i]
        }
        output.append(row)

    # Wrap in "values" for Vega-Lite
    vega_data = {"values": output}

    # Write to output file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(vega_data, f, indent=2, ensure_ascii=False)

    print(f"✅ Converted data saved to: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_to_scatter_data.py <input.json> <output.json>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    convert_survey_to_scatter(input_file, output_file)
