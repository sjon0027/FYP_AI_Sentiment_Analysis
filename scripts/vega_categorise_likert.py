import json
import os
import sys

CATEGORY_MAP = {
"AI in law enforcement threatens personal privacy.": "privacy",
"AI in law enforcement reduces my trust because of how personal data is collected and used.": "data_misuse",
"AI in law enforcement reduces my trust because of increased monitoring (e.g., facial recognition, tracking).": "privacy",
"There is not enough accountability for how AI is used in law enforcement.": "accountability",
"If an AI system makes an incorrect or unfair decision, it should be easy for people to challenge or appeal that decision.": "accountability",
"AI decisions in policing should always be subject to human review.": "",
"I find it easy to access and understand information about how AI is being used by law enforcement in Australia.": "transparency",
"Information about how AI is used in law enforcement should be easily accessible to the public.": "transparency",
"The government should involve the public in decisions about how AI is used in law enforcement.": "",
"I believe AI could reduce human bias in policing compared to traditional methods.": "bias",
"AI decision-making can reinforce bias or discrimination.": "bias",
"AI increases the risk of misuse or abuse of power.": "",
"I believe there are adequate legal safeguards in Australia to regulate the use of AI in policing and security.": "accountability",
"I believe AI can make law enforcement more effective in preventing crime.": "",
"I would feel safer if AI technologies were widely used in national security in Australia.": "",
"I am more concerned about the use of AI by law enforcement than its use in other areas (e.g., healthcare, education, business).": "",
}

SENTIMENT_MAP = {
"AI in law enforcement threatens personal privacy.": "negative",
"AI in law enforcement reduces my trust because of how personal data is collected and used.": "negative",
"AI in law enforcement reduces my trust because of increased monitoring (e.g., facial recognition, tracking).": "negative",
"There is not enough accountability for how AI is used in law enforcement.": "negative",
"If an AI system makes an incorrect or unfair decision, it should be easy for people to challenge or appeal that decision.": "neutral",
"AI decisions in policing should always be subject to human review.": "neutral",
"I find it easy to access and understand information about how AI is being used by law enforcement in Australia.": "positive",
"Information about how AI is used in law enforcement should be easily accessible to the public.": "neutral",
"The government should involve the public in decisions about how AI is used in law enforcement.": "neutral",
"I believe AI could reduce human bias in policing compared to traditional methods.": "positive",
"AI decision-making can reinforce bias or discrimination.": "negative",
"AI increases the risk of misuse or abuse of power.": "negative",
"I believe there are adequate legal safeguards in Australia to regulate the use of AI in policing and security.": "positive",
"I believe AI can make law enforcement more effective in preventing crime.": "positive",
"I would feel safer if AI technologies were widely used in national security in Australia.": "positive",
"I am more concerned about the use of AI by law enforcement than its use in other areas (e.g., healthcare, education, business).": "neutral",
}

def add_fields(input_file, output_file):
    """Add 'sentiment' and 'category' fields to the dataset."""
    # Load input JSON
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    updated = []
    for entry in data:
        question = entry.get("question", "")        
        # Add sentiment
        sentiment = SENTIMENT_MAP.get(question, "other")
        
        # Add category based on question
        category = CATEGORY_MAP.get(question, "other")

        # Append new entry
        entry.update({
            "sentiment": sentiment,
            "category": category
        })
        updated.append(entry)

    # Write updated data
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(updated, f, indent=2, ensure_ascii=False)

    print(f"✅ Added sentiment & category fields to {len(updated)} entries.")
    print(f"💾 Output written to: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python add_sentiment_and_category.py <input_file.json> <output_file.json>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    add_fields(input_file, output_file)