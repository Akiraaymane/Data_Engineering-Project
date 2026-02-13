
import json

print("Checking JSONL:")
try:
    with open('../data/raw/reviews_raw.jsonl', 'r', encoding='utf-8') as f:
        line = f.readline()
        if line:
            data = json.loads(line)
            print("First review keys:", list(data.keys()))
            if 'appId' in data:
                print("YES, appId exists in JSONL.")
            else:
                print("NO, appId missing in JSONL.")
except FileNotFoundError:
    print("JSONL file not found.")

print("\nChecking JSON:")
try:
    with open('../data/raw/reviews_raw.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        if len(data) > 0:
            print("First review keys:", list(data[0].keys()))
            if 'appId' in data[0]:
                print("YES, appId exists in JSON.")
            else:
                print("NO, appId missing in JSON.")
except Exception as e:
    print(e)
