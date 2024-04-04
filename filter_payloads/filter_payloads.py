import json

    

# Load JSON data from a file
with open('payload.json', 'r') as f:
    data = json.load(f)

with open("indicators.txt", "r") as file:
    # Read lines from the file
    lines = file.readlines()

indicators = []

for line in lines:
    indicators.append(line.strip())

errased_keys = []

for key, value in data['data'].items():
    if key not in indicators:
        errased_keys.append(key)

for key in errased_keys:
    data['data'].pop(key)

with open("filtered_payload.json", "w") as output_file:
    json.dump(data, output_file)