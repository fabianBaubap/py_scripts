import json

class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def encode(self, obj):
        if isinstance(obj, dict):
            formatted_dict = '\n'.join([f'"{key}": {json.dumps(value)}' for key, value in obj.items()])
            return '{\n' + formatted_dict + '\n}'
        return super().encode(obj)
    

# Load JSON data from a file
with open('medians.json', 'r') as f:
    data = json.load(f)

with open("indicators.txt", "r") as file:
    # Read lines from the file
    lines = file.readlines()

indicators = []

for line in lines:
    indicators.append(line.strip())

errased_keys = []

for key, value in data.items():
    if key not in indicators:
        errased_keys.append(key)

for key in errased_keys:
    data.pop(key)

with open("filtered_medians.json", "w") as output_file:
    json.dump(data, output_file, cls=CustomJSONEncoder)