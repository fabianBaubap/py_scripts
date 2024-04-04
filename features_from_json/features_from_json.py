import json

# Load JSON data from a file
with open('data.in', 'r') as f:
    data = json.load(f)

# Now 'data' contains the parsed JSON data
print(data)

def formatted_key(key):
    return f"{key}\n"

list = ""
for key, value in data.items():
#for key, value in data.items():
        list = list + formatted_key(key) 
with open("data.out", "w") as output_file:
    output_file.write(list)

