import json

# Read the notebook file
with open('WebScraping-Review-Lab-v2.ipynb', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Remove all cell IDs (watermarks)
for cell in data['cells']:
    cell.pop('id', None)

# Remove prev_pub_hash from metadata if it exists
data['metadata'].pop('prev_pub_hash', None)

# Write the cleaned notebook
with open('WebScraping-Review-Lab-v2-clean.ipynb', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=1)

print("Cleaned notebook saved as WebScraping-Review-Lab-v2-clean.ipynb")
print("Removed all cell IDs and watermarks")
