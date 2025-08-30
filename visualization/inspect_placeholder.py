import json
from pathlib import Path

p = Path(r'd:\study folder\90DayML\visualization\DV0101EN-Exercise-Waffle-Charts-Word-Clouds-and-Regression-Plots-v2.ipynb')
nb = json.loads(p.read_text(encoding='utf-8'))

cells = nb.get('cells', [])
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and any('type your answer here' in ''.join(cell.get('source', [])) for _ in [0]):
        print(f'Placeholder index: {i}')
        # previous markdown
        j = i - 1
        while j >= 0 and cells[j].get('cell_type') != 'markdown':
            j -= 1
        if j >= 0:
            print('--- Previous markdown ---')
            print(''.join(cells[j].get('source', [])))
        # next markdown
        k = i + 1
        while k < len(cells) and cells[k].get('cell_type') != 'markdown':
            k += 1
        if k < len(cells):
            print('--- Next markdown ---')
            print(''.join(cells[k].get('source', [])))
        break
