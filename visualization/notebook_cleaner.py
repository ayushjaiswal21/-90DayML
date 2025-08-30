#!/usr/bin/env python3
"""
Notebook cleaner and filler for visualization notebooks.
- Removes watermark/logo markdown cells (cognitiveclass/IBM logos)
- Fills placeholder code cells that say "### type your answer here"
- Clears all outputs and resets execution counts
- Writes a .bak backup before overwriting
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import List, Dict, Any

# Files to process
FILES = [
    Path(r"d:\study folder\90DayML\visualization\DV0101EN-Exercise-Generating-Maps-in-Python.ipynb"),
    Path(r"d:\study folder\90DayML\visualization\DV0101EN-Exercise-Waffle-Charts-Word-Clouds-and-Regression-Plots-v2.ipynb"),
]

WATERMARK_KEYWORDS = [
    "cognitiveclass.ai",  # alt text in logo image
    "logo.png",           # IBM Skills Network logo asset
    "IBMDeveloperSkillsNetwork",  # course hosting path
]

PLACEHOLDER_MARKER = "### type your answer here"

# Replacement snippets for the Generating-Maps notebook placeholders
MEXICO_BASIC = (
    "mexico_latitude = 23.6345\n"
    "mexico_longitude = -102.5528\n\n"
    "mexico_map = folium.Map(location=[mexico_latitude, mexico_longitude], zoom_start=4)\n\n"
    "mexico_map\n"
)

MEXICO_POSITRON = (
    "mexico_latitude = 23.6345\n"
    "mexico_longitude = -102.5528\n\n"
    "mexico_map = folium.Map(location=[mexico_latitude, mexico_longitude], zoom_start=6, tiles='Cartodb positron')\n\n"
    "mexico_map\n"
)


def remove_watermark_cells(cells: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove markdown cells that contain watermark/logo references."""
    cleaned: List[Dict[str, Any]] = []
    for cell in cells:
        if cell.get("cell_type") != "markdown":
            cleaned.append(cell)
            continue
        src_lines = cell.get("source", [])
        src_text = "".join(src_lines)
        if any(k in src_text for k in WATERMARK_KEYWORDS):
            # skip this cell
            continue
        cleaned.append(cell)
    return cleaned


def clear_outputs(cells: List[Dict[str, Any]]) -> None:
    """Clear outputs and reset execution_count on all code cells."""
    for cell in cells:
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None


def fill_placeholders(nb: Dict[str, Any], path: Path) -> None:
    """Replace placeholder code cells in known contexts.
    For the Generating Maps notebook, the first placeholder is Mexico zoom 4, the second is Mexico positron zoom 6.
    Other notebooks: leave placeholders unless we can confidently infer context.
    """
    fn = path.name
    if fn == "DV0101EN-Exercise-Generating-Maps-in-Python.ipynb":
        idx = 0
        for cell in nb.get("cells", []):
            if cell.get("cell_type") == "code" and PLACEHOLDER_MARKER in "".join(cell.get("source", [])):
                replacement = MEXICO_BASIC if idx == 0 else MEXICO_POSITRON
                cell["source"] = [replacement]
                idx += 1
    else:
        # For other notebooks, only remove watermarks and clear outputs for now
        pass


def load_nb(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_nb(path: Path, nb: Dict[str, Any]) -> None:
    backup = path.with_suffix(path.suffix + ".bak")
    if not backup.exists():
        backup.write_bytes(path.read_bytes())
    with path.open("w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False, indent=1)
        f.write("\n")


def process(path: Path) -> None:
    print(f"Processing: {path}")
    nb = load_nb(path)
    cells = nb.get("cells", [])
    # Step 1: remove watermark/logo cells
    cells = remove_watermark_cells(cells)
    # Step 2: clear outputs
    clear_outputs(cells)
    # Step 3: apply placeholder fills where known
    nb["cells"] = cells
    fill_placeholders(nb, path)
    # Save
    save_nb(path, nb)
    print(f"Done: {path}")


def main() -> None:
    for p in FILES:
        if not p.exists():
            print(f"Skipping missing file: {p}")
            continue
        process(p)


if __name__ == "__main__":
    main()
