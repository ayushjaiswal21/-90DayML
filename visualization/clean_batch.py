import json
import hashlib
from pathlib import Path
from typing import List, Dict

VIZ_DIR = Path(r"D:\study folder\90DayML\visualization")

FILES = [
    VIZ_DIR / "DV0101EN-Exercise-Plotting-directly-with-Matplotlib-v2.ipynb",
    VIZ_DIR / "DV0101EN-Exercise-Pie-Charts-Box-Plots-Scatter-Plots-and-Bubble-Plots-v2.ipynb",
    VIZ_DIR / "DV0101EN-Exercise-Introduction-to-Matplotlib-and-Line-Plots--v2.ipynb",
    VIZ_DIR / "DV0101EN-Exercise-Dataset-Preprocessing-Exploring-with-Pandas.ipynb",
]


def is_watermark_markdown(cell_src: List[str]) -> bool:
    joined = "".join(cell_src).lower()
    if "<img" not in joined:
        return False
    keys = [
        "idsnlogo",
        "cognitiveclass",
        "ibmdeveloperskillsnetwork",
        "cf-courses-data.s3",
    ]
    return any(k in joined for k in keys)


def is_empty_code(cell_src: List[str]) -> bool:
    if not cell_src:
        return True
    return all((s.strip() == "") for s in cell_src)


def clean_notebook(src: Path) -> Path:
    if not src.exists():
        print(f"SKIP (missing): {src}")
        return src
    dst = src.with_name(src.stem + ".clean.ipynb")
    with src.open("r", encoding="utf-8") as f:
        nb = json.load(f)

    cells = nb.get("cells", [])
    new_cells = []
    removed = 0
    filled = 0

    for c in cells:
        ctype = c.get("cell_type")
        src_lines = c.get("source", []) or []
        if ctype == "markdown":
            if is_watermark_markdown(src_lines):
                removed += 1
                continue
            new_cells.append(c)
        elif ctype == "code":
            if is_empty_code(src_lines):
                c["source"] = ["pass\n"]
                filled += 1
            new_cells.append(c)
        else:
            new_cells.append(c)

    nb["cells"] = new_cells

    with dst.open("w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False)

    print(f"CLEANED: {src.name} -> {dst.name} (removed_watermark_cells={removed}, filled_empty_code_cells={filled})")
    return dst


def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def list_duplicates_in_dir(dir_path: Path) -> Dict[str, List[Path]]:
    groups: Dict[str, List[Path]] = {}
    for p in dir_path.glob("*.ipynb"):
        try:
            digest = sha256_of_file(p)
        except Exception as e:
            print(f"HASH FAIL: {p.name}: {e}")
            continue
        groups.setdefault(digest, []).append(p)
    # Filter to only duplicates
    return {k: v for k, v in groups.items() if len(v) > 1}


def main():
    # Clean the requested notebooks
    for f in FILES:
        clean_notebook(f)

    # Report duplicates in the visualization directory
    dups = list_duplicates_in_dir(VIZ_DIR)
    if not dups:
        print("No duplicate .ipynb files detected in visualization/ by SHA-256 hash.")
        return
    print("\nDuplicate .ipynb groups (identical content):")
    for digest, paths in dups.items():
        print(f"- HASH {digest[:12]}... ({len(paths)} files)")
        for p in sorted(paths):
            print(f"    {p.name}")

    print("\nIf you want, I can prepare a deletion list keeping one file per group.")


if __name__ == "__main__":
    main()
