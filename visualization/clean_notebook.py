import json
from pathlib import Path

SRC = Path(r"D:\study folder\90DayML\visualization\DV0101EN-Exercise-Area-Plots-Histograms-and-Bar-Charts--v2.ipynb")
DST = SRC.with_name(SRC.stem + ".clean.ipynb")


def is_watermark_markdown(cell_src: list[str]) -> bool:
    joined = "".join(cell_src).lower()
    if "<img" not in joined:
        return False
    # Heuristics for CognitiveClass/IBM Skills Network logo in this lab
    keys = [
        "idsnlogo",  # file name in the URL
        "cognitiveclass",  # alt text and domain
        "ibmdeveloperskillsnetwork",  # bucket path
        "cf-courses-data.s3",
    ]
    return any(k in joined for k in keys)


def is_empty_code(cell_src: list[str]) -> bool:
    if not cell_src:
        return True
    # If lines are only whitespace/newlines
    return all((s.strip() == "") for s in cell_src)


def main():
    with SRC.open("r", encoding="utf-8") as f:
        nb = json.load(f)

    cells = nb.get("cells", [])
    new_cells = []

    for c in cells:
        ctype = c.get("cell_type")
        src = c.get("source", [])

        if ctype == "markdown":
            if is_watermark_markdown(src):
                # Skip this watermark cell
                continue
            new_cells.append(c)
        elif ctype == "code":
            if is_empty_code(src):
                c["source"] = ["pass\n"]
            new_cells.append(c)
        else:
            new_cells.append(c)

    nb["cells"] = new_cells

    with DST.open("w", encoding="utf-8") as f:
        json.dump(nb, f, ensure_ascii=False)

    print(f"Cleaned notebook written to: {DST}")


if __name__ == "__main__":
    main()
