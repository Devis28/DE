import json
from pathlib import Path

input_path = Path(r"silver_transform_merged0/silver_enrich.json")
output_path = Path(r"silver_transform_merged1/silver_enrich_durationsec.json")
output_path.parent.mkdir(parents=True, exist_ok=True)

with input_path.open("r", encoding="utf-8") as f:
    data = json.load(f)

for row in data:
    dur = row.get("duration")
    if isinstance(dur, (int, float)) and dur is not None:
        if dur == 0:
            continue
        # všetko nad 10 000 ber ako ms
        if dur > 10_000:
            row["duration"] = round(dur / 1000)

with output_path.open("w", encoding="utf-8") as f:
    json.dump(
        data,
        f,
        ensure_ascii=False,
        indent=2,
        separators=(",", ": ")
    )
