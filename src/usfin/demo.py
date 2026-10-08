"""`_demo`: a no-network job for exercising the runner (registered only when `USFIN_DEMO=1`).

Each key stores a small JSON body made up locally as a raw response (`demo://<key>/<date>`),
upserts its items into `demo_item` and advances the key's watermark to the newest day.
`USFIN_DEMO_FAIL=<key>` makes that key's fetch fail after its body is stored.
"""

import json
import os
from datetime import timedelta

from usfin import jobs, raw

DATASET = "_demo"


def fetch(ctx: "jobs.JobContext", key: str) -> list[int]:
    items = [
        {"day": (ctx.today - timedelta(days=n)).isoformat(), "value": float(len(key) * 10 + n)}
        for n in range(3)
    ]
    raw_id = raw.save(
        ctx.con,
        source="demo",
        dataset=DATASET,
        url=f"demo://{key}/{ctx.today.isoformat()}",
        body=json.dumps({"key": key, "items": items}).encode(),
        http_status=200,
        content_type="application/json",
        raw_dir=ctx.settings.raw_dir,
    )
    if os.environ.get("USFIN_DEMO_FAIL") == key:
        raise RuntimeError(f"USFIN_DEMO_FAIL={key}")
    return [raw_id]


def parse(ctx: "jobs.JobContext", raw_id: int) -> int:
    with raw.open(ctx.con, raw_id, raw_dir=ctx.settings.raw_dir) as fh:
        doc = json.loads(fh.read())
    ctx.con.execute(
        "CREATE TABLE IF NOT EXISTS demo_item (key VARCHAR, day DATE, value DOUBLE,"
        " PRIMARY KEY (key, day))"
    )
    for item in doc["items"]:
        ctx.con.execute(
            "INSERT INTO demo_item VALUES (?, ?, ?)"
            " ON CONFLICT (key, day) DO UPDATE SET value = excluded.value",
            [doc["key"], item["day"], item["value"]],
        )
    ctx.advance(doc["key"], max(item["day"] for item in doc["items"]))
    return len(doc["items"])


JOB = jobs.Job(name="_demo", keys=("alpha", "beta"), fetch=fetch, parsers={DATASET: parse})
