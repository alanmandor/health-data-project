"""Compare raw batch serialization overhead in isolated in-memory databases."""

import json
import time
import sys

import duckdb

columns = ("record_id", "file_id", "source_name", "row_number", "payload_json", "original_record_text", "ingested_at")
rows = [(str(i), "file", "steps", i, '{"steps":1}', None, "2026-01-12T08:00:00Z") for i in range(1000)]
for method in (sys.argv[1:] or ["struct", "json"]):
    with duckdb.connect() as db:
        db.execute("CREATE TABLE records (record_id VARCHAR,file_id VARCHAR,source_name VARCHAR,row_number BIGINT,payload_json JSON,original_record_text VARCHAR,ingested_at TIMESTAMPTZ)")
        started = time.monotonic()
        for _ in range(20):
            if method == "struct":
                db.execute("INSERT INTO records SELECT x.record_id,x.file_id,x.source_name,x.row_number,x.payload_json,x.original_record_text,x.ingested_at FROM unnest(?) t(x)", [[dict(zip(columns, row)) for row in rows]])
            else:
                expressions = ",".join(f"json_extract_string(value,'$[{i}]')" for i in range(7))
                db.execute(f"INSERT INTO records SELECT {expressions} FROM json_each(?)", [json.dumps(rows)])
        print(method, round(time.monotonic() - started, 3), flush=True)
