import os
from datetime import datetime
from jinja2 import Template


DISCOVERY_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>ETL Validator — Discovery Report</title>
<style>
  body { font-family: Arial, sans-serif; font-size: 13px; margin: 20px; background: #f5f5f5; }
  h1 { color: #2c3e50; }
  h2 { color: #34495e; margin-top: 30px; border-bottom: 2px solid #ddd; padding-bottom: 6px; }
  h3 { color: #555; margin-top: 20px; }
  table { border-collapse: collapse; width: 100%; margin-top: 10px; background: white; }
  th { background-color: #2c3e50; color: white; padding: 8px; text-align: left; }
  td { padding: 6px 8px; border: 1px solid #ddd; }
  tr:nth-child(even) { background-color: #f9f9f9; }
  .HIGH    { background: #d4edda; color: #155724; font-weight: bold; padding: 2px 8px; border-radius: 4px; }
  .MEDIUM  { background: #fff3cd; color: #856404; font-weight: bold; padding: 2px 8px; border-radius: 4px; }
  .LOW     { background: #f8d7da; color: #721c24; font-weight: bold; padding: 2px 8px; border-radius: 4px; }
  .NONE    { background: #e2e3e5; color: #383d41; font-weight: bold; padding: 2px 8px; border-radius: 4px; }
  .AUTO         { background: #d4edda; color: #155724; font-weight: bold; padding: 2px 8px; border-radius: 4px; }
  .NEEDS_REVIEW { background: #fff3cd; color: #856404; font-weight: bold; padding: 2px 8px; border-radius: 4px; }
  .summary-box { display: inline-block; padding: 12px 20px; margin: 8px; border-radius: 6px; font-size: 14px; font-weight: bold; }
  .box-blue  { background: #d6eaf8; color: #1a5276; }
  .box-green { background: #d4edda; color: #155724; }
  .box-warn  { background: #fff3cd; color: #856404; }
  .box-red   { background: #f8d7da; color: #721c24; }
  .section { background: white; padding: 16px; margin-top: 16px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  .note { background: #e8f4fd; border-left: 4px solid #3498db; padding: 10px 14px; margin: 10px 0; border-radius: 0 4px 4px 0; }
</style>
</head>
<body>
<h1>ETL Validator — Discovery Report</h1>
<p><strong>Generated:</strong> {{ timestamp }} &nbsp;&nbsp;
   <strong>Source DB:</strong> {{ source_db }} &nbsp;&nbsp;
   <strong>Target DB:</strong> {{ target_db }}</p>

<div class="note">
  This is an <strong>auto-generated discovery report</strong>. Mappings marked
  <span class="NEEDS_REVIEW">NEEDS_REVIEW</span> require your confirmation before validation runs.
  Edit <code>output/generated_validation_config.yaml</code> to correct any mappings.
</div>

<h2>Summary</h2>
<div>
  <span class="summary-box box-blue">Source Tables: {{ summary.source_tables }}</span>
  <span class="summary-box box-blue">Target Tables: {{ summary.target_tables }}</span>
  <span class="summary-box box-green">High Confidence Matches: {{ summary.high }}</span>
  <span class="summary-box box-warn">Medium Confidence: {{ summary.medium }}</span>
  <span class="summary-box box-red">Needs Review: {{ summary.needs_review }}</span>
  <span class="summary-box box-red">No Match: {{ summary.no_match }}</span>
</div>

<h2>Table Mappings</h2>
<div class="section">
<table>
  <thead>
    <tr>
      <th>Source Table</th>
      <th>Target Table</th>
      <th>Confidence</th>
      <th>Match Reason</th>
      <th>Source Rows</th>
      <th>Target Rows</th>
      <th>Suggested Key</th>
      <th>Key Confidence</th>
      <th>Status</th>
    </tr>
  </thead>
  <tbody>
    {% for m in table_matches %}
    <tr>
      <td>{{ m.source_table }}</td>
      <td>{{ m.target_table or '—' }}</td>
      <td><span class="{{ m.confidence.upper() }}">{{ m.confidence }}</span></td>
      <td>{{ m.match_reason }}</td>
      <td>{{ m.source_rows }}</td>
      <td>{{ m.target_rows }}</td>
      <td>{{ m.suggested_key }}</td>
      <td><span class="{{ m.key_confidence.upper() }}">{{ m.key_confidence }}</span></td>
      <td><span class="{{ m.status }}">{{ m.status }}</span></td>
    </tr>
    {% endfor %}
  </tbody>
</table>
</div>

{% for tbl in column_details %}
<h2>Column Mappings — {{ tbl.source_table }} → {{ tbl.target_table }}</h2>
<div class="section">
<table>
  <thead>
    <tr>
      <th>Source Column</th>
      <th>Source Type</th>
      <th>Target Column</th>
      <th>Target Type</th>
      <th>Confidence</th>
      <th>Match Reason</th>
      <th>Status</th>
    </tr>
  </thead>
  <tbody>
    {% for col in tbl.columns %}
    <tr>
      <td>{{ col.source_col }}</td>
      <td>{{ col.source_type }}</td>
      <td>{{ col.target_col or '—' }}</td>
      <td>{{ col.target_type or '—' }}</td>
      <td><span class="{{ col.confidence.upper() }}">{{ col.confidence }}</span></td>
      <td>{{ col.match_reason }}</td>
      <td><span class="{{ col.status }}">{{ col.status }}</span></td>
    </tr>
    {% endfor %}
  </tbody>
</table>
</div>
{% endfor %}

<h2>Next Steps</h2>
<div class="section">
  <ol>
    <li>Review all <span class="NEEDS_REVIEW">NEEDS_REVIEW</span> mappings above.</li>
    <li>Open <code>output/generated_validation_config.yaml</code> and correct any wrong mappings.</li>
    <li>Set <code>source_where</code> for any tables that need a dbt/business filter.</li>
    <li>Run validation:<br><code>python -m src.main --config output/generated_validation_config.yaml</code></li>
  </ol>
</div>

</body>
</html>
"""


def generate_discovery_report(
    table_matches: list,
    source_metadata: dict,
    target_metadata: dict,
    key_suggestions: dict,
    column_matches: dict,
    source_db: str,
    target_db: str,
    output_path: str = "output/discovery_report.html",
):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    high = sum(1 for m in table_matches if m["confidence"] == "high")
    medium = sum(1 for m in table_matches if m["confidence"] == "medium")
    needs_review = sum(1 for m in table_matches if m["status"] == "NEEDS_REVIEW")
    no_match = sum(1 for m in table_matches if m["target_table"] is None)

    summary = {
        "source_tables": len(source_metadata),
        "target_tables": len(target_metadata),
        "high": high,
        "medium": medium,
        "needs_review": needs_review,
        "no_match": no_match,
    }

    enriched_matches = []
    for m in table_matches:
        src = m["source_table"]
        tgt = m["target_table"]
        key_info = key_suggestions.get(src, {})
        enriched_matches.append({
            **m,
            "source_rows": source_metadata.get(src, {}).get("row_count", "—"),
            "target_rows": target_metadata.get(tgt, {}).get("row_count", "—") if tgt else "—",
            "suggested_key": ", ".join(key_info.get("suggested_key", ["—"])),
            "key_confidence": key_info.get("confidence", "low"),
        })

    column_details = []
    for m in table_matches:
        src = m["source_table"]
        tgt = m["target_table"]
        if tgt is None:
            continue
        cols = column_matches.get(src, [])
        column_details.append({
            "source_table": src,
            "target_table": tgt,
            "columns": cols,
        })

    template = Template(DISCOVERY_HTML_TEMPLATE)
    html = template.render(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        source_db=source_db,
        target_db=target_db,
        summary=summary,
        table_matches=enriched_matches,
        column_details=column_details,
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return output_path
