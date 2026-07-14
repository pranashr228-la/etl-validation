import os
import csv
from datetime import datetime
from jinja2 import Template


REPORT_COLUMNS = [
    "run_id", "run_timestamp", "table_name", "layer", "column_name",
    "key_value", "check_type", "source_result", "target_result",
    "status", "difference", "issue_type", "severity", "remarks",
    "source_query", "target_query",
]

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>ETL Validation Report</title>
<style>
  body { font-family: Arial, sans-serif; font-size: 13px; margin: 20px; }
  h1 { color: #2c3e50; }
  h2 { color: #34495e; margin-top: 30px; }
  table { border-collapse: collapse; width: 100%; margin-top: 10px; }
  th { background-color: #2c3e50; color: white; padding: 8px; text-align: left; }
  td { padding: 6px 8px; border: 1px solid #ddd; }
  tr:nth-child(even) { background-color: #f9f9f9; }
  .PASS { background-color: #d4edda; color: #155724; font-weight: bold; }
  .FAIL { background-color: #f8d7da; color: #721c24; font-weight: bold; }
  .WARNING { background-color: #fff3cd; color: #856404; font-weight: bold; }
  .summary-box { display: inline-block; padding: 12px 20px; margin: 8px;
                 border-radius: 6px; font-size: 14px; font-weight: bold; }
  .box-total { background: #d6eaf8; color: #1a5276; }
  .box-pass { background: #d4edda; color: #155724; }
  .box-fail { background: #f8d7da; color: #721c24; }
  .box-warn { background: #fff3cd; color: #856404; }
</style>
</head>
<body>
<h1>ETL Validation Report</h1>
<p><strong>Run ID:</strong> {{ run_id }} &nbsp;&nbsp;
   <strong>Timestamp:</strong> {{ run_timestamp }}</p>

<h2>Summary</h2>
<div>
  <span class="summary-box box-total">Tables Checked: {{ summary.total_tables_checked }}</span>
  <span class="summary-box box-pass">Tables Passed: {{ summary.tables_passed }}</span>
  <span class="summary-box box-fail">Tables Failed: {{ summary.tables_failed }}</span>
  <span class="summary-box box-total">Total Checks: {{ summary.total_checks }}</span>
  <span class="summary-box box-pass">Passed: {{ summary.passed_checks }}</span>
  <span class="summary-box box-fail">Failed: {{ summary.failed_checks }}</span>
  <span class="summary-box box-warn">Warnings: {{ summary.warning_checks }}</span>
</div>

<h2>Detailed Results</h2>
<table>
  <thead>
    <tr>
      {% for col in columns %}
      <th>{{ col.replace('_', ' ').title() }}</th>
      {% endfor %}
    </tr>
  </thead>
  <tbody>
    {% for row in rows %}
    <tr>
      {% for col in columns %}
      <td {% if col == 'status' %}class="{{ row[col] }}"{% endif %}>
        {{ row.get(col, '') }}
      </td>
      {% endfor %}
    </tr>
    {% endfor %}
  </tbody>
</table>
</body>
</html>
"""


def _compute_summary(results: list[dict]) -> dict:
    tables = set(r["table_name"] for r in results)
    total_checks = len(results)
    passed = sum(1 for r in results if r.get("status") == "PASS")
    failed = sum(1 for r in results if r.get("status") == "FAIL")
    warnings = sum(1 for r in results if r.get("status") == "WARNING")

    failed_tables = set(r["table_name"] for r in results if r.get("status") == "FAIL")
    passed_tables = tables - failed_tables

    return {
        "total_tables_checked": len(tables),
        "tables_passed": len(passed_tables),
        "tables_failed": len(failed_tables),
        "total_checks": total_checks,
        "passed_checks": passed,
        "failed_checks": failed,
        "warning_checks": warnings,
    }


def generate_report(results: list[dict], config: dict, run_id: str, run_timestamp: str):
    report_config = config.get("report", {})
    output_formats = report_config.get("output_format", ["html"])
    output_path = report_config.get("output_path", "./output/")

    os.makedirs(output_path, exist_ok=True)

    base_name = f"validation_report_{run_id}"


    if "html" in output_formats:
        html_path = os.path.join(output_path, f"{base_name}.html")
        summary = _compute_summary(results)
        template = Template(HTML_TEMPLATE)
        html_content = template.render(
            run_id=run_id,
            run_timestamp=run_timestamp,
            summary=summary,
            columns=REPORT_COLUMNS,
            rows=results,
        )
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"  HTML report written: {html_path}")

    return _compute_summary(results)
