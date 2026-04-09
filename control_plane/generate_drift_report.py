"""
Generate detailed schema drift attribute analysis report.

Two outputs:
  1. schema_drift_attribute_analysis.csv  — per-attribute change log across every
     consecutive day pair.  Columns: date, source, attribute, prev_type,
     curr_type, change_type  (SAME | FORMAT_CHANGE | TYPE_CHANGE | ADDED | REMOVED)
  2. schema_drift_line_graph.png — line graph: X = date, Y = attributes changed,
     one line per API source.  Shows drift spikes over time.
"""

import csv
import json
import os
import re
from collections import defaultdict

import psycopg2
from dotenv import load_dotenv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

REPORT_FILENAMES = (
    "schema_drift_attribute_analysis.csv",
    "schema_drift_line_graph.png",
    "schema_drift_interactive.html",
)


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def prepare_reports_output(reports_dir):
    """Keep report outputs in reports/latest and remove stale top-level artifacts."""
    latest_dir = os.path.join(reports_dir, "latest")
    os.makedirs(latest_dir, exist_ok=True)

    for name in REPORT_FILENAMES:
        latest_path = os.path.join(latest_dir, name)
        if os.path.exists(latest_path):
            os.remove(latest_path)

        top_level_path = os.path.join(reports_dir, name)
        if os.path.exists(top_level_path):
            os.remove(top_level_path)

    return latest_dir


def normalize_attribute_name(name):
    """Strip delimiters and lowercase — used to detect FORMAT_CHANGE."""
    return re.sub(r'[_\-.\[\]{}]', '', name).lower()


def extract_paths(obj, prefix="$"):
    """
    Recursively walk a parsed JSON object and return a dict:
      { "$.path.to.leaf:typename" : ("$.path.to.leaf", "typename") }
    """
    attributes = {}
    if isinstance(obj, dict):
        for key, val in obj.items():
            path = f"{prefix}.{key}"
            if isinstance(val, (dict, list)):
                attributes.update(extract_paths(val, path))
            else:
                if val is None:
                    t = "null"
                elif isinstance(val, bool):
                    t = "boolean"
                elif isinstance(val, int):
                    t = "integer"
                elif isinstance(val, float):
                    t = "number"
                else:
                    t = "string"
                attributes[f"{path}:{t}"] = (path, t)
    elif isinstance(obj, list):
        if obj:
            path = f"{prefix}[]"
            attributes.update(extract_paths(obj[0], path))
            attributes[f"{path}:array"] = (path, "array")
    return attributes


def get_source_attributes(conn, source, day):
    """Return (attribute dict, fetch_timestamp) for a source on a given date."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload, fetched_at FROM raw_api_events
            WHERE source = %s AND fetched_at::date = %s
            ORDER BY fetched_at ASC LIMIT 1
            """,
            (source, day),
        )
        row = cur.fetchone()
    if not row:
        return {}, None
    return extract_paths(row[0]), row[1]


def classify_change(key, prev_attrs, curr_attrs):
    """
    Given an attribute key (path:type), classify how it changed between two days.
    Returns (attribute_path, prev_type, curr_type, change_type)
    """
    in_prev = key in prev_attrs
    in_curr = key in curr_attrs

    if in_prev and in_curr:
        path, t = prev_attrs[key]
        return path, t, t, "SAME"

    if not in_prev and in_curr:
        path, t = curr_attrs[key]
        return path, "N/A", t, "ADDED"

    if in_prev and not in_curr:
        path, t = prev_attrs[key]
        # Check if attribute still exists but with a different type (TYPE_CHANGE)
        # or a different delimiter/casing pattern (FORMAT_CHANGE)
        prev_path, prev_type = prev_attrs[key]
        prev_norm = normalize_attribute_name(prev_path)
        for ck, (cp, ct) in curr_attrs.items():
            curr_norm = normalize_attribute_name(cp)
            if curr_norm == prev_norm:
                if ct != prev_type:
                    # Null transitions are value-availability changes, not structural drift
                    if prev_type == "null" or ct == "null":
                        return prev_path, prev_type, ct, "SAME"
                    return prev_path, prev_type, ct, "TYPE_CHANGE"
                return prev_path, prev_type, ct, "FORMAT_CHANGE"
        return path, t, "N/A", "REMOVED"

    return key, "N/A", "N/A", "SAME"


def get_all_days_for_source(conn, source):
    """Return sorted list of dates that have data for a source."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT fetched_at::date
            FROM raw_api_events
            WHERE source = %s
            ORDER BY 1
            """,
            (source,),
        )
        return [row[0] for row in cur.fetchall()]


def build_day_over_day_analysis(conn):
    """
    For each source, compare every consecutive day pair.
    Returns:
      csv_rows     — list of dicts for the CSV
      drift_by_day — {source: {date: {"changed": int, "total": int,
                                      "fetch_ts": datetime,
                                      "by_type": {change_type: count},
                                      "added": [...], "removed": [...],
                                      "type_changed": [...], "format_changed": [...]}}}
    """
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT source FROM raw_api_events ORDER BY source")
        sources = [r[0] for r in cur.fetchall()]

    csv_rows = []
    drift_by_day = {s: {} for s in sources}

    for source in sources:
        days = get_all_days_for_source(conn, source)
        if len(days) < 2:
            continue

        for i in range(1, len(days)):
            prev_day = days[i - 1]
            curr_day = days[i]

            prev_attrs, _         = get_source_attributes(conn, source, prev_day)
            curr_attrs, fetch_ts  = get_source_attributes(conn, source, curr_day)

            all_keys = set(prev_attrs) | set(curr_attrs)
            daily_changed = 0
            by_type = defaultdict(int)
            added = []
            removed = []
            type_changed = []
            format_changed = []

            for key in sorted(all_keys):
                attr_path, prev_type, curr_type, change_type = classify_change(
                    key, prev_attrs, curr_attrs
                )
                csv_rows.append({
                    "date": str(curr_day),
                    "source": source,
                    "attribute": attr_path,
                    "prev_type": prev_type,
                    "curr_type": curr_type,
                    "change_type": change_type,
                    "prev_day": str(prev_day),
                })
                by_type[change_type] += 1
                if change_type != "SAME":
                    daily_changed += 1

                if change_type == "ADDED":
                    added.append(attr_path)
                elif change_type == "REMOVED":
                    removed.append(attr_path)
                elif change_type == "TYPE_CHANGE":
                    type_changed.append(f"{attr_path} ({prev_type} → {curr_type})")
                elif change_type == "FORMAT_CHANGE":
                    format_changed.append(f"{attr_path} ({prev_type} → {curr_type})")

            drift_by_day[source][curr_day] = {
                "changed": daily_changed,
                "total":   len(curr_attrs),
                "fetch_ts": fetch_ts,
                "by_type": dict(by_type),
                "added": added,
                "removed": removed,
                "type_changed": type_changed,
                "format_changed": format_changed,
            }

    return csv_rows, drift_by_day


def write_csv(csv_rows, output_path):
    fieldnames = ["date", "source", "attribute", "prev_type", "curr_type",
                  "change_type", "prev_day"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)


def generate_line_graph(drift_by_day, output_path):
    """
    Static PNG line graph: X = date, Y = attributes changed vs previous day.
    One line per API source.
    """
    colors = {
        "amazon_products":       "#e74c3c",
        "openfoodfacts_snacks":  "#2ecc71",
        "product_search_details":"#3498db",
        "walmart_reviews":       "#f39c12",
    }
    markers = ["o", "s", "^", "D"]

    fig, ax = plt.subplots(figsize=(13, 6))

    has_data = False
    for idx, (source, day_data) in enumerate(sorted(drift_by_day.items())):
        if not day_data:
            continue
        dates = sorted(day_data.keys())
        counts = [day_data[d]["changed"] for d in dates]
        date_objs = [datetime.combine(d, datetime.min.time()) for d in dates]

        color = colors.get(source, f"C{idx}")
        marker = markers[idx % len(markers)]

        ax.plot(
            date_objs, counts,
            marker=marker, label=source,
            color=color, linewidth=2.5, markersize=8,
            markerfacecolor="white", markeredgewidth=2,
        )
        for dt, cnt in zip(date_objs, counts):
            if cnt > 0:
                ax.annotate(
                    str(cnt),
                    xy=(dt, cnt),
                    xytext=(0, 10),
                    textcoords="offset points",
                    ha="center", fontsize=9, color=color, fontweight="bold",
                )
        has_data = True

    if not has_data:
        ax.text(0.5, 0.5, "Not enough data — need ≥ 2 days per source",
                ha="center", va="center", transform=ax.transAxes, fontsize=13)

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    fig.autofmt_xdate(rotation=30, ha="right")

    ax.set_xlabel("Date", fontsize=12, fontweight="bold")
    ax.set_ylabel("Attributes Changed vs Previous Day", fontsize=12, fontweight="bold")
    ax.set_title(
        "Schema Drift Over Time — Attribute Changes per Day",
        fontsize=14, fontweight="bold",
    )
    ax.legend(loc="upper left", fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_ylim(bottom=0)

    fig.tight_layout()
    fig.savefig(output_path, dpi=100, bbox_inches="tight")
    print(f"Line graph saved to: {output_path}")


def generate_interactive_graph(drift_by_day, output_path):
    """
    Generate a polished standalone HTML graph and sync it to the UI copy.
    This restores the richer dashboard-style chart instead of the raw Plotly export.
    """
    palette = {
        "amazon_products": "#4fc3f7",
        "openfoodfacts_snacks": "#81c784",
        "product_search_details": "#ffb74d",
        "walmart_reviews": "#f06292",
    }

    symbols = ["circle", "square", "triangle-up", "diamond"]
    traces = []

    for idx, (source, day_data) in enumerate(sorted(drift_by_day.items())):
        if not day_data:
            continue

        dates = sorted(day_data.keys())
        counts = [day_data[d]["changed"] for d in dates]
        hover_texts = []

        for d in dates:
            meta = day_data[d]
            changed = meta["changed"]
            total = meta["total"]
            fetch_ts = meta["fetch_ts"]
            added = meta.get("added", [])
            removed = meta.get("removed", [])
            type_changed = meta.get("type_changed", [])
            format_changed = meta.get("format_changed", [])

            ts_str = fetch_ts.strftime("%Y-%m-%d %H:%M:%S UTC") if fetch_ts else "N/A"

            def section(title, items):
                if not items:
                    return f"<b>{title} (0)</b><br>None"
                return f"<b>{title} ({len(items)})</b><br>" + "<br>".join(
                    f"&bull; {item}" for item in items
                )

            if changed == 0:
                hover_html = (
                    f"<b>Source:</b> {source}<br>"
                    f"<b>Date:</b> {d}<br>"
                    f"<b>Fetched at:</b> {ts_str}<br>"
                    f"<b>Total attributes:</b> {total}<br>"
                    f"<b>Changed vs previous day:</b> 0<br>"
                    f"<b>No schema changes on this pull.</b>"
                )
            else:
                sections = [
                    f"<b>Source:</b> {source}<br>"
                    f"<b>Date:</b> {d}<br>"
                    f"<b>Fetched at:</b> {ts_str}<br>"
                    f"<b>Total attributes:</b> {total}<br>"
                    f"<b>Changed vs previous day:</b> {changed}",
                    section("Added", added),
                    section("Type Changed", type_changed),
                ]

                important_removed = len(removed) >= 3
                if important_removed:
                    sections.append(section("Removed", removed))

                if format_changed:
                    sections.append(section("Format Changed", format_changed))

                hover_html = "<br><br>".join(sections)

            hover_texts.append(hover_html)

        color = palette.get(source, f"hsl({idx * 90}, 70%, 55%)")
        symbol = symbols[idx % len(symbols)]
        traces.append(
            {
                "type": "scatter",
                "mode": "lines+markers+text",
                "name": source,
                "x": [str(d) for d in dates],
                "y": counts,
                "line": {"color": color, "width": 3.5},
                "marker": {
                    "size": 11,
                    "symbol": symbol,
                    "color": color,
                    "line": {"color": color, "width": 1.5},
                },
                "text": [str(c) if c > 0 else "" for c in counts],
                "textposition": "top center",
                "textfont": {"color": color, "size": 11},
                "hovertext": hover_texts,
                "hoverinfo": "text",
            }
        )

    layout = {
        "paper_bgcolor": "#151a22",
        "plot_bgcolor": "#151a22",
        "margin": {"l": 95, "r": 55, "t": 50, "b": 160},
        "hovermode": "closest",
        "font": {"color": "#e8edf6", "size": 14},
        "xaxis": {
            "title": {"text": "Pull Date", "standoff": 20},
            "type": "date",
            "tickformat": "%Y-%m-%d",
            "tickangle": -20,
            "automargin": True,
            "gridcolor": "rgba(140, 160, 190, 0.18)",
            "linecolor": "rgba(140, 160, 190, 0.24)",
        },
        "yaxis": {
            "title": {"text": "Attributes Changed", "standoff": 18},
            "dtick": 1,
            "rangemode": "tozero",
            "automargin": True,
            "tickmode": "linear",
            "gridcolor": "rgba(140, 160, 190, 0.18)",
            "linecolor": "rgba(140, 160, 190, 0.24)",
        },
        "legend": {
            "orientation": "h",
            "x": 0.5,
            "y": -0.32,
            "xanchor": "center",
            "yanchor": "top",
            "bgcolor": "rgba(0, 0, 0, 0)",
        },
    }

    config = {
        "responsive": True,
        "displaylogo": False,
        "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    }

    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Schema Drift Chart</title>
  <script src=\"https://cdn.plot.ly/plotly-3.4.0.min.js\"></script>
  <style>
    :root {{
      --bg: #0e1117;
      --panel: #151a22;
      --text: #e8edf6;
      --muted: #9aa4b2;
      --border: rgba(140, 160, 190, 0.24);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at 20% 0%, #1b2432 0%, #0e1117 45%, #0b0e13 100%);
      color: var(--text);
      font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }}
    .card {{
      width: min(1100px, 100%);
      background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0));
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: 0 16px 40px rgba(0, 0, 0, 0.35);
      overflow: hidden;
    }}
    .header {{ padding: 16px 20px 8px; border-bottom: 1px solid var(--border); }}
    .title {{ margin: 0; font-size: 1.05rem; font-weight: 700; letter-spacing: 0.3px; }}
    .subtitle {{ margin: 6px 0 0; color: var(--muted); font-size: 0.9rem; }}
    #chart {{ width: 100%; height: 780px; }}
  </style>
</head>
<body>
  <section class=\"card\">
    <header class=\"header\">
      <h1 class=\"title\">Schema Drift Over Time</h1>
      <p class=\"subtitle\">Restored interactive drift chart with the original dashboard styling.</p>
    </header>
    <div id=\"chart\"></div>
  </section>

  <script>
    const traces = {json.dumps(traces)};
    const layout = {json.dumps(layout)};
    const config = {json.dumps(config)};

    if (!traces.length) {{
      document.querySelector('.subtitle').textContent = 'Not enough data yet to show schema drift trends.';
    }}

    Plotly.newPlot('chart', traces, layout, config);
  </script>
</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    ui_chart_path = os.path.join(project_dir, "orchestrator", "ui", "schema_drift_chart.html")
    with open(ui_chart_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Interactive graph saved to: {output_path}")
    print(f"UI chart synced to: {ui_chart_path}")


def print_summary(drift_by_day, csv_rows):
    print("\nDay-over-day drift summary:")
    print("-" * 70)
    for source in sorted(drift_by_day):
        day_data = drift_by_day[source]
        if not day_data:
            print(f"\n{source}: only 1 day of data — no comparison possible")
            continue
        print(f"\n{source}:")
        for day in sorted(day_data):
            n = day_data[day]["changed"]
            status = "stable" if n == 0 else f"{n} attribute(s) changed"
            print(f"  {day}: {status}")

    change_type_counts = defaultdict(lambda: defaultdict(int))
    for row in csv_rows:
        change_type_counts[row["source"]][row["change_type"]] += 1

    print("\nCumulative change type breakdown:")
    print("-" * 70)
    for source in sorted(change_type_counts):
        counts = change_type_counts[source]
        total = sum(counts.values())
        print(f"\n{source} (total attribute-day observations: {total}):")
        for ct in ["SAME", "FORMAT_CHANGE", "TYPE_CHANGE", "ADDED", "REMOVED"]:
            n = counts.get(ct, 0)
            if n > 0:
                print(f"    {ct}: {n}")



def main():
    print("=" * 70)
    print("Schema Drift Attribute Analysis — Day-over-Day Report")
    print("=" * 70)

    conn = get_db_connection()
    try:
        print("\nBuilding day-over-day attribute comparison...")
        csv_rows, drift_by_day = build_day_over_day_analysis(conn)
    finally:
        conn.close()

    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    reports_dir = os.path.join(project_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    latest_dir = prepare_reports_output(reports_dir)

    csv_path = os.path.join(latest_dir, "schema_drift_attribute_analysis.csv")
    graph_path = os.path.join(latest_dir, "schema_drift_line_graph.png")
    interactive_path = os.path.join(latest_dir, "schema_drift_interactive.html")

    print(f"\nWriting CSV ({len(csv_rows)} rows)...")
    write_csv(csv_rows, csv_path)
    print(f"CSV saved to: {csv_path}")

    print("\nGenerating static line graph (PNG)...")
    generate_line_graph(drift_by_day, graph_path)

    print("\nGenerating interactive graph (HTML)...")
    generate_interactive_graph(drift_by_day, interactive_path)

    print_summary(drift_by_day, csv_rows)

    print("\n" + "=" * 70)
    print("Report generation complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
