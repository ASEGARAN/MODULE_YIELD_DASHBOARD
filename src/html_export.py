"""
HTML Report Generator for Module Yield Dashboard.

Generates standalone shareable HTML reports with embedded charts and data.
"""

import io
import json
import os
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio


def create_shareable_html(
    title: str,
    filters: dict[str, Any],
    sections: list[dict],
    output_dir: str = "/tmp",
) -> str:
    """
    Generate a standalone HTML report that can be shared.

    Args:
        title: Report title
        filters: Current filter settings
        sections: List of section dicts with 'title', 'content' (html or plotly fig)
        output_dir: Directory to save the HTML file

    Returns:
        Path to the generated HTML file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"yield_report_{timestamp}.html"
    filepath = os.path.join(output_dir, filename)

    # Build filter summary
    filter_html = "<div class='filters'><h3>Report Filters</h3><ul>"
    if filters.get('workweeks'):
        wws = filters['workweeks']
        if isinstance(wws, list) and len(wws) > 1:
            filter_html += f"<li><b>Work Weeks:</b> {min(wws)} - {max(wws)}</li>"
        else:
            filter_html += f"<li><b>Work Week:</b> {wws}</li>"
    if filters.get('design_ids'):
        filter_html += f"<li><b>Design IDs:</b> {', '.join(filters['design_ids'])}</li>"
    if filters.get('form_factor'):
        filter_html += f"<li><b>Form Factor:</b> {filters['form_factor']}</li>"
    if filters.get('facility'):
        filter_html += f"<li><b>Facility:</b> {filters['facility']}</li>"
    filter_html += "</ul></div>"

    # Build sections HTML
    sections_html = ""
    plotly_scripts = []

    for i, section in enumerate(sections):
        section_title = section.get('title', f'Section {i+1}')
        content = section.get('content')
        section_type = section.get('type', 'html')

        sections_html += f"<div class='section'><h2>{section_title}</h2>"

        if section_type == 'plotly' and content is not None:
            # Convert Plotly figure to HTML div
            div_id = f"plotly-div-{i}"
            fig_json = pio.to_json(content)
            sections_html += f"<div id='{div_id}' class='plotly-chart'></div>"
            plotly_scripts.append(f"Plotly.newPlot('{div_id}', {fig_json});")
        elif section_type == 'table' and content is not None:
            # Convert DataFrame to HTML table
            if isinstance(content, pd.DataFrame):
                table_html = content.to_html(classes='data-table', index=False, escape=False)
                sections_html += table_html
            else:
                sections_html += str(content)
        elif section_type == 'html':
            sections_html += str(content) if content else "<p>No data available</p>"
        else:
            sections_html += "<p>No data available</p>"

        sections_html += "</div>"

    # Build complete HTML
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        :root {{
            --bg-primary: #0a0a1a;
            --bg-secondary: #1a1a2e;
            --bg-card: #16213e;
            --text-primary: #ffffff;
            --text-secondary: #a0a0a0;
            --accent-green: #00C853;
            --accent-red: #FF1744;
            --accent-yellow: #FFB300;
            --accent-blue: #00B0FF;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        header {{
            background: linear-gradient(135deg, var(--bg-secondary), var(--bg-card));
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 20px;
            border-left: 4px solid var(--accent-blue);
        }}
        header h1 {{ font-size: 28px; margin-bottom: 10px; }}
        header .timestamp {{ color: var(--text-secondary); font-size: 14px; }}
        .filters {{
            background: var(--bg-secondary);
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .filters h3 {{ margin-bottom: 10px; color: var(--accent-blue); }}
        .filters ul {{ list-style: none; display: flex; flex-wrap: wrap; gap: 15px; }}
        .filters li {{ background: var(--bg-card); padding: 8px 15px; border-radius: 6px; }}
        .section {{
            background: var(--bg-secondary);
            padding: 25px;
            border-radius: 12px;
            margin-bottom: 20px;
        }}
        .section h2 {{
            font-size: 20px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }}
        .plotly-chart {{ width: 100%; min-height: 400px; }}
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        .data-table th, .data-table td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }}
        .data-table th {{
            background: var(--bg-card);
            font-weight: 600;
        }}
        .data-table tr:hover {{ background: rgba(255,255,255,0.05); }}
        footer {{
            text-align: center;
            padding: 20px;
            color: var(--text-secondary);
            font-size: 12px;
        }}
        @media print {{
            body {{ background: white; color: black; }}
            .section {{ break-inside: avoid; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 {title}</h1>
            <div class="timestamp">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
        </header>

        {filter_html}

        {sections_html}

        <footer>
            Module Yield Dashboard Report | Generated automatically
        </footer>
    </div>

    <script>
        {chr(10).join(plotly_scripts)}
    </script>
</body>
</html>"""

    # Write to file
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)

    return filepath


def create_smt6_html_report(
    site_data: pd.DataFrame,
    machine_data: pd.DataFrame | None,
    filters: dict[str, Any],
    socket_grid_html: str,
    heatmap_fig: go.Figure | None,
    summary_html: str,
    output_dir: str = "/tmp",
) -> str:
    """
    Create an HTML report specifically for SMT6 Socket & Site Health.

    Args:
        site_data: Site-level yield data
        machine_data: Machine-level yield data
        filters: Current filter settings
        socket_grid_html: Pre-rendered socket health grid HTML
        heatmap_fig: Plotly heatmap figure
        summary_html: Pre-rendered site health summary HTML
        output_dir: Directory to save the HTML file

    Returns:
        Path to the generated HTML file
    """
    sections = []

    # Socket Health Grid
    if socket_grid_html:
        sections.append({
            'title': '🔌 Socket Health Grid',
            'content': socket_grid_html,
            'type': 'html'
        })

    # Site Yield Heatmap
    if heatmap_fig:
        sections.append({
            'title': '🗺️ Site Yield Heatmap',
            'content': heatmap_fig,
            'type': 'plotly'
        })

    # Site Health Summary
    if summary_html:
        sections.append({
            'title': '📊 Site Health Summary',
            'content': summary_html,
            'type': 'html'
        })

    # Machine Summary Table
    if machine_data is not None and not machine_data.empty:
        summary_df = machine_data.groupby('machine_id').agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        summary_df['yield_pct'] = (summary_df['upass_adj'] / summary_df['uin_adj'] * 100).round(2)
        summary_df.columns = ['Machine', 'UIN', 'UPASS', 'Yield %']
        summary_df['Machine'] = summary_df['Machine'].str.upper()

        sections.append({
            'title': '📋 Machine Summary',
            'content': summary_df,
            'type': 'table'
        })

    ww_info = ""
    if site_data is not None and not site_data.empty and 'workweek' in site_data.columns:
        wws = sorted(site_data['workweek'].unique())
        ww_info = f" (WW{wws[0]}-{wws[-1]})" if len(wws) > 1 else f" (WW{wws[0]})"

    return create_shareable_html(
        title=f"SMT6 Socket & Site Health Report{ww_info}",
        filters=filters,
        sections=sections,
        output_dir=output_dir
    )
