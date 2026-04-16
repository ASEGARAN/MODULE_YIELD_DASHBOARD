"""
PDF Report Generator for Module Yield Dashboard.

Generates comprehensive PDF reports with charts, tables, and metrics.
"""

import io
import tempfile
from datetime import datetime
from typing import Any

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, HRFlowable
)


def create_dashboard_pdf(
    filters: dict[str, Any],
    yield_data: pd.DataFrame | None = None,
    elc_data: pd.DataFrame | None = None,
    pareto_data: pd.DataFrame | None = None,
    smt6_data: pd.DataFrame | None = None,
    grace_data: pd.DataFrame | None = None,
    charts: dict[str, bytes] | None = None,
) -> bytes:
    """
    Generate a comprehensive PDF report for the dashboard.

    Args:
        filters: Current filter settings (workweeks, design_ids, etc.)
        yield_data: Yield analysis data
        elc_data: Module ELC yield data
        pareto_data: Pareto analysis data
        smt6_data: SMT6 tester yield data
        grace_data: GRACE motherboard data
        charts: Dictionary of chart images as bytes (key: chart_name, value: PNG bytes)

    Returns:
        PDF file as bytes
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.5 * inch,
        leftMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch
    )

    # Styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=20,
        textColor=colors.HexColor('#1a1a2e'),
        alignment=1  # Center
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        spaceBefore=15,
        spaceAfter=10,
        textColor=colors.HexColor('#16213e')
    )
    subheading_style = ParagraphStyle(
        'CustomSubheading',
        parent=styles['Heading3'],
        fontSize=12,
        spaceBefore=10,
        spaceAfter=5,
        textColor=colors.HexColor('#333')
    )
    normal_style = styles['Normal']

    elements = []

    # =========================================================================
    # TITLE PAGE
    # =========================================================================
    elements.append(Spacer(1, 1.5 * inch))
    elements.append(Paragraph("Module Yield Dashboard Report", title_style))
    elements.append(Spacer(1, 0.3 * inch))

    # Report metadata
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    elements.append(Paragraph(f"<b>Generated:</b> {report_date}", normal_style))

    # Filter summary
    if filters:
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(Paragraph("<b>Report Filters:</b>", normal_style))

        filter_items = []
        if filters.get('workweeks'):
            ww_range = f"{min(filters['workweeks'])} - {max(filters['workweeks'])}"
            filter_items.append(f"Work Weeks: {ww_range}")
        if filters.get('design_ids'):
            filter_items.append(f"Design IDs: {', '.join(filters['design_ids'])}")
        if filters.get('facility'):
            filter_items.append(f"Facility: {filters['facility']}")
        if filters.get('density'):
            filter_items.append(f"Density: {filters['density']}")
        if filters.get('speed'):
            filter_items.append(f"Speed: {filters['speed']}")

        for item in filter_items:
            elements.append(Paragraph(f"  • {item}", normal_style))

    elements.append(PageBreak())

    # =========================================================================
    # YIELD ANALYSIS SECTION
    # =========================================================================
    if yield_data is not None and not yield_data.empty:
        elements.append(Paragraph("1. Yield Analysis", heading_style))
        elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

        # Summary stats
        summary_stats = [
            ["Metric", "Value"],
            ["Total Records", f"{len(yield_data):,}"],
            ["Work Weeks", f"{yield_data['workweek'].nunique()}"],
            ["Design IDs", f"{yield_data['design_id'].nunique() if 'design_id' in yield_data.columns else 'N/A'}"],
        ]

        # Check for yield column (could be 'yield_pct' or 'YIELD%')
        yield_col = 'yield_pct' if 'yield_pct' in yield_data.columns else 'YIELD%' if 'YIELD%' in yield_data.columns else None
        if yield_col:
            summary_stats.append(["Avg Yield", f"{yield_data[yield_col].mean():.2f}%"])
            summary_stats.append(["Min Yield", f"{yield_data[yield_col].min():.2f}%"])
            summary_stats.append(["Max Yield", f"{yield_data[yield_col].max():.2f}%"])

        summary_table = Table(summary_stats, colWidths=[2 * inch, 2 * inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a1a2e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('TOPPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        elements.append(summary_table)
        elements.append(Spacer(1, 0.3 * inch))

        # Add yield chart if provided
        if charts and 'yield_trend' in charts:
            elements.append(Paragraph("Yield Trend Chart", subheading_style))
            elements.append(_add_chart_image(charts['yield_trend']))

        elements.append(PageBreak())

    # =========================================================================
    # MODULE ELC YIELD SECTION
    # =========================================================================
    if elc_data is not None and not elc_data.empty:
        elements.append(Paragraph("2. Module ELC Yield", heading_style))
        elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

        # ELC summary by step (handle both uppercase and lowercase column names)
        if 'step' in elc_data.columns:
            uin_col = 'UIN' if 'UIN' in elc_data.columns else 'uin'
            upass_col = 'UPASS' if 'UPASS' in elc_data.columns else 'upass'

            step_summary = elc_data.groupby('step').agg({
                uin_col: 'sum',
                upass_col: 'sum'
            }).reset_index()
            step_summary['yield_pct'] = (step_summary[upass_col] / step_summary[uin_col] * 100).round(2)

            step_data = [["Step", "UIN", "UPASS", "Yield %"]]
            for _, row in step_summary.iterrows():
                step_data.append([
                    row['step'],
                    f"{int(row[uin_col]):,}",
                    f"{int(row[upass_col]):,}",
                    f"{row['yield_pct']:.2f}%"
                ])

            step_table = Table(step_data, colWidths=[1.5 * inch, 1.5 * inch, 1.5 * inch, 1.5 * inch])
            step_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a1a2e')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('TOPPADDING', (0, 0), (-1, 0), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ]))
            elements.append(step_table)

        # Add ELC chart if provided
        if charts and 'elc_trend' in charts:
            elements.append(Spacer(1, 0.3 * inch))
            elements.append(Paragraph("ELC Yield Trend", subheading_style))
            elements.append(_add_chart_image(charts['elc_trend']))

        elements.append(PageBreak())

    # =========================================================================
    # SMT6 TESTER YIELD SECTION
    # =========================================================================
    if smt6_data is not None and not smt6_data.empty:
        elements.append(Paragraph("3. SMT6 Tester Yield", heading_style))
        elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

        # Machine summary
        machine_summary = smt6_data.groupby('machine_id').agg({
            'uin_adj': 'sum',
            'upass_adj': 'sum'
        }).reset_index()
        machine_summary['yield_pct'] = (machine_summary['upass_adj'] / machine_summary['uin_adj'] * 100).round(2)
        machine_summary = machine_summary.sort_values('yield_pct', ascending=False)

        machine_data = [["Machine", "UIN", "UPASS", "Yield %"]]
        for _, row in machine_summary.iterrows():
            machine_data.append([
                row['machine_id'].upper(),
                f"{int(row['uin_adj']):,}",
                f"{int(row['upass_adj']):,}",
                f"{row['yield_pct']:.2f}%"
            ])

        machine_table = Table(machine_data, colWidths=[2 * inch, 1.5 * inch, 1.5 * inch, 1.5 * inch])
        machine_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a1a2e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('TOPPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f5')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        elements.append(machine_table)

        # Add SMT6 chart if provided
        if charts and 'smt6_trend' in charts:
            elements.append(Spacer(1, 0.3 * inch))
            elements.append(Paragraph("SMT6 Machine Yield Trend", subheading_style))
            elements.append(_add_chart_image(charts['smt6_trend']))

        elements.append(PageBreak())

    # =========================================================================
    # GRACE MOTHERBOARD SECTION
    # =========================================================================
    if grace_data is not None and not grace_data.empty:
        elements.append(Paragraph("4. GRACE Motherboard Health", heading_style))
        elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))

        # Hang summary
        if 'hang_cdpm' in grace_data.columns:
            hang_machines = grace_data[grace_data['hang_cdpm'] > 0]
            elements.append(Paragraph(
                f"Machines with Hang Issues: {len(hang_machines)} / {len(grace_data)}",
                normal_style
            ))

        # Add GRACE chart if provided
        if charts and 'grace_hang' in charts:
            elements.append(Spacer(1, 0.3 * inch))
            elements.append(Paragraph("Hang cDPM by Machine", subheading_style))
            elements.append(_add_chart_image(charts['grace_hang']))

    # =========================================================================
    # FOOTER
    # =========================================================================
    elements.append(Spacer(1, 0.5 * inch))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.grey))
    elements.append(Paragraph(
        f"<i>Report generated by Module Yield Dashboard - {report_date}</i>",
        ParagraphStyle('Footer', parent=normal_style, fontSize=8, textColor=colors.grey, alignment=1)
    ))

    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()


def _add_chart_image(chart_bytes: bytes, max_width: float = 7 * inch) -> Image:
    """Add a chart image to the PDF from bytes."""
    img_buffer = io.BytesIO(chart_bytes)
    img = Image(img_buffer)

    # Scale to fit page width while maintaining aspect ratio
    aspect = img.imageHeight / img.imageWidth
    img.drawWidth = max_width
    img.drawHeight = max_width * aspect

    # Cap height if too tall
    max_height = 4.5 * inch
    if img.drawHeight > max_height:
        img.drawHeight = max_height
        img.drawWidth = max_height / aspect

    return img


def export_chart_to_png(fig, width: int = 1200, height: int = 600) -> bytes:
    """
    Export a Plotly figure to PNG bytes.

    Args:
        fig: Plotly figure object
        width: Image width in pixels
        height: Image height in pixels

    Returns:
        PNG image as bytes
    """
    return fig.to_image(format="png", width=width, height=height, scale=2)
