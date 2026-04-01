"""FABLOT vs ROW CDPM Analysis for SOCAMM2 modules in WW202612."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from io import StringIO

# FABLOT ROW CDPM Data
cdpm_data = """FABLOT,Row
777663L,0
777939L,0
778955L,0
779235L,0
780030L,0
780286L,0
781235L,0
781237L,0
781526L,0
781756L,0
781758L,0
781998L,0
782233L,0
782235L,0
782237L,0
782246L,0
782504L,0
782505L,0
782632L,0
782635L,0
783297L,0
783300L,0
783544L,0
783545L,0
783546L,0
783801L,0
783804L,0
784079L,0
784080L,0
784082L,0
784083L,0
784084L,0
784328L,0
784329L,0
784450L,0
784451L,0
784453L,0
784566L,0
784711L,0
784712L,0
784819L,0
784821L,0
784822L,0
785076L,0
785078L,0
785321L,0
785322L,0
785324L,0
785569L,0
785571L,0
785572L,0
785573L,0
785820L,0
785821L,0
785822L,0
785823L,0
785824L,0
786079L,0
786080L,0
786081L,0
786380L,0
786381L,0
786637L,0
786639L,0
786640L,296.1
786641L,0
786924L,0
786925L,0
786926L,0
787333L,0
787334L,0
787337L,0
787584L,0
787588L,0
787741L,0
787742L,0
787743L,0
787744L,0
787745L,0
787999L,0
788000L,0
788264L,0
788265L,0
788266L,0
788505L,0
788506L,0
788507L,0
788537L,0
788538L,0
788539L,0
788540L,0
788541L,0
788791L,0
788793L,0
788794L,0
789061L,0
789062L,0
789063L,0
789264L,0
789265L,0
789266L,0
789267L,0
789268L,0
789513L,0
789521L,0
789522L,0
789523L,0
789524L,0
789525L,0
789774L,0
789776L,0
789778L,0
790018L,0
790021L,0
790023L,0
790024L,0
790025L,0
790026L,0
790027L,0
790302L,0
790304L,0
790305L,0
790306L,0
790309L,0
790310L,0
790311L,0
790616L,0
790617L,0
790619L,0
790620L,0
790913L,0
790914L,0
790915L,0
790916L,0
791151L,0
791152L,378.1
791153L,0
791155L,0
791383L,0
791384L,0
791385L,220
791387L,0
791647L,0
791648L,132.7
791891L,221.1
791893L,0
792127L,0
792128L,0
792381L,0
792907L,0
792910L,0
793186L,0
793440L,0
794210L,0
794212L,0
794503L,0
794995L,0
794997L,0
794999L,0
796326L,0
798364L,0
798923L,0
799176L,0
800116L,0
800117L,0
800118L,0
800454L,0
800464L,0
800468L,0
800717L,0
800718L,0
800719L,0
800720L,0
801012L,0
801301L,0
801600L,0
802662L,0
802888L,0
804006L,0
804017L,756.7
806702L,0
"""

# Failure Details Data
failure_data = """FID,SUMMARY,MSN,FID_STATUS2,MSN_STATUS,FAILCRAWLER,DRAMFAIL,ULOC,ADDRMASK,ADDRCNT,BITCNT,ROWCNT,COLCNT,DQCNT
786640L:08:P05:28,JAB/AY/SH/001NB~2,56BFF062,MB,Row,SINGLE_BURST_SINGLE_ROW,YES,U3D2,2C0DC0:::,5,5,1,2,4
791152L:09:P14:15,JAB/AX/MN/001NB~3,56BE3CB3,MB,Row,SINGLE_BURST_SINGLE_ROW,NO,U3D3,D10:::,4,4,1,2,3
791385L:17:P08:14,JAB/AX/MN/001NB~2,56BDFEA9,DB,Row,DB,YES,U2D3,C0810:::,2,2,1,2,2
791648L:13:P18:20,JAB/AY/2W/001NB~30,56C0D75E,MB,Row,SINGLE_BURST_SINGLE_ROW,NO,U1D2,200D60:::,5,5,1,2,4
791891L:12:N03:19,JAB/AX/MN/001NB~4,56BDED99,MB,Row,SINGLE_BURST_SINGLE_ROW,NO,U1D2,200D60:::,5,5,1,2,4
804017L:04:N17:13,JAB/AR/ZW/001NB~84,56A40286,TB,Row,SINGLE_BURST_SINGLE_ROW,NO,U2D3,100C40:::,3,3,1,2,2
804017L:13:N02:15,JAB/AR/ZW/001NB~125,56A41088,MB,Row,SINGLE_BURST_SINGLE_ROW,NO,U3D3,280D30:::,5,5,1,2,4
"""

# Parse data
df_cdpm = pd.read_csv(StringIO(cdpm_data))
df_fail = pd.read_csv(StringIO(failure_data))

# Extract FABLOT from FID (first 7 characters)
df_fail['FABLOT'] = df_fail['FID'].str.split(':').str[0]

print(f"Total FABLOTs: {len(df_cdpm)}")
print(f"FABLOTs with ROW > 0: {len(df_cdpm[df_cdpm['Row'] > 0])}")
print(f"Total Failure Records: {len(df_fail)}")

# Get non-zero CDPM data
non_zero_df = df_cdpm[df_cdpm['Row'] > 0].copy()

# Count failures per FABLOT
fail_counts = df_fail.groupby('FABLOT').size().reset_index(name='Fail_Count')

# Merge CDPM with failure counts
merged_df = non_zero_df.merge(fail_counts, on='FABLOT', how='left')
merged_df['Fail_Count'] = merged_df['Fail_Count'].fillna(0).astype(int)
merged_df = merged_df.sort_values('Row', ascending=False)

# Get failure details for hover
def get_failure_details(fablot):
    failures = df_fail[df_fail['FABLOT'] == fablot]
    if failures.empty:
        return "No failure details"
    details = []
    for _, row in failures.iterrows():
        details.append(
            f"FID: {row['FID']}<br>"
            f"MSN: {row['MSN']}<br>"
            f"FAILCRAWLER: {row['FAILCRAWLER']}<br>"
            f"ULOC: {row['ULOC']}<br>"
            f"DRAMFAIL: {row['DRAMFAIL']}"
        )
    return "<br>---<br>".join(details)

merged_df['Failure_Details'] = merged_df['FABLOT'].apply(get_failure_details)

print(f"\n--- Merged Data ---")
print(merged_df[['FABLOT', 'Row', 'Fail_Count']].to_string(index=False))

# Create visualization
fig = make_subplots(
    rows=3, cols=1,
    subplot_titles=(
        "FABLOT vs ROW CDPM (All Lots - WW202612)",
        "FABLOTs with Non-Zero ROW CDPM & Failure Counts",
        "Failure Breakdown by FABLOTs - FAILCRAWLER & DRAMFAIL"
    ),
    vertical_spacing=0.12,
    row_heights=[0.3, 0.35, 0.35],
    specs=[[{"secondary_y": False}], [{"secondary_y": True}], [{"secondary_y": False}]]
)

# Plot 1: All FABLOTs - bar chart
fig.add_trace(
    go.Bar(
        x=df_cdpm['FABLOT'],
        y=df_cdpm['Row'],
        marker_color=['red' if v > 0 else 'lightblue' for v in df_cdpm['Row']],
        name="ROW CDPM",
        hovertemplate="<b>FABLOT:</b> %{x}<br><b>ROW CDPM:</b> %{y}<extra></extra>",
        showlegend=False,
    ),
    row=1, col=1
)

# Plot 2: Non-zero FABLOTs with dual axis (CDPM + Fail Count)
# ROW CDPM bars
fig.add_trace(
    go.Bar(
        x=merged_df['FABLOT'],
        y=merged_df['Row'],
        name="ROW CDPM",
        marker_color='indianred',
        text=merged_df['Row'].apply(lambda x: f"{x:.1f}"),
        textposition='outside',
        customdata=merged_df[['Fail_Count', 'Failure_Details']].values,
        hovertemplate=(
            "<b>FABLOT:</b> %{x}<br>"
            "<b>ROW CDPM:</b> %{y:.1f}<br>"
            "<b>Fail Count:</b> %{customdata[0]}<br>"
            "<br><b>--- Failure Details ---</b><br>"
            "%{customdata[1]}"
            "<extra></extra>"
        ),
    ),
    row=2, col=1, secondary_y=False
)

# Failure count line
fig.add_trace(
    go.Scatter(
        x=merged_df['FABLOT'],
        y=merged_df['Fail_Count'],
        name="Fail Count",
        mode='lines+markers+text',
        marker=dict(size=12, color='blue', symbol='diamond'),
        line=dict(color='blue', width=2),
        text=merged_df['Fail_Count'],
        textposition='top center',
        textfont=dict(size=11, color='blue'),
    ),
    row=2, col=1, secondary_y=True
)

# Plot 3: Failure breakdown by FAILCRAWLER and DRAMFAIL
# Create combined category for visualization
df_fail['Category'] = df_fail['FAILCRAWLER'] + ' (DRAMFAIL=' + df_fail['DRAMFAIL'] + ')'

fail_by_category = df_fail.groupby(['FABLOT', 'Category']).size().unstack(fill_value=0)

# Define colors for each combination
colors = {
    'SINGLE_BURST_SINGLE_ROW (DRAMFAIL=YES)': 'darkorange',
    'SINGLE_BURST_SINGLE_ROW (DRAMFAIL=NO)': 'orange',
    'DB (DRAMFAIL=YES)': 'darkviolet',
    'DB (DRAMFAIL=NO)': 'violet',
}

sorted_fablots = merged_df['FABLOT'].tolist()

for category in fail_by_category.columns:
    y_vals = [fail_by_category.loc[f, category] if f in fail_by_category.index else 0 for f in sorted_fablots]

    fig.add_trace(
        go.Bar(
            x=sorted_fablots,
            y=y_vals,
            name=category,
            marker_color=colors.get(category, 'gray'),
            hovertemplate=f"<b>FABLOT:</b> %{{x}}<br><b>{category}:</b> %{{y}}<extra></extra>",
            legendgroup="failcrawler",
        ),
        row=3, col=1
    )

# Update layout
fig.update_layout(
    title=dict(
        text="SOCAMM2 FABLOT vs ROW CDPM Correlation Analysis (MFG_WW 202612)",
        font=dict(size=16)
    ),
    height=1100,
    barmode='stack',
    legend=dict(
        orientation="h",
        yanchor="top",
        y=-0.05,
        xanchor="center",
        x=0.5,
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor="gray",
        borderwidth=1,
    ),
    showlegend=True,
)

# Update axes
fig.update_xaxes(title_text="FABLOT", row=1, col=1, tickangle=45, tickfont=dict(size=6))
fig.update_xaxes(title_text="FABLOT", row=2, col=1)
fig.update_xaxes(title_text="FABLOT", row=3, col=1)

fig.update_yaxes(title_text="ROW CDPM", row=1, col=1)
fig.update_yaxes(title_text="ROW CDPM", row=2, col=1, secondary_y=False)
fig.update_yaxes(title_text="Fail Count", row=2, col=1, secondary_y=True, range=[0, 3])
fig.update_yaxes(title_text="Failure Count", row=3, col=1)

# Save to HTML
fig.write_html("/home/asegaran/MODULE_YIELD_DASHBOARD/fablot_row_chart.html")
print(f"\nChart saved to: /home/asegaran/MODULE_YIELD_DASHBOARD/fablot_row_chart.html")

# Create detailed failure table
print(f"\n--- Detailed Failure Table ---")
display_cols = ['FID', 'FABLOT', 'MSN', 'FAILCRAWLER', 'ULOC', 'DRAMFAIL', 'BITCNT', 'ROWCNT', 'COLCNT']
print(df_fail[display_cols].to_string(index=False))

# Summary statistics
print(f"\n--- Summary Statistics ---")
print(f"Total FABLOTs: {len(df_cdpm)}")
print(f"FABLOTs with ROW = 0: {len(df_cdpm[df_cdpm['Row'] == 0])}")
print(f"FABLOTs with ROW > 0: {len(df_cdpm[df_cdpm['Row'] > 0])}")
print(f"Total Failure Records: {len(df_fail)}")
print(f"Max ROW CDPM: {df_cdpm['Row'].max():.1f} (FABLOT: {df_cdpm.loc[df_cdpm['Row'].idxmax(), 'FABLOT']})")
print(f"FABLOT with most failures: 804017L (2 failures)")

# Correlation analysis
print(f"\n--- Correlation Analysis ---")
corr_df = merged_df[['Row', 'Fail_Count']].copy()
correlation = corr_df['Row'].corr(corr_df['Fail_Count'])
print(f"Correlation between ROW CDPM and Fail Count: {correlation:.3f}")
