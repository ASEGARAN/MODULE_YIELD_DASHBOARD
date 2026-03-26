# Module Yield Dashboard

Weekly yield tracking dashboard for SOCAMM and SOCAMM2 modules at HMFN, HMB1, and QMON test steps.

## Features

- **Weekly Yield Trend Charts**: Track yield over time by test step and form factor
- **Bin Distribution Analysis**: View bin percentages by test step
- **Density/Speed Heatmap**: Visualize yield across density and speed combinations
- **Summary Table**: Detailed breakdown with CSV export

## Requirements

- Python 3.8 or higher

## Installation

```bash
# Navigate to project directory
cd MODULE_YIELD_DASHBOARD

# Install dependencies (use pip with Python 3.8+)
python3.11 -m pip install -r requirements.txt
# or
pip install -r requirements.txt
```

## Usage

```bash
# Run the dashboard
streamlit run app.py
```

The dashboard will open in your browser at `http://localhost:8501`.

## Configuration

### Sidebar Filters

| Filter | Description |
|--------|-------------|
| Module Form Factor | SOCAMM, SOCAMM2 |
| Test Step | HMFN, HMB1, QMON |
| Database | y6cp, y6ck, y6cn |
| Test Facility | PENANG, SUZHOU, XIAN |
| Work Week Range | Start/End year and week |
| Density | 48GB, 96GB, 192GB, 384GB (optional) |
| Speed | 7500MTPS, 9600MTPS, etc. (optional) |

### frpt Command

The dashboard executes frpt commands with the following template:

```
frpt -xf -bin=soft \
    -myquick=/MFG_WORKWEEK,DBASE,MODULE_FORM_FACTOR,MODULE_DENSITY,MODULE_SPEED/ \
    -quick=/MFG_WORKWEEK,myquick/ -sort=// +nowrap +echo +regwidth +module \
    -dbase={dbase} -step={step} -all -n -r +imesh \
    -nonshippable=N/A -eng_summary=N/A -standard_flow=yes \
    -machine_id=~SMT6 +# -module_form_factor={form_factor} \
    -mfg_workweek={workweek} -test_facility={facility} +% +debug
```

## Project Structure

```
MODULE_YIELD_DASHBOARD/
├── app.py                    # Main Streamlit application
├── requirements.txt          # Python dependencies
├── src/
│   ├── __init__.py
│   ├── frpt_runner.py       # Execute frpt commands
│   ├── frpt_parser.py       # Parse frpt output
│   └── data_processor.py    # Transform data for visualization
├── config/
│   ├── __init__.py
│   └── settings.py          # Configuration (default params, etc.)
└── README.md
```

## Dependencies

- streamlit >= 1.28.0
- pandas >= 2.0.0
- plotly >= 5.18.0
