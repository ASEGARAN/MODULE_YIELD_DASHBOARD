# Module Yield Dashboard

Weekly yield tracking dashboard for SOCAMM and SOCAMM2 modules at HMFN, HMB1, and QMON test steps.

## Live Dashboard

**URL:** http://bolpedh02.micron.com:8501

Access the production dashboard from any machine on the Micron network.

## Features

- **Weekly Yield Trend Charts**: Track yield over time by test step and form factor
- **Bin Distribution Analysis**: View bin percentages by test step
- **Density/Speed Heatmap**: Visualize yield across density and speed combinations
- **Summary Table**: Detailed breakdown with CSV export
- **FAILCRAWLER DPM Analysis**: Live mtsums cDPM trending
- **SMT6 Tester Yield**: Machine and socket-level yield monitoring

## Quick Start

```bash
# Clone the repository
git clone https://github.com/ASEGARAN/MODULE_YIELD_DASHBOARD.git
cd MODULE_YIELD_DASHBOARD

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the dashboard
streamlit run app.py
```

The dashboard will open in your browser at `http://localhost:8501`.

## Requirements

- Python 3.8 or higher
- Access to Micron network (for frpt/mtsums commands)

## Installation (Detailed)

### 1. Clone the Repository

```bash
git clone https://github.com/ASEGARAN/MODULE_YIELD_DASHBOARD.git
cd MODULE_YIELD_DASHBOARD
```

### 2. Set Up Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the Dashboard

```bash
streamlit run app.py --server.port 8501
```

To make it accessible to others on the network:

```bash
streamlit run app.py --server.address 0.0.0.0 --server.port 8501
```

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
