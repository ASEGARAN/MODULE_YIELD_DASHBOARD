# Module Yield Dashboard - Architecture Document

**Version:** 1.0
**Last Updated:** 2026-04-28
**Author:** Abbiramavali Segaran
**For:** Vignesh (TM)

---

## 1. Overview

The Module Yield Dashboard is a **Streamlit-based web application** that provides real-time visibility into DRAM module manufacturing yield metrics, failure analysis, and test equipment health monitoring for the SOCAMM/SOCAMM2 product lines.

### Key Capabilities
- Weekly yield trend analysis by Design ID, density, and speed
- FAILCRAWLER-based failure categorization and DPM tracking
- FID-level fail visualization with address decoding
- SMT6 tester and GRACE motherboard health monitoring
- De-pop & Re-ball request tracking with HOLD/GO decisions

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           USER INTERFACE (Streamlit)                        │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │  Home   │ │  Yield  │ │Module   │ │ Pareto  │ │  Fail   │ │ Machine │   │
│  │         │ │Analysis │ │ELC Yield│ │Analysis │ │ Viewer  │ │ Trends  │   │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
│                                  │                                          │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                         De-pop & Re-ball Tab                          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          APPLICATION LAYER (Python)                         │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │DataProcessor │  │FailCrawler   │  │ FailViewer   │  │GraceMobo     │    │
│  │(yield calc)  │  │(DPM metrics) │  │(FID decode)  │  │(health mon)  │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │SMT6Yield     │  │SanityCheck   │  │DepopReball   │  │FiscalCalendar│    │
│  │(tester yield)│  │(RCA valid)   │  │(tracker)     │  │(WW mapping)  │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA ACCESS LAYER                                  │
│                                                                             │
│  ┌──────────────────────┐      ┌──────────────────────┐                    │
│  │    FrptRunner        │      │     FrptCache        │                    │
│  │ (frptx CLI wrapper)  │◄────►│ (24hr file cache)    │                    │
│  └──────────────────────┘      └──────────────────────┘                    │
│             │                                                               │
│             ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                    External CLI Tools                                 │  │
│  │   • frptx (yield/bin data)    • mtsums (FAILCRAWLER/DPM metrics)     │  │
│  │   • mfm (module history)      • tdat (ENG test details)              │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                       │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  iMESH DB    │  │ FAILCRAWLER  │  │   Excel      │  │  Geometry    │    │
│  │(module test  │  │   Database   │  │ (De-pop DOE) │  │  JSON Files  │    │
│  │   results)   │  │              │  │              │  │              │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Directory Structure

```
MODULE_YIELD_DASHBOARD/
├── app.py                      # Main Streamlit application (6400+ lines)
├── run_dashboard.sh            # Startup script for systemd
├── ARCHITECTURE.md             # This document
│
├── config/                     # Configuration
│   ├── settings.py             # App settings, CLI templates, defaults
│   ├── yield_targets.py        # D1 yield target definitions by DID/config
│   └── curve_history.py        # Historical yield curve data
│
├── src/                        # Core modules (~15,000 lines)
│   ├── data_processor.py       # Yield calculations, filtering, aggregation
│   ├── failcrawler.py          # FAILCRAWLER/DPM metrics (5500 lines)
│   ├── frpt_runner.py          # frptx CLI wrapper with validation
│   ├── frpt_parser.py          # Parse frpt output to DataFrame
│   ├── cache.py                # 24-hour file-based caching
│   ├── fiscal_calendar.py      # Workweek ↔ fiscal month mapping
│   ├── grace_motherboard.py    # GRACE/NVGRACE health monitoring
│   ├── smt6_yield.py           # SMT6 tester socket/site yield
│   ├── sanity_check.py         # RCA validation via dual-query
│   ├── ai_assistant.py         # Claude AI integration (optional)
│   ├── html_export.py          # HTML report generation
│   └── pdf_report.py           # PDF export functionality
│
├── src/depop_reball/           # De-pop & Re-ball Tracker
│   ├── tracker.py              # Data model, KPI calculations
│   ├── ui.py                   # Streamlit UI components
│   └── DOE_SOCAMM_SUMMARY.xlsx # Input data file
│
├── fail_viewer/                # FID-level Fail Visualization
│   ├── viewer.py               # Plotly-based fail viewer
│   ├── utils.py                # Data loading utilities
│   ├── repair.py               # Repair address decoding
│   ├── repair_loader.py        # Load repair data from tdat
│   ├── geometry/               # Bank geometry definitions
│   │   ├── y62p.py             # Y62P (SOCAMM) geometry
│   │   ├── y63n.py             # Y63N (SOCAMM2) geometry
│   │   └── y6cp.py             # Y6CP (SOCAMM2) geometry
│   ├── decoders/               # Address decoders by DID
│   │   ├── y42m/decoder.py
│   │   ├── y62p/decoder.py
│   │   ├── y63n/decoder.py
│   │   └── y6cp/decoder.py
│   └── validation/             # Decoder validation scripts
│
├── sandbox/                    # Experimental/dev scripts
│   └── cdpm_recovery_simulation/
│
└── scripts/                    # Utility scripts
    └── create_slt_yield_pptx.py
```

---

## 4. Main Tabs & Features

### Tab 1: Yield Analysis
- **Weekly yield trends** with target lines
- **Bin distribution** charts (Pass/Fail breakdown)
- **Density × Speed heatmaps**
- **Design ID breakdown** tables
- Filters: Form Factor, Step (HMFN/HMB1/QMON), Density, Speed, Workweek range

### Tab 2: Module ELC Yield
- **HMFN → HMB1 → QMON** yield cascade
- **Target vs Actual** comparison with D1 targets
- Configurable yield targets per DID/density/speed

### Tab 3: Pareto Analysis
| Subtab | Description |
|--------|-------------|
| **FAILCRAWLER DPM** | cDPM by failure category, WoW trends, Top Movers |
| **Register Fallout** | Top 5 registers by fallout % |
| **SLT FID Recovery** | Recovery projection (RPx, BIOS, HW+SOP) |
| **Sanity Check** | RCA validation via FAILCRAWLER × MSN_STATUS |

### Tab 4: Fail Viewer
- **FID address decoding** with bank/row/column visualization
- **Interactive Plotly charts** showing fail locations
- Support for Y42M, Y62P, Y63N, Y6CP geometries
- Repair data overlay

### Tab 5: Machine Trends
| Subtab | Description |
|--------|-------------|
| **SMT6 Tester Yield** | Socket & site yield analysis, trend classification |
| **GRACE Motherboard** | NVGRACE health monitoring, HANG SOP violations |

### Tab 6: De-pop & Re-ball
- **SOCAMM2 LPDRAMM** de-pop/reball request tracking
- **HOLD/GO decision** based on trailing 2-week success rate
- Target: 80% component functional after re-ball
- ULOC position analysis (ULOC1-ULOC4)

---

## 5. Data Flow

### 5.1 Yield Data Flow
```
User selects filters (DID, Step, Workweek, etc.)
           │
           ▼
    ┌──────────────┐
    │ FrptRunner   │──► Check FrptCache (24hr TTL)
    └──────────────┘           │
           │                   │ Cache hit?
           │                   │
           ▼                   ▼
    ┌──────────────┐    ┌──────────────┐
    │ Execute frptx│    │ Return cached │
    │ CLI command  │    │ DataFrame    │
    └──────────────┘    └──────────────┘
           │
           ▼
    ┌──────────────┐
    │ FrptParser   │──► Parse stdout to pandas DataFrame
    └──────────────┘
           │
           ▼
    ┌──────────────┐
    │DataProcessor │──► Filter, aggregate, calculate yield
    └──────────────┘
           │
           ▼
    ┌──────────────┐
    │ Plotly/      │──► Render charts & tables
    │ Streamlit    │
    └──────────────┘
```

### 5.2 FAILCRAWLER DPM Flow
```
User selects Step + DID + Workweeks
           │
           ▼
    ┌──────────────┐
    │ mtsums CLI   │──► +fidag +fc +fm -format=...
    └──────────────┘
           │
           ▼
    ┌──────────────┐
    │ Parse to     │──► FAILCRAWLER, MSN_STATUS, cDPM columns
    │ DataFrame    │
    └──────────────┘
           │
           ▼
    ┌────────────────────────────────────────┐
    │ Calculate metrics:                      │
    │ • cDPM = (Unique MSNs / Total FIDs) × 1M│
    │ • WoW change %                          │
    │ • Top Movers (≥25% AND ≥10 cDPM)       │
    │ • Recovery projections                  │
    └────────────────────────────────────────┘
           │
           ▼
    Render charts, tables, trend summary
```

---

## 6. Key External Dependencies

### 6.1 CLI Tools (Micron Internal)
| Tool | Purpose | Example |
|------|---------|---------|
| `frptx` | Yield/bin data from iMESH | `frptx -xf -dbase=Y6CP -step=hmb1 ...` |
| `mtsums` | FAILCRAWLER/DPM metrics | `mtsums +fidag +fc +fm -format=...` |
| `mfm` | Module history lookup | `mfm <MSN> -step=hmb1 +failcrawler` |
| `tdat` | ENG test details | `tdat <ENG_SUM>` |

### 6.2 Python Packages
```
streamlit>=1.28.0      # Web framework
pandas>=2.0.0          # Data manipulation
plotly>=5.18.0         # Interactive charts
numpy>=1.24.0          # Numerical operations
openpyxl>=3.1.0        # Excel file support
python-pptx>=0.6.21    # PowerPoint export
```

---

## 7. Deployment

### 7.1 Production URL
```
http://bolpedh03.micron.com:8502
```

### 7.2 Systemd Service
```bash
# Service file location
~/.config/systemd/user/module-yield-dashboard.service

# Common commands
systemctl --user status module-yield-dashboard   # Check status
systemctl --user restart module-yield-dashboard  # Restart
systemctl --user stop module-yield-dashboard     # Stop
journalctl --user -u module-yield-dashboard -f   # View logs

# Log file
tail -f ~/MODULE_YIELD_DASHBOARD/dashboard_service.log
```

### 7.3 Service Configuration
```ini
[Unit]
Description=Module Yield Dashboard (Streamlit)
After=network.target

[Service]
Type=simple
WorkingDirectory=/home/asegaran/MODULE_YIELD_DASHBOARD
ExecStart=/home/asegaran/MODULE_YIELD_DASHBOARD/run_dashboard.sh
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

---

## 8. Caching Strategy

| Cache Type | TTL | Location | Purpose |
|------------|-----|----------|---------|
| **FrptCache** | 24 hours | `~/.cache/module_yield_dashboard/` | frptx query results |
| **Streamlit session** | Session | Memory | User filter selections |
| **mtsums cache** | None (live) | N/A | Always fetches fresh DPM data |

---

## 9. Key Metrics Definitions

### 9.1 Yield Metrics
| Metric | Formula |
|--------|---------|
| **Yield %** | `(UPASS / UIN) × 100` |
| **Fallout %** | `100 - Yield %` |

### 9.2 DPM Metrics (FAILCRAWLER)
| Metric | Formula | Purpose |
|--------|---------|---------|
| **cDPM** | `(Unique Failing MSNs / Total FIDs) × 1,000,000` | Component-level fail rate |
| **MDPM** | `(Unique Failing MSNs / Total MUIDs) × 1,000,000` | Module-level fail rate |
| **WoW Change** | `((Current - Previous) / Previous) × 100` | Week-over-week trend |

### 9.3 De-pop & Re-ball
| Metric | Definition |
|--------|------------|
| **E2E Success** | Component functional after re-ball |
| **Target** | 80% success rate |
| **HOLD/GO** | Trailing 2-week rate vs 80% threshold |

---

## 10. Security Considerations

1. **Input Validation**: All CLI parameters validated via regex (`SAFE_PARAM_PATTERN`)
2. **No Shell Injection**: Commands built with `shlex.split()`, executed with `shell=False`
3. **No Secrets in Code**: Relies on Micron internal auth (Kerberos/LDAP)
4. **Network**: Internal network only (not exposed to internet)

---

## 11. Adding New Features

### To add a new tab:
1. Create module in `src/` (e.g., `src/new_feature.py`)
2. Add imports to `app.py`
3. Create `render_new_feature_tab()` function
4. Add tab to `st.tabs()` in main()
5. Call render function in the tab's `with` block

### To add a new FAILCRAWLER metric:
1. Add fetch function in `src/failcrawler.py`
2. Add calculation function
3. Add HTML/chart rendering function
4. Wire up in `render_failcrawler_subtab()`

---

## 12. Contact & Support

| Role | Contact |
|------|---------|
| **Owner** | Abbiramavali Segaran |
| **Dashboard URL** | http://bolpedh03.micron.com:8502 |
| **Code Location** | `/home/asegaran/MODULE_YIELD_DASHBOARD/` |

---

*Document generated for Module Yield Dashboard v1.0*
