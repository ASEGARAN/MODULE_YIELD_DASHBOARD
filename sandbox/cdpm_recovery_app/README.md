# SOCAMM2 cDPM Recovery Simulation Tool

Analyzes actual DPM vs projected recovery using Kevin Roos's hybrid DPM approach.

## Features

- **Hybrid DPM Calculation**: MODULE-level (MSNs/FIDs) vs FID-level (FIDs/FIDs)
- **Recovery Simulation**: Projects DPM reduction from three fix initiatives
- **Correlation Heatmaps**: MSN_STATUS x FAILCRAWLER breakdown
- **4-Week Cumulative**: Trend analysis over rolling 4-week period

## Recovery Types

| Fix | Status | Target | Description |
|-----|--------|--------|-------------|
| **New RPx Fix** | VERIFIED | False Miscompare | Signature detection via false_miscompare.py |
| **New BIOS Fix** | PROJECTED | MULTI_BANK_MULTI_DQ | Timing/speed fix - Ongoing CCE validation |
| **HW + SOP Fix** | PROJECTED | Hang | Debris/Speed + HUNG2 retest handling |

## Installation

### Option 1: MU App Store (Recommended)

Install from the MU App Store:
```bash
# Set SSL environment variable (required for Micron network)
export UV_NATIVE_TLS=1
export SSL_CERT_FILE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem

# Install the tool
appstore install cdpm-recovery-sim

# Run (after installation, the tool is available as a command)
cdpm-recovery-sim --did Y6CP --steps HMB1 --ww 202615
```

To upgrade to the latest version:
```bash
export UV_NATIVE_TLS=1
export SSL_CERT_FILE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
appstore install cdpm-recovery-sim --upgrade
```

### Option 2: Manual Installation

1. Download `cdpm_recovery_sim.py` to your bolpedh server
2. Ensure Python 3.6+ is available
3. Install dependencies:
   ```bash
   pip install pandas plotly
   ```
4. Run directly:
   ```bash
   python cdpm_recovery_sim.py --did Y6CP --steps HMB1 --ww 202615
   ```

## Usage

```bash
# Basic usage
cdpm-recovery-sim --did Y6CP --steps HMB1,QMON --ww 202615

# Multiple design IDs
cdpm-recovery-sim --did Y6CP,Y63N --steps HMB1,QMON --ww 202615

# Custom output file
cdpm-recovery-sim --did Y6CP --steps HMB1 --ww 202615 --output my_report.html

# Open browser after generation (opt-in)
cdpm-recovery-sim --did Y6CP --steps HMB1 --ww 202615 --browser
```

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--did` | Yes | Design ID(s), comma-separated (e.g., Y6CP,Y63N) |
| `--steps` | Yes | Test step(s), comma-separated (e.g., HMB1,QMON) |
| `--ww` | Yes | Work week in YYYYWW format (e.g., 202615) |
| `--output` | No | Output HTML file path (default: cdpm_recovery_WW{ww}.html) |
| `--browser` | No | Open browser after generating report |

## Output

- **Terminal**: Summary tables with DPM and recovery metrics
- **HTML Report**: Interactive report with:
  - MSN_STATUS x FAILCRAWLER correlation heatmaps
  - Single week and 4-week cumulative DPM breakdown
  - Recovery simulation with all fix types
  - Detailed MSN_STATUS and FAILCRAWLER breakdowns

## Example Output

```
======================================================================
SOCAMM2 cDPM Recovery Simulation
======================================================================
Design IDs: Y6CP
Steps: HMB1, QMON
Work Week: 202615
======================================================================

RECOVERY SUMMARY
======================================================================
Total DPM: 370.22

Fix Type          Recoverable       Rate
----------------------------------------
New RPx                  6.07         2%
New BIOS               103.17        28%
HW + SOP               121.38        33%
----------------------------------------
COMBINED               230.62        62%

After All Fixes: 139.60 DPM
```

## Dependencies

- Python 3.6+
- pandas
- plotly
- Access to mtsums command
- Access to /home/nmewes/Y6CP_FA/socamm_false_miscompare.py (for RPx verification)

## Cache

Results are cached in a `cache/` subdirectory to speed up repeated queries. Cache is valid for the same day only.

To clear cache:
```bash
rm -rf cache/
```

## Troubleshooting

### SSL Certificate Errors

If you see "invalid peer certificate: UnknownIssuer" errors, set these environment variables:
```bash
export UV_NATIVE_TLS=1
export SSL_CERT_FILE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
export REQUESTS_CA_BUNDLE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
```

### Command Not Found

If `cdpm-recovery-sim` is not found after installation, add `~/.local/bin` to your PATH:
```bash
export PATH="$HOME/.local/bin:$PATH"
```

Add this line to your `~/.bashrc` to make it permanent.

## Author

Manufacturing Engineering Team

## Version

1.0.2
