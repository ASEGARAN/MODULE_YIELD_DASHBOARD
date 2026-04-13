#!/bin/bash
# Module Yield Dashboard startup script

cd /home/asegaran/MODULE_YIELD_DASHBOARD
source .venv/bin/activate
exec streamlit run app.py \
    --server.address 0.0.0.0 \
    --server.port 8502 \
    --server.headless true \
    --browser.gatherUsageStats false
