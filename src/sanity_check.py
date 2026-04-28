"""
Correlation Sanity Check Module
Validates RCA conclusions from MSN_STATUS × FAILCRAWLER analysis.

This module provides a validation layer that confirms the RCA conclusion,
not re-derives it. It shows evidence per dimension with consistency indicators.

Key Principle: "This tab exists to validate the RCA conclusion, not to re‑derive it."
"""

import subprocess
import pandas as pd
import logging
from io import StringIO
from typing import Optional

logger = logging.getLogger(__name__)

# =============================================================================
# MSN_STATUS × FAILCRAWLER Debug Flows
# =============================================================================
# Maps each MSN_STATUS × FAILCRAWLER combination to expected debug dimensions

DEBUG_FLOWS = {
    # =========================================================================
    # HANG failures - always check machine first (HW+SOP expected)
    # =========================================================================
    ('Hang', 'HANG'): {
        'rca_type': 'HW+SOP',
        'expected_recovery': 100,
        'primary_checks': ['MACHINE_ID', 'TESTER', 'SITE'],
        'secondary_checks': ['TEST_FACILITY', 'TEST_VERSION'],
        'description': 'Hardware/SOP issue - check machine clustering',
        'validation_criteria': {
            'machine_clustering': 'Strong signal if >50% on single machine',
            'site_clustering': 'Specific slot = socket/MOBO issue',
            'retest_pattern': 'Should improve after MOBO rotation per SOP',
        }
    },

    # =========================================================================
    # Mod-Sys failures - System-level, typically BIOS recoverable
    # =========================================================================
    ('Mod-Sys', 'MULTI_BANK_MULTI_DQ'): {
        'rca_type': 'BIOS',
        'expected_recovery': 100,
        'primary_checks': ['TEST_VERSION', 'FLOW', 'MACHINE_ID'],
        'secondary_checks': ['TESTER', 'SITE'],
        'description': 'System-level, BIOS-recoverable - check test config',
        'validation_criteria': {
            'config_correlation': 'Check if specific test version shows spike',
            'dq_spread': 'Multi-bank + multi-DQ = wide distribution expected',
        }
    },

    ('Mod-Sys', 'HGDC'): {
        'rca_type': 'BIOS',
        'expected_recovery': 100,
        'primary_checks': ['TEST_VERSION', 'MACHINE_ID', 'FLOW'],
        'secondary_checks': ['SITE', 'TESTER'],
        'description': 'HGDC (High Granularity Data Corruption) - system issue',
        'validation_criteria': {
            'test_version_spike': 'New test version may have issue',
            'machine_spread': 'Spread across machines = not HW issue',
        }
    },

    ('Mod-Sys', 'SINGLE_BURST_SINGLE_ROW'): {
        'rca_type': 'BIOS_PARTIAL',
        'expected_recovery': 50,
        'primary_checks': ['FABLOT', 'ULOC', 'WAFER'],
        'secondary_checks': ['MACHINE_ID', 'WREG'],
        'description': 'Mod-Sys with single-row pattern - may be DRAM',
        'validation_criteria': {
            'fablot_clustering': 'Strong fablot signal = DRAM issue',
            'wafer_clustering': 'Specific wafers = process issue',
        }
    },

    ('Mod-Sys', 'SYS_ODD_BURST_BIT'): {
        'rca_type': 'BIOS',
        'expected_recovery': 100,
        'primary_checks': ['MACHINE_ID', 'ULOC', 'TEST_VERSION'],
        'secondary_checks': ['SITE', 'FABLOT', 'BANK'],
        'description': 'Odd-burst timing margin failure - BIOS recoverable',
        'validation_criteria': {
            'hmfn_correlation': 'HMFN Pass → SLT Fail = stress-induced marginal defect',
            'uloc_clustering': 'U4 dominant (50%), U2 immunity (0%) = signal path length issue',
            'channel_mapping': 'U3/U4 = Channel 1 (longer trace paths)',
            'fablot_clustering': 'NO clustering expected (not silicon)',
            'bank_distribution': 'Random across all 16 banks = NOT array-specific',
            'thermal_pattern': 'HMB1 Pass → QMON Fail = thermal stress trigger',
        },
        'y6cp_topology': {
            'architecture': '16 banks in 2×8 layout, ASEL_BANK_SHIFT=17',
            'bank_decoding': 'bank = (row >> 17) & 0xF',
            'package_mapping': 'U1/U2=Channel 0, U3/U4=Channel 1',
            'die_stacking': '16 stacked dies per package (D0-D15)',
        },
        'analysis_findings': {
            'sample_size': '7 MSNs analyzed (WW16-18 data)',
            'uloc_distribution': {'U1': '29%', 'U2': '0%', 'U3': '21%', 'U4': '50%'},
            'bank_spread': 'Even distribution: Upper/Lower 46%/54%, Left/Right 51%/49%',
            'conclusion': 'Timing/signal integrity issue on odd burst cycles, NOT memory defect',
        },
        'key_learnings': [
            'Single DQ line affected per failure (not multi-bit)',
            'Random row addresses across failures',
            'U4 susceptibility = longest signal path from controller',
            'U2 immunity = shortest path, better signal integrity',
            'All 16 banks affected = NOT wordline/bitline defect',
            'BIOS timing margin adjustment should recover 100%',
        ],
    },

    # =========================================================================
    # DQ failures - could be BIOS or DRAM depending on pattern
    # =========================================================================
    ('DQ', 'SINGLE_BURST_SINGLE_ROW'): {
        'rca_type': 'BIOS_PARTIAL',
        'expected_recovery': 50,
        'primary_checks': ['FABLOT', 'ULOC', 'WAFER'],
        'secondary_checks': ['MACHINE_ID', 'PCB_SUPPLIER', 'WREG'],
        'description': 'Single-bit DQ issue - check die/fablot clustering',
        'validation_criteria': {
            'address_stability': 'Same DQ line across retests = true defect',
            'fablot_clustering': '>3 fails from same fablot = silicon issue',
            'wafer_region': 'Edge wafer region = process sensitivity',
        }
    },

    ('DQ', 'SYS_EVEN_BURST_BIT'): {
        'rca_type': 'BIOS_PARTIAL',
        'expected_recovery': 50,
        'primary_checks': ['MACHINE_ID', 'TEST_VERSION', 'SITE'],
        'secondary_checks': ['FABLOT', 'ULOC'],
        'description': 'Even-burst DQ pattern - may be test artifact',
        'validation_criteria': {
            'machine_correlation': 'Single machine = likely test issue',
            'pattern_consistency': 'Even bits across modules = system signature',
        }
    },

    ('DQ', 'SYS_ODD_BURST_BIT'): {
        'rca_type': 'BIOS_PARTIAL',
        'expected_recovery': 50,
        'primary_checks': ['MACHINE_ID', 'TEST_VERSION', 'SITE'],
        'secondary_checks': ['FABLOT', 'ULOC'],
        'description': 'Odd-burst DQ pattern - may be test artifact',
        'validation_criteria': {
            'machine_correlation': 'Single machine = likely test issue',
            'pattern_consistency': 'Odd bits across modules = system signature',
        }
    },

    ('DQ', 'MULTI_HALFBANK_SINGLE_DQ'): {
        'rca_type': 'BIOS_PARTIAL',
        'expected_recovery': 50,
        'primary_checks': ['FABLOT', 'ULOC', 'MACHINE_ID'],
        'secondary_checks': ['WAFER', 'WREG', 'PCB'],
        'description': 'Half-bank single DQ - localized issue',
        'validation_criteria': {
            'uloc_clustering': 'Same die position = solder/via issue',
            'fablot_spread': 'Spread across fablots = not silicon',
        }
    },

    # =========================================================================
    # Multi-DQ - typically BIOS recoverable
    # =========================================================================
    ('Multi-DQ', 'MULTI_BANK_MULTI_DQ'): {
        'rca_type': 'BIOS',
        'expected_recovery': 100,
        'primary_checks': ['TEST_VERSION', 'MACHINE_ID', 'FLOW'],
        'secondary_checks': ['SITE', 'TESTER'],
        'description': 'Multi-DQ multi-bank - classic BIOS issue',
        'validation_criteria': {
            'bank_distribution': 'Spread across banks = system-level',
            'dq_distribution': 'Random DQ pattern = not single-bit defect',
        }
    },

    # =========================================================================
    # Row failures - typically DRAM defect, not recoverable
    # =========================================================================
    ('Row', 'SINGLE_BURST_SINGLE_ROW'): {
        'rca_type': 'DRAM',
        'expected_recovery': 0,
        'primary_checks': ['FABLOT', 'WAFER', 'ULOC'],
        'secondary_checks': ['WREG', 'RETICLE_WAVE_ID', 'PROBE_REV'],
        'description': 'Row defect - check fablot/wafer clustering for DRAM issue',
        'validation_criteria': {
            'dramfail_flag': 'DRAMFAIL=YES confirms DRAM defect',
            'fablot_clustering': 'Multiple fails same fablot = fab issue',
            'wafer_clustering': 'Same wafer = wafer-level defect',
            'row_consistency': 'Same row address = true defect',
        }
    },

    ('Row', 'DB'): {
        'rca_type': 'DRAM',
        'expected_recovery': 0,
        'primary_checks': ['FABLOT', 'WAFER', 'ULOC'],
        'secondary_checks': ['WREG', 'BURN_MAJOR_VERSION'],
        'description': 'Double-bit row failure - DRAM defect confirmed',
        'validation_criteria': {
            'dramfail_flag': 'DRAMFAIL=YES expected',
            'bit_proximity': 'Adjacent bits in same row = cell defect',
        }
    },

    ('Row', 'ROW'): {
        'rca_type': 'DRAM',
        'expected_recovery': 0,
        'primary_checks': ['FABLOT', 'WAFER', 'ULOC'],
        'secondary_checks': ['WREG', 'RETICLE_WAVE_ID'],
        'description': 'Full row failure - DRAM defect',
        'validation_criteria': {
            'fablot_clustering': 'Same fablot = fab process issue',
            'reticle_wave': 'Same reticle wave = lithography issue',
        }
    },

    # =========================================================================
    # SB_Int - Single Bit Intermittent, DRAM issue
    # =========================================================================
    ('SB_Int', 'SB'): {
        'rca_type': 'DRAM',
        'expected_recovery': 0,
        'primary_checks': ['FABLOT', 'WAFER', 'ULOC'],
        'secondary_checks': ['WREG', 'VERIFIED'],
        'description': 'Intermittent single-bit - marginal DRAM',
        'validation_criteria': {
            'address_stability': 'Same bit across retests = true defect',
            'retest_pattern': 'Intermittent appearance = marginal cell',
            'verified_flag': 'VERIFIED=Y confirms repeated failure',
        }
    },

    # =========================================================================
    # Boot failures - comprehensive debug flow
    # =========================================================================
    ('Boot', 'BOOT'): {
        'rca_type': 'NO_FIX',
        'expected_recovery': 0,
        'primary_checks': ['MACHINE_ID', 'ASSEMBLY_FACILITY', 'PCB', 'ULOC'],
        'secondary_checks': ['PCB_SUPPLIER', 'PCB_ARTWORK_REV', 'SITE', 'FABLOT'],
        'description': 'Boot failure - check connectivity, signals, and die mapping',
        'validation_criteria': {
            'machine_correlation': 'Single machine = MOBO issue, rotate per SOP',
            'assembly_correlation': 'Supplier clustering = assembly/solder issue',
            'pcb_correlation': 'PCB rev = design issue',
            'uloc_clustering': 'Same ULOC = specific die/component issue',
        },
        'debug_steps': [
            '1. Check test chronology: mhist <MSN> -sbin',
            '2. Get failure details: frpt -xf <SUM> -mod_id=/<MSN>/ +# +module',
            '3. Get FAILCRAWLER/ULOC: mfm <MSN> -step=hmb1 +failcrawler',
            '4. Extract failing signals from SBIN/test log',
            '5. Map signals to die using subchannel prefix (LP1C → U4)',
            '6. Cross-reference with PCB schematic for pin locations',
        ],
        'signal_to_die_mapping': {
            'LP0A/LP0B': 'U1 (Channel 0, SC 0-1)',
            'LP0C/LP0D': 'U2 (Channel 0, SC 2-3)',
            'LP1A/LP1B': 'U3 (Channel 1, SC 4-5)',
            'LP1C/LP1D': 'U4 (Channel 1, SC 6-7)',
        },
        'failure_type_actions': {
            'CONT': 'Connectivity failure → De-pop/Reball for solder joint evaluation',
            'SPD': 'Serial Presence Detect → Check EEPROM/resistor network',
            'DQ': 'Data line failure → pFA request for IV curve + thermal imaging',
            'NO_BOOT': 'Module did not initialize → Check VI, run HMFN retest 5X',
        },
        'pcb_references': {
            'Y62P': 'PCB 3928 - 694 ball SOCAMM, U1-U4 = LPDDR0A-1D',
            'Y63N': 'PCB 3957 - 315 ball SOCAMM2, U1-U4 = LPDDR0A-0H',
            'Y6CP': 'PCB 3957 - 315 ball SOCAMM2, U1-U4 = LPDDR0A-0H',
        },
        'key_learnings': [
            'No Boot = No Summary (ENG failures do not generate mtsums records)',
            'Use mfm +failcrawler for detailed DQ/address-level analysis',
            'Multiple signals on same die = die/solder issue, not system',
            'HMFN Pass + HMB1 No Boot = stress-related marginal defect',
            'Gold plate contamination is common - always check VI',
        ],
    },

    # =========================================================================
    # Unknown / Other patterns
    # =========================================================================
    ('Mod-Sys', 'UNKNOWN'): {
        'rca_type': 'UNCLASSIFIED',
        'expected_recovery': 0,
        'primary_checks': ['MACHINE_ID', 'FABLOT', 'TEST_VERSION'],
        'secondary_checks': ['ASSEMBLY_FACILITY', 'PCB_SUPPLIER'],
        'description': 'Unknown pattern - needs investigation',
        'validation_criteria': {
            'check_all_dimensions': 'No clear pattern expected',
        }
    },
}

# Default flow for unknown combinations
DEFAULT_DEBUG_FLOW = {
    'rca_type': 'UNCLASSIFIED',
    'expected_recovery': 0,
    'primary_checks': ['MACHINE_ID', 'FABLOT', 'TEST_FACILITY'],
    'secondary_checks': ['ASSEMBLY_FACILITY', 'PCB_SUPPLIER'],
    'description': 'Unknown pattern - run full correlation check',
    'validation_criteria': {}
}

# =============================================================================
# No-Boot Failure Debug Guide
# =============================================================================
# Comprehensive methodology for debugging boot failures with PCB signal tracing

NO_BOOT_DEBUG_GUIDE = {
    # =========================================================================
    # Step 1: Extract Test Chronology
    # =========================================================================
    'step1_chronology': {
        'title': 'Extract Test Chronology',
        'commands': [
            'mhist <MSN1> <MSN2> -sbin                    # Module history with SBIN',
        ],
        'what_to_look_for': [
            'HMFN PASS → HMB1 FAIL pattern = stress-related marginal defect',
            'Intermittent failures (1/5 pass) = connectivity or timing issue',
            'SBIN 20 CONT = continuity failure, check solder joints',
            'SBIN 3 FUNC = functional failure, check signal integrity',
        ],
    },

    # =========================================================================
    # Step 2: Get Failure Details from frpt
    # =========================================================================
    'step2_frpt': {
        'title': 'Get Production Failure Details',
        'commands': [
            'frpt -xf <HMB1_SUMMARY> -mod_id=/<MSN>/ +# -quick=/summary,mod_id/ +module',
            'frpt -xf <HMFN_SUMMARY> -mod_id=/<MSN>/ +# +module    # If HMFN failed',
        ],
        'fields_to_extract': [
            'SBIN - Soft bin number indicates failure type',
            'FAILCRAWLER - Failure classification',
            'ULOC - Unit location (U1D0, U2D2, etc.)',
            'FID - Failing component ID for tracing',
            'SLOT - Socket slot position',
            'TESTER - Machine ID for correlation',
        ],
    },

    # =========================================================================
    # Step 3: Get FAILCRAWLER Details from mtsums/mfm
    # =========================================================================
    'step3_failcrawler': {
        'title': 'Get Detailed FAILCRAWLER Analysis',
        'commands': [
            'mfm <MSN1> <MSN2> -step=hmb1 +failcrawler    # RECOMMENDED - detailed per-address info',
            'mtsums <MSN> -step=hmb1 +quiet +csv -format=MSN,FID,ULOC,FAILCRAWLER,FID_STATUS,DRAMFAIL,DQCNT',
        ],
        'mfm_advantages': [
            'ULOC + exact DQ line (not just aggregate)',
            'Per-address detail (ROW, COL, DQ)',
            'Failed test patterns for signal analysis',
            'Can identify specific signal to probe',
        ],
        'key_fields': {
            'FID_STATUS': 'DQ/Row/Boot/Hang - failure classification',
            'DRAMFAIL': 'YES = DRAM defect, NO = system classification',
            'DQCNT': 'Number of DQ lines failing',
            'FAILCRAWLER': 'SYS_EVEN_BURST_BIT, MULTI_HALFBANK_SINGLE_DQ, etc.',
        },
    },

    # =========================================================================
    # Step 4: Map Signals to Die (PCB Signal Tracing)
    # =========================================================================
    'step4_signal_mapping': {
        'title': 'Map Failing Signals to Die Location',
        'signal_prefix_to_die': {
            'LPDDR0A / LP0A': {'die': 'U1', 'channel': 0, 'subchannel': '0,1'},
            'LPDDR0B / LP0B': {'die': 'U1', 'channel': 0, 'subchannel': '0,1'},
            'LPDDR0C / LP0C': {'die': 'U2', 'channel': 0, 'subchannel': '2,3'},
            'LPDDR0D / LP0D': {'die': 'U2', 'channel': 0, 'subchannel': '2,3'},
            'LPDDR1A / LP1A': {'die': 'U3', 'channel': 1, 'subchannel': '4,5'},
            'LPDDR1B / LP1B': {'die': 'U3', 'channel': 1, 'subchannel': '4,5'},
            'LPDDR1C / LP1C': {'die': 'U4', 'channel': 1, 'subchannel': '6,7'},
            'LPDDR1D / LP1D': {'die': 'U4', 'channel': 1, 'subchannel': '6,7'},
        },
        'signal_translation': {
            'LP1C_WCLK0_N': 'LPDDR1C_WCK0_N (Write Clock)',
            'LP1C_CA3': 'LPDDR1C_CA[3] (Command/Address)',
            'LP0C_DQ6': 'LPDDR0C_DQ0[6] (Data bit 6)',
        },
        'signals_per_subchannel': [
            'CLK_P/N - Clock pair',
            'CA[6:0] - Command/Address (7 bits)',
            'CS[3:0] - Chip Select (4 bits)',
            'DMI0, DMI1 - Data Mask Inversion',
            'DQ0[7:0], DQ1[7:0] - Data bytes',
            'DQS0_P/N, DQS1_P/N - Data Strobe',
            'WCK0_P/N, WCK1_P/N - Write Clock',
        ],
    },

    # =========================================================================
    # Step 5: Cross-Reference with PCB Schematic
    # =========================================================================
    'step5_pcb_reference': {
        'title': 'Cross-Reference PCB Schematic',
        'pcb_documents': {
            'Y62P_SOCAMM': {
                'pcb': 'PCB 3928.03',
                'description': 'LPDDR5 SOCAMM - 694 Pin Socket, 16-layer',
                'dies': 'U1 (SC 0,1), U2 (SC 2,3), U3 (SC 4,5), U4 (SC 6,7)',
                'signal_prefix': 'LPDDR0A/0B/0C/0D, LPDDR1A/1B/1C/1D',
            },
            'Y63N_SOCAMM2': {
                'pcb': 'PCB 3957.02',
                'description': 'LPDDR5 315 BALL SOCAMM2 - 16-layer, Rev B',
                'dies': 'U1 (SC 0,1), U2 (SC 2,3), U3 (SC 4,5), U4 (SC 6,7)',
                'signal_prefix': 'LPDDR0A-0H (U1-U4)',
            },
            'Y6CP_SOCAMM2': {
                'pcb': 'PCB 3957.02',
                'description': 'LPDDR5 315 BALL SOCAMM2 - 16-layer, Rev B',
                'dies': 'U1 (SC 0,1), U2 (SC 2,3), U3 (SC 4,5), U4 (SC 6,7)',
                'signal_prefix': 'LPDDR0A-0H (U1-U4)',
            },
        },
        'example_pin_mapping_u4': {
            'CA[0]': 'F16', 'CA[1]': 'F18', 'CA[2]': 'C15', 'CA[3]': 'C17',
            'CA[4]': 'B18', 'CA[5]': 'A16', 'CA[6]': 'A15',
            'WCK0_P': 'M19', 'WCK0_N': 'N19',
        },
    },

    # =========================================================================
    # Step 6: Determine Root Cause and Action
    # =========================================================================
    'step6_rca_action': {
        'title': 'Determine Root Cause and Action',
        'decision_tree': {
            'multiple_signals_same_die': {
                'conclusion': 'Die/solder issue',
                'action': 'De-pop/Reball for component evaluation',
                'example': 'LP1C_CA3 + LP1C_WCLK0_N both fail → U4 issue',
            },
            'same_signal_multiple_dies': {
                'conclusion': 'System/PCB routing issue',
                'action': 'Check PCB signal integrity, power rails',
                'example': 'CA3 fails on U1, U2, U3, U4 → system issue',
            },
            'single_dq_line_consistent': {
                'conclusion': 'Die bump or via defect',
                'action': 'pFA request for IV curve + thermal imaging',
                'example': 'DQ6 fails consistently across retests',
            },
            'intermittent_failures': {
                'conclusion': 'Marginal connectivity',
                'action': 'Visual inspection + 5X retest, check gold plate',
                'example': '1/5 pass, 4/5 fail with different SBIN',
            },
        },
        'failure_type_recommendations': {
            'CONT': 'De-pop/Reball → evaluate solder joints under microscope',
            'SPD': 'Check EEPROM, resistor network → may need re-program',
            'DQ_single': 'pFA with probe points: Die bump → PCB via → Socket',
            'DQ_multi': 'Check signal routing on PCB, power integrity',
            'NO_BOOT': 'Run HMFN 5X first, check VI for contamination',
        },
    },

    # =========================================================================
    # Key Learnings from Case Studies
    # =========================================================================
    'key_learnings': [
        'No Boot = No Summary: ENG No Boot failures do not generate mtsums records',
        'Use mfm +failcrawler: Provides per-address DQ/ROW/COL detail that mtsums lacks',
        'Trace signals to common die: Multiple signals on same die = component issue',
        'HMFN Pass + HMB1 Fail: Stress-related marginal defect, likely timing/power',
        'Gold plate contamination: Common in SOCAMM - always check VI first',
        'DRAMFAIL=NO: System classification, not necessarily DRAM defect',
        'HAST is special case: Requires eFA team approval, not standard procedure',
    ],

    # =========================================================================
    # Commands Quick Reference
    # =========================================================================
    'commands_quick_ref': {
        'chronology': 'mhist <MSN> -sbin',
        'production_fail': 'frpt -xf <SUM> -mod_id=/<MSN>/ +# +module',
        'eng_test_detail': 'tdat <ENG_SUMMARY>',
        'failcrawler_detail': 'mfm <MSN> -step=hmb1 +failcrawler',
        'basic_mtsums': 'mtsums <MSN> -step=hmb1 +quiet +csv -format=MSN,FID,ULOC,FAILCRAWLER',
    },
}

# =============================================================================
# SOCAMM/SOCAMM2 Specific Guardrails
# =============================================================================
# These rules are critical for correct RCA in SOCAMM2 modules

SOCAMM2_GUARDRAILS = {
    'dram_requires_row_stability': True,  # Do not escalate DRAM without ROW stability
    'modsys_requires_die_count': True,    # Do not trust MOD-SYS without die-count context
    'fablot_is_dram_proof': True,         # Use FABLOT as DRAM proof, not labels
    'uloc_clustering_is_env': True,       # Treat ULOC clustering as ENV evidence
    'supplier_is_env_unless_proven': True,  # Treat supplier correlation as ENV unless proven otherwise
}

# Die count thresholds for 16DP modules
DIE_COUNT_THRESHOLDS = {
    'single_die': 2,      # 1-2 dies failing → possible DRAM
    'multi_die': 3,       # 3+ dies failing → ENV / interface likely
    'many_die': 8,        # 8+ dies failing → systemic (definitely not single DRAM)
}


# =============================================================================
# Sanity Check Dimensions
# =============================================================================

SANITY_CHECK_DIMENSIONS = {
    # =========================================================================
    # Equipment Dimensions
    # =========================================================================
    'MACHINE_ID': {
        'category': 'Equipment',
        'icon': '🔧',
        'description': 'Test machine correlation',
        'signal_threshold': 50,  # >50% on single machine = strong signal
    },
    'TESTER': {
        'category': 'Equipment',
        'icon': '🔧',
        'description': 'Tester correlation',
        'signal_threshold': 50,
    },
    'SITE': {
        'category': 'Equipment',
        'icon': '🔌',
        'description': 'Motherboard slot location',
        'signal_threshold': 40,
    },

    # =========================================================================
    # Location Dimensions
    # =========================================================================
    'TEST_FACILITY': {
        'category': 'Location',
        'icon': '🏭',
        'description': 'Test facility correlation',
        'signal_threshold': 80,  # Most tests at one facility is normal
    },
    'ASSEMBLY_FACILITY': {
        'category': 'Location',
        'icon': '🏭',
        'description': 'Assembly facility correlation',
        'signal_threshold': 60,
    },

    # =========================================================================
    # Materials / Supplier Dimensions
    # =========================================================================
    'PCB_SUPPLIER': {
        'category': 'Materials',
        'icon': '📦',
        'description': 'PCB supplier correlation',
        'signal_threshold': 60,
    },
    'REGISTER_SUPPLIER': {
        'category': 'Materials',
        'icon': '📦',
        'description': 'Register supplier correlation',
        'signal_threshold': 60,
    },
    'PCB': {
        'category': 'Materials',
        'icon': '🔲',
        'description': 'PCB design ID',
        'signal_threshold': 70,
    },
    'PCB_ARTWORK_REV': {
        'category': 'Materials',
        'icon': '🔲',
        'description': 'PCB artwork revision',
        'signal_threshold': 60,
    },

    # =========================================================================
    # Silicon / Process Dimensions (DRAM source)
    # =========================================================================
    'FABLOT': {
        'category': 'Silicon',
        'icon': '💎',
        'description': 'Fab lot clustering (DRAM source)',
        'signal_threshold': 30,  # >30% from single fablot = strong signal
    },
    'FAB': {
        'category': 'Silicon',
        'icon': '🏭',
        'description': 'Fabrication facility',
        'signal_threshold': 70,
    },
    'WAFER': {
        'category': 'Silicon',
        'icon': '💿',
        'description': 'Wafer number within fablot',
        'signal_threshold': 40,
    },
    'WREG': {
        'category': 'Silicon',
        'icon': '🎯',
        'description': 'Wafer region (center vs edge)',
        'signal_threshold': 50,
    },
    'ULOC': {
        'category': 'Silicon',
        'icon': '📍',
        'description': 'Unit location (die position on module)',
        'signal_threshold': 40,
    },
    'RETICLE_WAVE_ID': {
        'category': 'Silicon',
        'icon': '🌊',
        'description': 'Reticle wave ID (process correlation)',
        'signal_threshold': 50,
    },

    # =========================================================================
    # Probe / Burn History Dimensions
    # =========================================================================
    'PROBE_REV': {
        'category': 'Probe/Burn',
        'icon': '🔬',
        'description': 'Probe test revision',
        'signal_threshold': 50,
    },
    'BURN_MAJOR_VERSION': {
        'category': 'Probe/Burn',
        'icon': '🔥',
        'description': 'Burn major version',
        'signal_threshold': 50,
    },
    'BURN_MACHINE_CONFIG': {
        'category': 'Probe/Burn',
        'icon': '🔥',
        'description': 'Burn machine configuration',
        'signal_threshold': 50,
    },

    # =========================================================================
    # Test Configuration Dimensions
    # =========================================================================
    'TEST_VERSION': {
        'category': 'Test Config',
        'icon': '📋',
        'description': 'Test version/flow',
        'signal_threshold': 50,
    },
    'FLOW': {
        'category': 'Test Config',
        'icon': '📋',
        'description': 'Test flow',
        'signal_threshold': 60,
    },
    'SBIN': {
        'category': 'Test Config',
        'icon': '🏷️',
        'description': 'Soft bin code',
        'signal_threshold': 40,
    },
}


def get_debug_flow(msn_status: str, failcrawler: str) -> dict:
    """
    Get the debug flow for a MSN_STATUS × FAILCRAWLER combination.

    Args:
        msn_status: The MSN_STATUS value
        failcrawler: The FAILCRAWLER category

    Returns:
        Debug flow dictionary with RCA type and check dimensions
    """
    key = (msn_status, failcrawler)
    return DEBUG_FLOWS.get(key, DEFAULT_DEBUG_FLOW)


def fetch_fid_attributes(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str],
    densities: list[str] = None,
    speeds: list[str] = None
) -> pd.DataFrame:
    """
    Fetch FID-level attributes that cannot be combined with failcrawler query.

    These attributes (PROBE_REV, BURN_*, WAFER, RETICLE_WAVE_ID) are crucial for
    silicon-level correlation analysis but mtsums doesn't allow them with +fidag failcrawler.

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks
        densities: Optional list of densities
        speeds: Optional list of speeds

    Returns:
        DataFrame with FID-level attributes keyed by FID
    """
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    # Query FID attributes using +fidattr (without failcrawler)
    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        f'-DESIGN_ID={design_id_str}',
        f'-mfg_workweek={workweek_str}',
        # Key fields for joining
        '-format=MSN,FID',
        # FID attributes that conflict with failcrawler
        '-format+=WAFER,PROBE_REV,BURN_MAJOR_VERSION,BURN_MACHINE_CONFIG,RETICLE_WAVE_ID',
        f'-step={step_str}',
        '-msn_status!=Pass',
    ]

    # Add optional filters
    if densities:
        cmd.append(f'-module_density={",".join(densities)}')
    if speeds:
        cmd.append(f'-module_speed={",".join(speeds)}')

    # Use +fidattr for FID attribute data (no failcrawler)
    cmd.append('+fidattr')

    logger.info("Fetching FID attributes (WAFER, PROBE_REV, BURN_*, RETICLE_WAVE_ID)...")

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300
        )

        if result.returncode != 0:
            logger.warning(f"FID attributes query failed: {result.stderr.decode() if result.stderr else 'Unknown'}")
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [col.upper() for col in df.columns]

        # Deduplicate - keep first occurrence per FID
        if 'FID' in df.columns:
            df = df.drop_duplicates(subset=['FID'], keep='first')

        logger.info(f"Fetched {len(df)} FID attribute records")
        return df

    except Exception as e:
        logger.warning(f"Error fetching FID attributes: {e}")
        return pd.DataFrame()


def fetch_sanity_check_data(
    design_ids: list[str],
    steps: list[str],
    workweeks: list[str],
    densities: list[str] = None,
    speeds: list[str] = None
) -> pd.DataFrame:
    """
    Fetch failure data with all sanity check dimensions.

    Uses TWO queries to get complete data:
    1. Failcrawler query (+fidag) - gets failure patterns and most dimensions
    2. FID attributes query (+fidattr) - gets WAFER, PROBE_REV, BURN_*, RETICLE_WAVE_ID

    These are merged on FID to provide comprehensive silicon-level correlation data.

    Args:
        design_ids: List of design IDs
        steps: List of test steps
        workweeks: List of workweeks
        densities: Optional list of densities
        speeds: Optional list of speeds

    Returns:
        DataFrame with failure data and all sanity check dimensions merged
    """
    design_id_str = ','.join(design_ids)
    step_str = ','.join([s.lower() for s in steps])
    workweek_str = ','.join([str(ww) for ww in workweeks])

    # ==========================================================================
    # Query 1: Failcrawler data with +fidag
    # ==========================================================================
    # Note: +fidag gives raw MSN/FID-level data, unlike +modfm which pivots by MSN_STATUS
    # IMPORTANT: Can't request failcrawler and fidattr data at the same time in mtsums
    cmd = [
        '/u/dramsoft/bin/mtsums',
        '-FORCEAPI', '+quiet', '+csv', '+stdf',
        '-exclude_baseline=NULL',
        f'-DESIGN_ID={design_id_str}',
        f'-mfg_workweek={workweek_str}',
        # Core fields
        '-format=MSN,FID,DESIGN_ID,STEP,MFG_WORKWEEK,FAILCRAWLER,MSN_STATUS',
        # Equipment dimensions
        '-format+=MACHINE_ID,TESTER,SITE',
        # Location dimensions
        '-format+=TEST_FACILITY,ASSEMBLY_FACILITY',
        # Materials / Supplier dimensions
        '-format+=PCB_SUPPLIER,REGISTER_SUPPLIER,PCB,PCB_ARTWORK_REV',
        # Silicon / Process dimensions (DRAM source)
        '-format+=ULOC,WREG,FAB',
        # Test config dimensions
        '-format+=FLOW,TEST_VERSION,SBIN',
        # Failcrawler details
        '-format+=DRAMFAIL,FCFM,VERIFIED',
        # Address dimensions (for stability analysis)
        '-format+=ADDRMASK,ADDRCNT,BITCNT,ROWCNT,COLCNT,DQCNT',
        f'-step={step_str}',
        '-msn_status!=Pass',
    ]

    # Add optional filters
    # Note: mtsums uses -module_density and -module_speed (not -density/-speed)
    if densities:
        cmd.append(f'-module_density={",".join(densities)}')
    if speeds:
        cmd.append(f'-module_speed={",".join(speeds)}')

    # Use +fidag for FID-level aggregation (raw data per MSN/FID)
    cmd.append('+fidag')

    logger.info("Fetching sanity check data (failcrawler query)...")

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=600
        )

        if result.returncode != 0:
            logger.error(f"mtsums error: {result.stderr.decode() if result.stderr else 'Unknown'}")
            return pd.DataFrame()

        output = result.stdout.decode()
        if not output.strip():
            return pd.DataFrame()

        df = pd.read_csv(StringIO(output))
        df.columns = [col.upper() for col in df.columns]

        # Extract FABLOT from FID if available
        if 'FID' in df.columns:
            df['FABLOT'] = df['FID'].apply(lambda x: str(x).split(':')[0] if pd.notna(x) else '')

        logger.info(f"Fetched {len(df)} failcrawler records")

        # ==========================================================================
        # Query 2: FID attributes (separate query due to mtsums limitation)
        # ==========================================================================
        fid_attr_df = fetch_fid_attributes(
            design_ids=design_ids,
            steps=steps,
            workweeks=workweeks,
            densities=densities,
            speeds=speeds
        )

        # Merge FID attributes if we got them
        if not fid_attr_df.empty and 'FID' in df.columns and 'FID' in fid_attr_df.columns:
            # Get only the new columns (exclude MSN, FID which are already in df)
            new_cols = [c for c in fid_attr_df.columns if c not in ['MSN', 'FID']]
            if new_cols:
                merge_cols = ['FID'] + new_cols
                df = df.merge(
                    fid_attr_df[merge_cols],
                    on='FID',
                    how='left'
                )
                logger.info(f"Merged FID attributes: {new_cols}")

        logger.info(f"Total sanity check records: {len(df)}")
        return df

    except Exception as e:
        logger.exception(f"Error fetching sanity check data: {e}")
        return pd.DataFrame()


def analyze_dimension(
    df: pd.DataFrame,
    dimension: str,
    total_fails: int
) -> dict:
    """
    Analyze a single dimension for concentration and signal strength.

    Args:
        df: DataFrame with failure data
        dimension: Column name to analyze
        total_fails: Total number of failures for percentage calculation

    Returns:
        Dictionary with dimension analysis results
    """
    if dimension not in df.columns or df[dimension].isna().all():
        return None

    # Count by dimension value
    counts = df[dimension].value_counts()

    if counts.empty:
        return None

    # Get top value and its concentration
    top_value = counts.index[0]
    top_count = counts.iloc[0]
    top_pct = (top_count / total_fails * 100) if total_fails > 0 else 0

    # Get dimension metadata
    dim_info = SANITY_CHECK_DIMENSIONS.get(dimension, {
        'category': 'Other',
        'icon': '📊',
        'signal_threshold': 50,
    })

    # Determine signal strength
    threshold = dim_info.get('signal_threshold', 50)
    if top_pct >= threshold:
        signal = 'STRONG'
        signal_color = '#27AE60'  # Green
    elif top_pct >= threshold * 0.7:
        signal = 'MODERATE'
        signal_color = '#F39C12'  # Orange
    else:
        signal = 'WEAK'
        signal_color = '#9E9E9E'  # Grey

    return {
        'dimension': dimension,
        'category': dim_info.get('category', 'Other'),
        'icon': dim_info.get('icon', '📊'),
        'top_value': str(top_value),
        'top_count': int(top_count),
        'top_pct': round(top_pct, 1),
        'total_values': len(counts),
        'signal': signal,
        'signal_color': signal_color,
        'threshold': threshold,
        'distribution': counts.head(5).to_dict(),
    }


def analyze_address_stability(df: pd.DataFrame) -> dict:
    """
    Analyze ROW/DQ address stability across failures.

    Stable addresses (same ROW/DQ across modules) suggest true defect.
    Varying addresses suggest test artifact or system issue.

    Args:
        df: DataFrame with failure data including ROWCNT, COLCNT, DQCNT

    Returns:
        Dictionary with address stability analysis
    """
    result = {
        'row_stability': None,
        'dq_stability': None,
        'conclusion': 'INSUFFICIENT_DATA',
    }

    if df.empty:
        return result

    # ROW stability
    if 'ROWCNT' in df.columns and not df['ROWCNT'].isna().all():
        row_counts = df['ROWCNT'].value_counts()
        if len(row_counts) > 0:
            dominant_row = row_counts.iloc[0]
            row_stability = dominant_row / len(df) * 100
            result['row_stability'] = {
                'dominant_value': int(row_counts.index[0]),
                'concentration': round(row_stability, 1),
                'is_stable': row_stability >= 70,
                'unique_values': len(row_counts),
            }

    # DQ stability
    if 'DQCNT' in df.columns and not df['DQCNT'].isna().all():
        dq_counts = df['DQCNT'].value_counts()
        if len(dq_counts) > 0:
            dominant_dq = dq_counts.iloc[0]
            dq_stability = dominant_dq / len(df) * 100
            result['dq_stability'] = {
                'dominant_value': int(dq_counts.index[0]),
                'concentration': round(dq_stability, 1),
                'is_stable': dq_stability >= 70,
                'unique_values': len(dq_counts),
            }

    # Determine overall conclusion
    row_stable = result['row_stability'] and result['row_stability'].get('is_stable', False)
    dq_stable = result['dq_stability'] and result['dq_stability'].get('is_stable', False)

    if row_stable and dq_stable:
        result['conclusion'] = 'STABLE_ADDRESS'
        result['interpretation'] = 'Same fail addresses across modules - likely true silicon defect'
    elif row_stable or dq_stable:
        result['conclusion'] = 'PARTIAL_STABILITY'
        result['interpretation'] = 'Partial address consistency - may be combination of issues'
    else:
        result['conclusion'] = 'RANDOM_ADDRESS'
        result['interpretation'] = 'Random fail addresses - likely system/test issue, not silicon'

    return result


def analyze_die_count_per_msn(df: pd.DataFrame) -> dict:
    """
    Analyze die count per MSN for 16DP awareness (SOCAMM2 specific).

    For SOCAMM2 modules with 16 dies:
    - 1-2 dies failing → possible DRAM (localized defect)
    - 3-7 dies failing → mixed / interface issue
    - 8+ dies failing → systemic ENV issue (not single DRAM)

    Args:
        df: DataFrame with failure data including MSN and ULOC

    Returns:
        Dictionary with die count analysis
    """
    result = {
        'avg_dies_per_msn': 0,
        'max_dies_per_msn': 0,
        'single_die_msns': 0,
        'multi_die_msns': 0,
        'many_die_msns': 0,
        'conclusion': 'INSUFFICIENT_DATA',
        'interpretation': '',
    }

    if df.empty or 'MSN' not in df.columns or 'ULOC' not in df.columns:
        return result

    # Count unique dies (ULOCs) per MSN
    dies_per_msn = df.groupby('MSN')['ULOC'].nunique()

    if dies_per_msn.empty:
        return result

    result['avg_dies_per_msn'] = round(dies_per_msn.mean(), 1)
    result['max_dies_per_msn'] = int(dies_per_msn.max())
    result['total_msns'] = len(dies_per_msn)

    # Categorize MSNs by die count
    result['single_die_msns'] = int((dies_per_msn <= DIE_COUNT_THRESHOLDS['single_die']).sum())
    result['multi_die_msns'] = int(((dies_per_msn > DIE_COUNT_THRESHOLDS['single_die']) &
                                     (dies_per_msn < DIE_COUNT_THRESHOLDS['many_die'])).sum())
    result['many_die_msns'] = int((dies_per_msn >= DIE_COUNT_THRESHOLDS['many_die']).sum())

    # Calculate percentages
    total = result['total_msns']
    result['single_die_pct'] = round(result['single_die_msns'] / total * 100, 1) if total > 0 else 0
    result['multi_die_pct'] = round(result['multi_die_msns'] / total * 100, 1) if total > 0 else 0
    result['many_die_pct'] = round(result['many_die_msns'] / total * 100, 1) if total > 0 else 0

    # Determine conclusion based on distribution
    if result['many_die_pct'] > 50:
        result['conclusion'] = 'SYSTEMIC_ENV'
        result['interpretation'] = f"{result['many_die_pct']:.0f}% of MSNs have 8+ dies failing - systemic ENV issue"
    elif result['single_die_pct'] > 60:
        result['conclusion'] = 'LOCALIZED_DRAM'
        result['interpretation'] = f"{result['single_die_pct']:.0f}% of MSNs have 1-2 dies failing - possible DRAM defect"
    elif result['multi_die_pct'] > 40:
        result['conclusion'] = 'INTERFACE_ISSUE'
        result['interpretation'] = f"{result['multi_die_pct']:.0f}% of MSNs have 3-7 dies failing - interface/package issue"
    else:
        result['conclusion'] = 'MIXED'
        result['interpretation'] = 'Mixed die count distribution - requires further investigation'

    return result


def analyze_supplier_dominance(df: pd.DataFrame) -> dict:
    """
    Analyze supplier dominance for ENV correlation (SOCAMM2 specific).

    Checks PCB_SUPPLIER, and other supplier-related dimensions.
    Strong supplier correlation → ENV-LIKELY

    Args:
        df: DataFrame with failure data

    Returns:
        Dictionary with supplier dominance analysis
    """
    result = {
        'pcb_supplier': None,
        'pcb_artwork_rev': None,
        'assembly_facility': None,
        'has_supplier_signal': False,
        'dominant_supplier': None,
        'interpretation': '',
    }

    if df.empty:
        return result

    total_fails = len(df)
    suppliers_checked = []

    # Check PCB supplier
    if 'PCB_SUPPLIER' in df.columns and not df['PCB_SUPPLIER'].isna().all():
        counts = df['PCB_SUPPLIER'].value_counts()
        if len(counts) > 0:
            top_value = counts.index[0]
            top_pct = counts.iloc[0] / total_fails * 100
            result['pcb_supplier'] = {
                'value': str(top_value),
                'count': int(counts.iloc[0]),
                'pct': round(top_pct, 1),
                'is_dominant': top_pct >= 70,
            }
            if top_pct >= 70:
                suppliers_checked.append(('PCB_SUPPLIER', top_value, top_pct))

    # Check PCB artwork revision
    if 'PCB_ARTWORK_REV' in df.columns and not df['PCB_ARTWORK_REV'].isna().all():
        counts = df['PCB_ARTWORK_REV'].value_counts()
        if len(counts) > 0:
            top_value = counts.index[0]
            top_pct = counts.iloc[0] / total_fails * 100
            result['pcb_artwork_rev'] = {
                'value': str(top_value),
                'count': int(counts.iloc[0]),
                'pct': round(top_pct, 1),
                'is_dominant': top_pct >= 70,
            }

    # Check assembly facility
    if 'ASSEMBLY_FACILITY' in df.columns and not df['ASSEMBLY_FACILITY'].isna().all():
        counts = df['ASSEMBLY_FACILITY'].value_counts()
        if len(counts) > 0:
            top_value = counts.index[0]
            top_pct = counts.iloc[0] / total_fails * 100
            result['assembly_facility'] = {
                'value': str(top_value),
                'count': int(counts.iloc[0]),
                'pct': round(top_pct, 1),
                'is_dominant': top_pct >= 70,
            }
            if top_pct >= 70:
                suppliers_checked.append(('ASSEMBLY_FACILITY', top_value, top_pct))

    # Determine if there's a supplier signal
    if suppliers_checked:
        result['has_supplier_signal'] = True
        top_supplier = max(suppliers_checked, key=lambda x: x[2])
        result['dominant_supplier'] = {
            'dimension': top_supplier[0],
            'value': str(top_supplier[1]),
            'pct': round(top_supplier[2], 1),
        }
        result['interpretation'] = f"Strong supplier signal: {top_supplier[0]}={top_supplier[1]} ({top_supplier[2]:.0f}%) - ENV-LIKELY"
    else:
        result['interpretation'] = 'No dominant supplier correlation - supplier not a clear factor'

    return result


def apply_socamm2_guardrails(
    confidence: dict,
    address_stability: dict,
    die_count_analysis: dict,
    supplier_analysis: dict,
    debug_flow: dict
) -> dict:
    """
    Apply SOCAMM2-specific guardrails to adjust confidence.

    Guardrails:
    - Do not escalate DRAM without ROW stability
    - Do not trust MOD-SYS without die-count context
    - Use FABLOT as DRAM proof, not labels
    - Treat ULOC clustering as ENV evidence
    - Treat supplier correlation as ENV unless proven otherwise

    Args:
        confidence: Original confidence assessment
        address_stability: Address stability analysis
        die_count_analysis: Die count per MSN analysis
        supplier_analysis: Supplier dominance analysis
        debug_flow: Debug flow for the combination

    Returns:
        Adjusted confidence with guardrail notes
    """
    adjusted = confidence.copy()
    guardrail_notes = []

    rca_type = debug_flow.get('rca_type', 'UNCLASSIFIED')

    # Guardrail 1: DRAM requires ROW stability
    if rca_type == 'DRAM':
        if address_stability and address_stability.get('conclusion') == 'RANDOM_ADDRESS':
            adjusted['guardrail_adjustment'] = -20
            guardrail_notes.append("⚠️ DRAM classification but ROW addresses are random - confidence reduced")

    # Guardrail 2: MOD-SYS requires die-count context
    if rca_type in ('BIOS', 'BIOS_PARTIAL'):
        if die_count_analysis and die_count_analysis.get('conclusion') == 'SYSTEMIC_ENV':
            guardrail_notes.append("✅ Many dies failing per MSN confirms ENV/system issue")
        elif die_count_analysis and die_count_analysis.get('conclusion') == 'LOCALIZED_DRAM':
            adjusted['guardrail_adjustment'] = -15
            guardrail_notes.append("⚠️ Few dies failing per MSN - may be hidden DRAM issue")

    # Guardrail 3: Supplier correlation indicates ENV
    if supplier_analysis and supplier_analysis.get('has_supplier_signal'):
        if rca_type == 'DRAM':
            adjusted['guardrail_adjustment'] = adjusted.get('guardrail_adjustment', 0) - 15
            guardrail_notes.append(f"⚠️ Strong supplier signal conflicts with DRAM classification")
        elif rca_type in ('BIOS', 'HW+SOP'):
            guardrail_notes.append(f"✅ Supplier signal supports ENV classification")

    # Apply adjustments
    if 'guardrail_adjustment' in adjusted:
        adjusted['percentage'] = max(0, min(100, adjusted['percentage'] + adjusted['guardrail_adjustment']))

        # Re-determine verdict based on adjusted percentage
        if adjusted['percentage'] >= 70:
            adjusted['verdict'] = 'HIGH_CONFIDENCE'
            adjusted['verdict_color'] = '#27AE60'
        elif adjusted['percentage'] >= 40:
            adjusted['verdict'] = 'MODERATE_CONFIDENCE'
            adjusted['verdict_color'] = '#F39C12'
        else:
            adjusted['verdict'] = 'LOW_CONFIDENCE'
            adjusted['verdict_color'] = '#E74C3C'

        adjusted['verdict_text'] = f"{adjusted['verdict'].replace('_', ' ').title()} (adjusted by SOCAMM2 guardrails)"

    adjusted['guardrail_notes'] = guardrail_notes

    return adjusted


def run_sanity_check(
    df: pd.DataFrame,
    msn_status: str,
    failcrawler: str,
    step: str
) -> dict:
    """
    Run sanity check validation for a specific MSN_STATUS × FAILCRAWLER combination.

    Args:
        df: DataFrame with failure data
        msn_status: The MSN_STATUS value
        failcrawler: The FAILCRAWLER category
        step: Test step

    Returns:
        Dictionary with sanity check results and confidence assessment
    """
    # Get debug flow for this combination
    debug_flow = get_debug_flow(msn_status, failcrawler)

    # Filter data
    filtered_df = df.copy()
    if msn_status and 'MSN_STATUS' in df.columns:
        filtered_df = filtered_df[filtered_df['MSN_STATUS'] == msn_status]
    if failcrawler and 'FAILCRAWLER' in df.columns:
        filtered_df = filtered_df[filtered_df['FAILCRAWLER'] == failcrawler]

    if filtered_df.empty:
        return {
            'status': 'NO_DATA',
            'msn_status': msn_status,
            'failcrawler': failcrawler,
            'debug_flow': debug_flow,
        }

    total_fails = len(filtered_df)

    # Analyze primary dimensions
    primary_results = []
    for dim in debug_flow['primary_checks']:
        analysis = analyze_dimension(filtered_df, dim, total_fails)
        if analysis:
            primary_results.append(analysis)

    # Analyze secondary dimensions
    secondary_results = []
    for dim in debug_flow['secondary_checks']:
        analysis = analyze_dimension(filtered_df, dim, total_fails)
        if analysis:
            secondary_results.append(analysis)

    # Analyze address stability for DRAM-type issues
    address_stability = None
    if debug_flow['rca_type'] in ('DRAM', 'BIOS_PARTIAL'):
        address_stability = analyze_address_stability(filtered_df)

    # =========================================================================
    # SOCAMM2-Specific Analysis
    # =========================================================================

    # Analyze die count per MSN (16DP awareness)
    die_count_analysis = analyze_die_count_per_msn(filtered_df)

    # Analyze supplier dominance
    supplier_analysis = analyze_supplier_dominance(filtered_df)

    # Calculate base confidence score
    confidence = calculate_confidence(
        primary_results, secondary_results, address_stability, debug_flow
    )

    # Apply SOCAMM2 guardrails to adjust confidence
    confidence = apply_socamm2_guardrails(
        confidence=confidence,
        address_stability=address_stability,
        die_count_analysis=die_count_analysis,
        supplier_analysis=supplier_analysis,
        debug_flow=debug_flow
    )

    return {
        'status': 'OK',
        'msn_status': msn_status,
        'failcrawler': failcrawler,
        'step': step,
        'total_fails': total_fails,
        'debug_flow': debug_flow,
        'primary_results': primary_results,
        'secondary_results': secondary_results,
        'address_stability': address_stability,
        'die_count_analysis': die_count_analysis,
        'supplier_analysis': supplier_analysis,
        'confidence': confidence,
    }


def calculate_confidence(
    primary_results: list,
    secondary_results: list,
    address_stability: dict,
    debug_flow: dict
) -> dict:
    """
    Calculate confidence score for the RCA conclusion.

    Args:
        primary_results: Analysis results for primary dimensions
        secondary_results: Analysis results for secondary dimensions
        address_stability: Address stability analysis
        debug_flow: The expected debug flow

    Returns:
        Dictionary with confidence assessment
    """
    score = 0
    max_score = 0
    evidence = []

    # Primary dimensions (weighted 2x)
    for result in primary_results:
        max_score += 2
        if result['signal'] == 'STRONG':
            score += 2
            evidence.append(f"✅ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")
        elif result['signal'] == 'MODERATE':
            score += 1
            evidence.append(f"⚠️ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")
        else:
            evidence.append(f"❌ {result['dimension']}: No clear signal")

    # Secondary dimensions (weighted 1x)
    for result in secondary_results:
        max_score += 1
        if result['signal'] == 'STRONG':
            score += 1
            evidence.append(f"✅ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")
        elif result['signal'] == 'MODERATE':
            score += 0.5
            evidence.append(f"⚠️ {result['dimension']}: {result['top_value']} ({result['top_pct']}%)")

    # Address stability for DRAM issues
    if address_stability and debug_flow['rca_type'] in ('DRAM', 'BIOS_PARTIAL'):
        max_score += 2
        if address_stability['conclusion'] == 'STABLE_ADDRESS':
            score += 2
            evidence.append(f"✅ Address stable - confirms silicon defect")
        elif address_stability['conclusion'] == 'RANDOM_ADDRESS':
            evidence.append(f"❌ Address random - may not be silicon defect")
        else:
            score += 1
            evidence.append(f"⚠️ Address partially stable")

    # Calculate percentage
    confidence_pct = (score / max_score * 100) if max_score > 0 else 0

    # Determine verdict
    if confidence_pct >= 70:
        verdict = 'HIGH_CONFIDENCE'
        verdict_text = f"High confidence in {debug_flow['rca_type']} classification"
        verdict_color = '#27AE60'
    elif confidence_pct >= 40:
        verdict = 'MODERATE_CONFIDENCE'
        verdict_text = f"Moderate confidence - additional investigation may help"
        verdict_color = '#F39C12'
    else:
        verdict = 'LOW_CONFIDENCE'
        verdict_text = f"Low confidence - RCA classification may need review"
        verdict_color = '#E74C3C'

    return {
        'score': round(score, 1),
        'max_score': max_score,
        'percentage': round(confidence_pct, 1),
        'verdict': verdict,
        'verdict_text': verdict_text,
        'verdict_color': verdict_color,
        'evidence': evidence,
    }


def create_sanity_check_summary_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML summary for sanity check results.

    Args:
        result: Sanity check result dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for the summary
    """
    if result['status'] == 'NO_DATA':
        return '<div style="padding: 1rem; color: #999;">No data for this combination</div>'

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    debug_flow = result['debug_flow']
    confidence = result['confidence']

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
            <div>
                <h3 style="margin: 0; color: {text_color};">
                    {result['msn_status']} × {result['failcrawler']}
                </h3>
                <p style="margin: 0.25rem 0 0 0; color: #888; font-size: 0.9rem;">
                    {debug_flow['description']}
                </p>
            </div>
            <div style="text-align: right;">
                <div style="background: {confidence['verdict_color']}; color: white; padding: 0.5rem 1rem; border-radius: 4px; font-weight: bold;">
                    {confidence['percentage']:.0f}% Confidence
                </div>
                <div style="font-size: 0.8rem; color: #888; margin-top: 0.25rem;">
                    Expected: {debug_flow['rca_type']} ({debug_flow['expected_recovery']}% recovery)
                </div>
            </div>
        </div>

        <div style="margin-bottom: 1rem;">
            <strong style="color: {text_color};">Evidence:</strong>
            <ul style="margin: 0.5rem 0; padding-left: 1.5rem; color: {text_color};">
    '''

    for evidence_item in confidence['evidence']:
        html += f'<li style="margin: 0.25rem 0;">{evidence_item}</li>'

    html += f'''
            </ul>
        </div>

        <div style="background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; padding: 0.75rem; border-radius: 4px;">
            <strong style="color: {text_color};">Verdict:</strong>
            <span style="color: {confidence['verdict_color']}; margin-left: 0.5rem;">
                {confidence['verdict_text']}
            </span>
        </div>
    </div>
    '''

    return html


def create_dimension_table_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML table showing all dimension correlations.

    Args:
        result: Sanity check result dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for the dimension table
    """
    if result['status'] == 'NO_DATA':
        return ''

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    header_bg = '#2D2D2D' if dark_mode else '#4472C4'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    html = f'''
    <table style="width: 100%; border-collapse: collapse; font-size: 0.9rem; margin-top: 1rem;">
        <thead>
            <tr style="background: {header_bg}; color: white;">
                <th style="padding: 0.5rem; text-align: left;">Priority</th>
                <th style="padding: 0.5rem; text-align: left;">Dimension</th>
                <th style="padding: 0.5rem; text-align: left;">Category</th>
                <th style="padding: 0.5rem; text-align: left;">Top Value</th>
                <th style="padding: 0.5rem; text-align: right;">Count</th>
                <th style="padding: 0.5rem; text-align: right;">%</th>
                <th style="padding: 0.5rem; text-align: center;">Signal</th>
            </tr>
        </thead>
        <tbody>
    '''

    def add_row(analysis, priority, row_bg):
        signal_badge = {
            'STRONG': f'<span style="background: #27AE60; color: white; padding: 2px 8px; border-radius: 4px;">STRONG</span>',
            'MODERATE': f'<span style="background: #F39C12; color: white; padding: 2px 8px; border-radius: 4px;">MODERATE</span>',
            'WEAK': f'<span style="background: #9E9E9E; color: white; padding: 2px 8px; border-radius: 4px;">WEAK</span>',
        }
        return f'''
            <tr style="background: {row_bg}; border-bottom: 1px solid {border_color};">
                <td style="padding: 0.5rem; color: {text_color};">{priority}</td>
                <td style="padding: 0.5rem; color: {text_color};">{analysis['icon']} {analysis['dimension']}</td>
                <td style="padding: 0.5rem; color: {text_color};">{analysis['category']}</td>
                <td style="padding: 0.5rem; color: {text_color}; font-family: monospace;">{analysis['top_value']}</td>
                <td style="padding: 0.5rem; text-align: right; color: {text_color};">{analysis['top_count']}</td>
                <td style="padding: 0.5rem; text-align: right; color: {text_color};">{analysis['top_pct']}%</td>
                <td style="padding: 0.5rem; text-align: center;">{signal_badge.get(analysis['signal'], '')}</td>
            </tr>
        '''

    # Primary dimensions
    for i, analysis in enumerate(result.get('primary_results', [])):
        row_bg = '#2D2D2D' if dark_mode and i % 2 else (bg_color if not dark_mode or i % 2 == 0 else '#252525')
        html += add_row(analysis, '🔴 Primary', row_bg)

    # Secondary dimensions
    for i, analysis in enumerate(result.get('secondary_results', [])):
        row_bg = '#2D2D2D' if dark_mode and i % 2 else (bg_color if not dark_mode or i % 2 == 0 else '#252525')
        html += add_row(analysis, '🟡 Secondary', row_bg)

    html += '''
        </tbody>
    </table>
    '''

    return html


def create_address_stability_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML for address stability analysis.

    Args:
        result: Sanity check result dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for address stability section
    """
    address = result.get('address_stability')
    if not address:
        return ''

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    conclusion_colors = {
        'STABLE_ADDRESS': '#27AE60',
        'PARTIAL_STABILITY': '#F39C12',
        'RANDOM_ADDRESS': '#E74C3C',
        'INSUFFICIENT_DATA': '#9E9E9E',
    }

    conclusion_color = conclusion_colors.get(address['conclusion'], '#9E9E9E')

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-top: 1rem;">
        <h4 style="margin: 0 0 0.75rem 0; color: {text_color};">📍 Address Stability Analysis</h4>

        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
    '''

    # ROW stability
    row_data = address.get('row_stability')
    if row_data:
        row_color = '#27AE60' if row_data['is_stable'] else '#E74C3C'
        html += f'''
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px;">
                <div style="font-weight: bold; color: {text_color};">ROW Count Stability</div>
                <div style="font-size: 1.5rem; color: {row_color}; margin: 0.25rem 0;">
                    {row_data['concentration']:.1f}%
                </div>
                <div style="font-size: 0.8rem; color: #888;">
                    Dominant: {row_data['dominant_value']} ({row_data['unique_values']} unique values)
                </div>
            </div>
        '''

    # DQ stability
    dq_data = address.get('dq_stability')
    if dq_data:
        dq_color = '#27AE60' if dq_data['is_stable'] else '#E74C3C'
        html += f'''
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px;">
                <div style="font-weight: bold; color: {text_color};">DQ Count Stability</div>
                <div style="font-size: 1.5rem; color: {dq_color}; margin: 0.25rem 0;">
                    {dq_data['concentration']:.1f}%
                </div>
                <div style="font-size: 0.8rem; color: #888;">
                    Dominant: {dq_data['dominant_value']} ({dq_data['unique_values']} unique values)
                </div>
            </div>
        '''

    html += f'''
        </div>

        <div style="margin-top: 1rem; padding: 0.5rem; background: {'#2D2D2D' if dark_mode else '#F0F0F0'}; border-left: 4px solid {conclusion_color}; border-radius: 0 4px 4px 0;">
            <strong style="color: {conclusion_color};">{address['conclusion'].replace('_', ' ')}</strong>
            <span style="color: {text_color}; margin-left: 0.5rem;">
                — {address.get('interpretation', '')}
            </span>
        </div>
    </div>
    '''

    return html


def create_debug_flow_html(debug_flow: dict, dark_mode: bool = False) -> str:
    """
    Create HTML showing the expected debug flow for the combination.

    Args:
        debug_flow: Debug flow dictionary
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for debug flow display
    """
    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    rca_colors = {
        'HW+SOP': '#3498DB',
        'BIOS': '#27AE60',
        'BIOS_PARTIAL': '#F39C12',
        'DRAM': '#E74C3C',
        'NO_FIX': '#7F8C8D',
        'UNCLASSIFIED': '#9E9E9E',
    }

    rca_color = rca_colors.get(debug_flow['rca_type'], '#9E9E9E')

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
        <h4 style="margin: 0 0 0.75rem 0; color: {text_color};">🔍 Expected Debug Flow</h4>

        <div style="display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem;">
            <div style="padding: 0.5rem 1rem; background: {rca_color}; color: white; border-radius: 4px; font-weight: bold;">
                {debug_flow['rca_type']}
            </div>
            <div style="padding: 0.5rem 1rem; background: {'#2D2D2D' if dark_mode else '#F0F0F0'}; color: {text_color}; border-radius: 4px;">
                Expected Recovery: {debug_flow['expected_recovery']}%
            </div>
        </div>

        <div style="margin-bottom: 0.75rem;">
            <strong style="color: {text_color};">Primary Checks:</strong>
            <span style="color: #888; margin-left: 0.5rem;">
                {' → '.join(debug_flow['primary_checks'])}
            </span>
        </div>
    '''

    if debug_flow['secondary_checks']:
        html += f'''
        <div style="margin-bottom: 0.75rem;">
            <strong style="color: {text_color};">Secondary Checks:</strong>
            <span style="color: #888; margin-left: 0.5rem;">
                {' → '.join(debug_flow['secondary_checks'])}
            </span>
        </div>
        '''

    # Validation criteria
    if debug_flow.get('validation_criteria'):
        html += f'''
        <div style="margin-top: 1rem; padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px;">
            <strong style="color: {text_color};">Validation Criteria:</strong>
            <ul style="margin: 0.5rem 0 0 0; padding-left: 1.5rem; color: #888;">
        '''
        for key, value in debug_flow['validation_criteria'].items():
            html += f'<li style="margin: 0.25rem 0;"><strong>{key.replace("_", " ").title()}:</strong> {value}</li>'
        html += '''
            </ul>
        </div>
        '''

    html += '</div>'

    return html


def create_die_count_analysis_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML for 16DP die count analysis (SOCAMM2 specific).

    Args:
        result: Sanity check result containing die_count_analysis
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for die count section
    """
    die_count = result.get('die_count_analysis')
    if not die_count or die_count.get('conclusion') == 'INSUFFICIENT_DATA':
        return ''

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    conclusion_colors = {
        'SYSTEMIC_ENV': '#3498DB',      # Blue - ENV
        'LOCALIZED_DRAM': '#E74C3C',    # Red - DRAM
        'INTERFACE_ISSUE': '#F39C12',   # Orange - Interface
        'MIXED': '#9E9E9E',             # Grey - Mixed
    }

    conclusion_color = conclusion_colors.get(die_count['conclusion'], '#9E9E9E')

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-top: 1rem;">
        <h4 style="margin: 0 0 0.75rem 0; color: {text_color};">🔢 16DP Die Count Analysis (SOCAMM2)</h4>

        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 1rem;">
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px; text-align: center;">
                <div style="font-size: 0.8rem; color: #888;">Avg Dies/MSN</div>
                <div style="font-size: 1.5rem; color: {text_color}; font-weight: bold;">{die_count['avg_dies_per_msn']}</div>
            </div>
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px; text-align: center;">
                <div style="font-size: 0.8rem; color: #888;">1-2 Dies (DRAM)</div>
                <div style="font-size: 1.5rem; color: #E74C3C; font-weight: bold;">{die_count['single_die_pct']}%</div>
                <div style="font-size: 0.7rem; color: #888;">{die_count['single_die_msns']} MSNs</div>
            </div>
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px; text-align: center;">
                <div style="font-size: 0.8rem; color: #888;">3-7 Dies (Interface)</div>
                <div style="font-size: 1.5rem; color: #F39C12; font-weight: bold;">{die_count['multi_die_pct']}%</div>
                <div style="font-size: 0.7rem; color: #888;">{die_count['multi_die_msns']} MSNs</div>
            </div>
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px; text-align: center;">
                <div style="font-size: 0.8rem; color: #888;">8+ Dies (ENV)</div>
                <div style="font-size: 1.5rem; color: #3498DB; font-weight: bold;">{die_count['many_die_pct']}%</div>
                <div style="font-size: 0.7rem; color: #888;">{die_count['many_die_msns']} MSNs</div>
            </div>
        </div>

        <div style="padding: 0.5rem; background: {'#2D2D2D' if dark_mode else '#F0F0F0'}; border-left: 4px solid {conclusion_color}; border-radius: 0 4px 4px 0;">
            <strong style="color: {conclusion_color};">{die_count['conclusion'].replace('_', ' ')}</strong>
            <span style="color: {text_color}; margin-left: 0.5rem;">
                — {die_count.get('interpretation', '')}
            </span>
        </div>
    </div>
    '''

    return html


def create_supplier_analysis_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML for supplier dominance analysis.

    Args:
        result: Sanity check result containing supplier_analysis
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for supplier analysis section
    """
    supplier = result.get('supplier_analysis')
    if not supplier:
        return ''

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-top: 1rem;">
        <h4 style="margin: 0 0 0.75rem 0; color: {text_color};">📦 Supplier Correlation Analysis</h4>

        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-bottom: 1rem;">
    '''

    # PCB Supplier
    if supplier.get('pcb_supplier'):
        pcb = supplier['pcb_supplier']
        is_dom = pcb.get('is_dominant', False)
        html += f'''
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px; border-left: 4px solid {'#27AE60' if is_dom else '#9E9E9E'};">
                <div style="font-size: 0.8rem; color: #888;">PCB Supplier</div>
                <div style="font-size: 1.1rem; color: {text_color}; font-weight: bold; font-family: monospace;">{pcb['value']}</div>
                <div style="font-size: 0.9rem; color: {'#27AE60' if is_dom else '#888'};">{pcb['pct']}% ({pcb['count']})</div>
            </div>
        '''

    # PCB Artwork Rev
    if supplier.get('pcb_artwork_rev'):
        pcb_rev = supplier['pcb_artwork_rev']
        is_dom = pcb_rev.get('is_dominant', False)
        html += f'''
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px; border-left: 4px solid {'#27AE60' if is_dom else '#9E9E9E'};">
                <div style="font-size: 0.8rem; color: #888;">PCB Artwork Rev</div>
                <div style="font-size: 1.1rem; color: {text_color}; font-weight: bold; font-family: monospace;">{pcb_rev['value']}</div>
                <div style="font-size: 0.9rem; color: {'#27AE60' if is_dom else '#888'};">{pcb_rev['pct']}% ({pcb_rev['count']})</div>
            </div>
        '''

    # Assembly Facility
    if supplier.get('assembly_facility'):
        assy = supplier['assembly_facility']
        is_dom = assy.get('is_dominant', False)
        html += f'''
            <div style="padding: 0.75rem; background: {'#2D2D2D' if dark_mode else '#F5F5F5'}; border-radius: 4px; border-left: 4px solid {'#27AE60' if is_dom else '#9E9E9E'};">
                <div style="font-size: 0.8rem; color: #888;">Assembly Facility</div>
                <div style="font-size: 1.1rem; color: {text_color}; font-weight: bold; font-family: monospace;">{assy['value']}</div>
                <div style="font-size: 0.9rem; color: {'#27AE60' if is_dom else '#888'};">{assy['pct']}% ({assy['count']})</div>
            </div>
        '''

    html += '</div>'

    # Interpretation
    signal_color = '#27AE60' if supplier.get('has_supplier_signal') else '#9E9E9E'
    html += f'''
        <div style="padding: 0.5rem; background: {'#2D2D2D' if dark_mode else '#F0F0F0'}; border-left: 4px solid {signal_color}; border-radius: 0 4px 4px 0;">
            <span style="color: {text_color};">{supplier.get('interpretation', 'No supplier analysis available')}</span>
        </div>
    </div>
    '''

    return html


def create_guardrail_notes_html(result: dict, dark_mode: bool = False) -> str:
    """
    Create HTML for SOCAMM2 guardrail notes.

    Args:
        result: Sanity check result containing confidence with guardrail_notes
        dark_mode: Whether to use dark mode colors

    Returns:
        HTML string for guardrail notes
    """
    confidence = result.get('confidence', {})
    notes = confidence.get('guardrail_notes', [])

    if not notes:
        return ''

    bg_color = '#1E1E1E' if dark_mode else '#FFFFFF'
    text_color = '#E0E0E0' if dark_mode else '#333333'
    border_color = '#444444' if dark_mode else '#E0E0E0'

    html = f'''
    <div style="background: {bg_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 1rem; margin-top: 1rem;">
        <h4 style="margin: 0 0 0.75rem 0; color: {text_color};">🛡️ SOCAMM2 Guardrail Notes</h4>
        <ul style="margin: 0; padding-left: 1.5rem; color: {text_color};">
    '''

    for note in notes:
        html += f'<li style="margin: 0.25rem 0;">{note}</li>'

    html += '''
        </ul>
    </div>
    '''

    return html
