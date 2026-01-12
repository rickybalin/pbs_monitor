# Implementation Plan: `pbs-monitor score-formula` Command

## Overview

Add a new CLI command `pbs-monitor score-formula` that displays the PBS job sorting formula and documents all input parameters with their descriptions and current default values.

## Goal

Provide users with a clear, readable view of:
1. The current `job_sort_formula` from the PBS server (dynamically fetched, not hardcoded)
2. All input variables used in the formula with descriptions
3. Current default values from server configuration

## Files to Modify

### 1. `/home/parton/pbs_monitor/pbs_monitor/cli/main.py`

**Changes:**
- Add import for `ScoreFormulaCommand` (line ~14)
- Add argument parser for `score-formula` command in `create_parser()` (after line ~697, before config command)
- Add dispatch branch in `main()` (after line ~1032, before the else clause)

### 2. `/home/parton/pbs_monitor/pbs_monitor/cli/commands.py`

**Changes:**
- Add new `ScoreFormulaCommand` class (at end of file, after `ReservationsCommand`)

## Implementation Details

### New Command: `ScoreFormulaCommand`

The command will:

1. **Fetch the formula dynamically** from PBS server using `self.collector.pbs_commands.get_job_sort_formula()`

2. **Display the formula** in a readable, formatted way:
   - Raw formula string
   - Optionally break down into components/terms

3. **List all parameters** with:
   - Variable name
   - Description (from a static documentation dict)
   - Current default value (from `resources_default` in server data)
   - Source (job resource vs server default)

### Parameter Documentation

Based on analysis of `pbs_commands.py:650-672`, the variables are:

| Variable | Description | Source |
|----------|-------------|--------|
| `base_score` | Base priority score for the job | Job resource or server default |
| `score_boost` | Additional priority boost | Job resource or server default |
| `enable_wfp` | Enable Wait-time Factor Priority (0/1) | Job resource or server default |
| `wfp_factor` | Multiplier for WFP calculation | Job resource or server default |
| `enable_backfill` | Enable backfill scheduling (0/1) | Job resource or server default |
| `backfill_max` | Maximum backfill bonus | Job resource or server default |
| `backfill_factor` | Divisor for backfill time calculation | Job resource or server default |
| `enable_fifo` | Enable FIFO ordering (0/1) | Job resource or server default |
| `fifo_factor` | Divisor for FIFO time calculation | Job resource or server default |
| `project_priority` | Project-level priority multiplier | Job resource (default: 1) |
| `nodect` | Number of nodes requested | Job resource (default: 1) |
| `total_cpus` | Total CPUs in cluster | Server default |
| `walltime` | Requested walltime in seconds | Job resource |
| `eligible_time` | Time since job became eligible (seconds) | Job attribute |

### CLI Arguments

```
pbs-monitor score-formula [options]

Options:
  --raw          Show only the raw formula string
  --no-defaults  Hide the default values table
  -r, --refresh  Force refresh of server data
```

### Output Format

Default output will show:

```
PBS Job Sort Formula
====================

Formula:
  base_score + score_boost + (enable_wfp * wfp_factor * ...) + ...

Formula Components:
  1. Base Score:     base_score + score_boost
  2. Wait-time Factor Priority (WFP):
     enable_wfp * wfp_factor * (eligible_time² / min(max(walltime,21600),43200)³ * project_priority * nodect / total_cpus)
  3. Backfill Bonus:
     enable_backfill * min(backfill_max, eligible_time / backfill_factor)
  4. FIFO Bonus:
     enable_fifo * eligible_time / fifo_factor

Parameters:
┌─────────────────┬─────────────────────────────────────────┬─────────────┬─────────┐
│ Variable        │ Description                             │ Default     │ Source  │
├─────────────────┼─────────────────────────────────────────┼─────────────┼─────────┤
│ base_score      │ Base priority score for the job         │ 0           │ server  │
│ score_boost     │ Additional priority boost               │ 0           │ server  │
│ enable_wfp      │ Enable Wait-time Factor Priority (0/1)  │ 0           │ server  │
│ ...             │ ...                                     │ ...         │ ...     │
└─────────────────┴─────────────────────────────────────────┴─────────────┴─────────┘
```

## Code Structure

```python
class ScoreFormulaCommand(BaseCommand):
    """Display and explain the PBS job sort formula"""

    # Static documentation for formula variables
    VARIABLE_DOCS = {
        "base_score": "Base priority score for the job",
        "score_boost": "Additional priority boost",
        # ... etc
    }

    def execute(self, args: argparse.Namespace) -> int:
        # 1. Get server data
        # 2. Extract formula and defaults
        # 3. Display based on args (--raw, etc.)
        # 4. Return 0 on success

    def _format_formula_breakdown(self, formula: str) -> str:
        # Parse and format the formula into readable components

    def _get_parameters_table(self, server_defaults: dict) -> Table:
        # Build rich Table with variable documentation
```

## Verification

1. **Test with sample data:**
   ```bash
   pbs-monitor --use-sample-data score-formula
   ```

2. **Test on live system:**
   ```bash
   pbs-monitor score-formula
   pbs-monitor score-formula --raw
   pbs-monitor score-formula --no-defaults
   ```

3. **Verify help text:**
   ```bash
   pbs-monitor score-formula --help
   ```

## Notes

- Formula is fetched dynamically from PBS server, never hardcoded
- Variable descriptions are static documentation (these don't change with the formula)
- Default values are fetched from the live server's `resources_default`
- Uses existing patterns from other commands (BaseCommand, rich tables, etc.)
