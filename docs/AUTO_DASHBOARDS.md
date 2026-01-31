# Auto Dashboard Setup - Superset Native YAML Format

## Overview

This document describes the plan for converting dashboard JSON files to Superset's native YAML export format for reliable import with full chart definitions.

## Current State

**Custom JSON format** (`superset/dashboards/*.json`):
- Dashboard metadata + embedded chart array
- Charts reference datasets by table name (e.g., `"datasource": "daily_active_users"`)
- Contains SQL queries, viz_type, params

**Superset Native format** (ZIP with YAML files):
```
export.zip/
├── metadata.yaml
├── databases/DatabaseName.yaml
├── datasets/DatabaseName/table_name.yaml
├── charts/chart_name.yaml          # Each chart is separate
└── dashboards/dashboard_name.yaml  # References charts by UUID
```

## Implementation Plan

### Step 1: Create Conversion Script

**File:** `scripts/convert_dashboards.py`

**Functionality:**
1. Read custom JSON files from `superset/dashboards/`
2. Export existing datasets from Superset to get UUIDs (or generate deterministic UUIDs)
3. Generate chart YAML files with required fields:
   - `slice_name`, `viz_type`, `uuid`, `version: 1.0.0`
   - `dataset_uuid` (mapped from table name)
   - `params` (visualization config)
   - `query_context` (optional)
4. Generate dashboard YAML with position JSON referencing charts
5. Package everything into a ZIP file

### Step 2: YAML Schema Mapping

**Chart YAML structure:**
```yaml
slice_name: DAU/MAU Time Series
description: Daily and Monthly Active Users trend
viz_type: echarts_timeseries_line
uuid: <generated-uuid>
version: 1.0.0
dataset_uuid: <from-dataset-export>
params:
  viz_type: echarts_timeseries_line
  x_axis: date
  metrics: [dau]
  time_grain_sqla: P1D
  ...
query_context: null  # Let Superset generate
cache_timeout: 3600
```

**Dashboard position JSON with chart references:**
```yaml
position:
  CHART-uuid1:
    type: CHART
    id: CHART-uuid1
    meta:
      uuid: <chart-uuid>
      chartId: 1
      sliceName: DAU/MAU Time Series
      width: 4
      height: 50
    children: []
    parents: [ROOT_ID, GRID_ID, ROW-1]
```

### Step 3: Dataset UUID Resolution

Options (in order of preference):
1. **Query existing datasets via API** - Get UUIDs from running Superset
2. **Use deterministic UUIDs** - Generate UUID from `uuid5(NAMESPACE, table_name)`
3. **Include datasets in export** - Bundle dataset YAMLs in the ZIP

Recommendation: Option 1 with Option 2 as fallback.

### Step 4: Update setup_superset.py

Modify the script to:
1. Call conversion script if native export doesn't exist
2. Use `superset import-dashboards` CLI for import
3. Fall back to current API method if CLI fails

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `scripts/convert_dashboards.py` | Create | Main conversion script |
| `scripts/setup_superset.py` | Modify | Integrate native import |
| `superset/exports/` | Create | Output directory for generated ZIP |

## Conversion Mapping

| Custom JSON Field | Native YAML Field |
|-------------------|-------------------|
| `dashboard_title` | `dashboard_title` |
| `slug` | `slug` |
| `charts[].slice_name` | `charts/*.yaml: slice_name` |
| `charts[].viz_type` | `charts/*.yaml: viz_type` |
| `charts[].params.datasource` | Lookup → `dataset_uuid` |
| `charts[].params.*` | `charts/*.yaml: params.*` |
| `charts[].cache_timeout` | `charts/*.yaml: cache_timeout` |

## Verification

1. Run conversion script:
   ```bash
   .venv/bin/python scripts/convert_dashboards.py
   ```

2. Inspect generated ZIP:
   ```bash
   unzip -l superset/exports/dashboards_native.zip
   ```

3. Import to fresh Superset:
   ```bash
   docker exec goodnote-superset superset import-dashboards -p /path/to/export.zip
   ```

4. Verify in Superset UI:
   - All 4 dashboards appear
   - Each dashboard has its charts
   - Charts display data correctly

## Scope

**In scope:**
- Convert 4 dashboard JSON files
- Generate chart YAMLs with proper viz_type and params
- Generate dashboard YAMLs with position layout
- Package as importable ZIP

**Out of scope:**
- Complex filter configurations
- Custom CSS
- Alert/report schedules

## References

- [Superset Import/Export Docs](https://superset.apache.org/docs/configuration/importing-exporting-datasources/)
- [Superset Chart Schema](https://github.com/apache/superset/blob/master/superset/charts/schemas.py)
