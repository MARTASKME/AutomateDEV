# ConvertToStonebranch

Python implementation of ESP to Stonebranch UAC Conversion Tool  
(Similar to xpressconversiontool2)

## Features

- Parse CA ESP (Workload Automation ESP) job definitions
- Convert to Stonebranch UAC format:
  - Tasks (Unix, Linux, Windows)
  - Workflows with dependencies
  - Triggers with schedules
  - Variables
- Export to JSON and CSV formats
- Generate Stonebranch import files

## Files

| File | Description |
|------|-------------|
| `esp_parser.py` | Parse ESP format files |
| `stonebranch_converter.py` | Convert ESP to Stonebranch format |
| `workflow_builder.py` | Build workflows with dependencies |
| `exporter.py` | Export to JSON, CSV, and import formats |
| `config.py` | Configuration settings |
| `main.py` | Main entry point with CLI |
| `run_converter.py` | Simple run script |

## Usage

### Command Line

```bash
# Interactive mode
python run_converter.py

# With arguments
python run_converter.py input.txt ./output
```

### As Module

```python
from ConvertToStonebranch.esp_parser import ESPParser
from ConvertToStonebranch.stonebranch_converter import StonebranchConverter
from ConvertToStonebranch.exporter import Exporter

# Parse ESP file
parser = ESPParser(file_path="input.txt")
esp_data = parser.parse()

# Convert to Stonebranch
converter = StonebranchConverter(esp_data)
converted = converter.convert()

# Export
exporter = Exporter(converted, "./output")
exporter.export_all("my_conversion")
```

## ESP to Stonebranch Mapping

| ESP Type | Stonebranch Type |
|----------|------------------|
| JOB | taskUnix |
| LINUX_JOB | taskLinux |
| NT_JOB | taskWindows |
| APPLEND | taskManual |
| APPLICATION | taskWorkflow |
| RUN schedule | triggerTime |
| AFTER | workflow edge |
| RELEASE | workflow edge |

## Output Files

- `*_tasks_*.json` - Stonebranch task definitions
- `*_workflows_*.json` - Stonebranch workflow definitions
- `*_tasks_*.csv` - Tasks in CSV format
- `*_workflows_*.csv` - Workflows in CSV format
- `*_stonebranch_import.json` - Ready for UAC API import
- `*_report.txt` - Conversion summary report

## Configuration

Edit `config.py` to customize:
- Agent mappings
- Default values
- Output formats
- Task type mappings

## Requirements

- Python 3.8+
- No external dependencies (standard library only)
