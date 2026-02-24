"""
Configuration settings for ESP to Stonebranch conversion
"""

# ======================= Stonebranch Task Types =======================
STONEBRANCH_TASK_TYPES = {
    'JOB': 'taskUnix',           # Mainframe JOB -> Unix Task
    'LINUX_JOB': 'taskLinux',    # Linux Job -> Linux Task
    'NT_JOB': 'taskWindows',     # Windows Job -> Windows Task
    'APPLEND': 'taskManual',     # Application End -> Manual Task
}

# ======================= Schedule Mapping =======================
ESP_TO_CRON_MAPPING = {
    'DAILY': '0 0 * * *',
    '1ST DAY OF MONTH': '0 0 1 * *',
    '2ND DAY OF MONTH': '0 0 2 * *',
    '3RD DAY OF MONTH': '0 0 3 * *',
    'LAST DAY OF MONTH': '0 0 L * *',
    '1ST MON OF MONTH': '0 0 * * 1#1',
    '2ND MON OF MONTH': '0 0 * * 1#2',
    '1ST FRI OF MONTH': '0 0 * * 5#1',
    '2ND FRI OF MONTH': '0 0 * * 5#2',
}

# ======================= Agent Mapping =======================
# Map ESP agents to Stonebranch agents
DEFAULT_AGENT_MAPPING = {
    # Example mappings - customize based on your environment
    # 'ESP_AGENT_NAME': 'stonebranch_agent_name',
}

# ======================= Default Values =======================
DEFAULT_BUSINESS_SERVICE = "Migrated from ESP"
DEFAULT_CREDENTIAL = "default_credential"
DEFAULT_RUNTIME_DIR = "/tmp"
DEFAULT_AGENT = "default-agent"

# ======================= Output Settings =======================
OUTPUT_FORMAT = 'json'  # 'json' or 'xml'
INDENT_JSON = 2

# ======================= Workflow Settings =======================
WORKFLOW_PREFIX = "WF_"
TASK_PREFIX = "TASK_"

# ======================= Logging =======================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
