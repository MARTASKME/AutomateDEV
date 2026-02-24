import sys
import os
import re
import json
import pandas as pd
from collections import OrderedDict
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.createFile import createJson
from utils.readFile import loadText

# ======================= Configuration =======================
JSON_OUTPUT_FILE = 'textJsonOutput.json'
EXCEL_OUTPUT_FILE = 'textJsonOutput.xlsx'

# Excel limits
MAX_ROWS_PER_SHEET = 1000000  # Excel limit is 1,048,576 but leave some buffer
MAX_COLS_PER_SHEET = 16000    # Excel limit is 16,384
CHUNK_SIZE = 50000            # Rows per chunk for progress display

# Define job block start patterns for different mainframe schedulers
JOB_START_PATTERNS = {
    'jil': r'^insert_job:\s*(\S+)',                    # Autosys JIL format
    'control_m': r'^(?:SMART_FOLDER|FOLDER|JOB)(?:_NAME)?:\s*(\S+)',  # BMC Control-M
    'tws': r'^JOBS\s+(\S+)',                           # IBM TWS
    'generic': r'^(?:JOB|JOBNAME|NAME)\s*[:\s=]\s*(\S+)',  # Generic format
}

# Define attribute patterns for parsing
ATTRIBUTE_PATTERNS = {
    'key_value_colon': r'^(\w+):\s*(.*)$',           # key: value
    'key_value_equals': r'^(\w+)\s*=\s*(.*)$',       # key = value
    'key_value_space': r'^(\w+)\s+(.+)$',            # key value (space separated)
}

# Sheet names for Excel output
SHEETNAME_LIST = ["JOBS", "FOLDERS", "TRIGGERS", "CALENDARS", "VARIABLES", "CONDITIONS"]

# ======================= Helper Functions =======================

def detect_format(content):
    """Detect the mainframe job file format based on content patterns"""
    lines = content.strip().split('\n')[:100]  # Check first 100 lines
    
    # Check for Autosys JIL format
    if any(re.match(r'^insert_job:', line, re.IGNORECASE) for line in lines):
        return 'jil'
    
    # Check for CA ESP format (MEMBER NAME + ESPPROC)
    if any(re.match(r'^MEMBER\s+NAME\s+', line, re.IGNORECASE) for line in lines):
        return 'esp'
    if any(re.search(r':ESPPROC', line, re.IGNORECASE) for line in lines):
        return 'esp'
    
    # Check for Control-M format
    if any(re.match(r'^(SMART_)?FOLDER', line, re.IGNORECASE) for line in lines):
        return 'control_m'
    
    # Check for TWS format
    if any(re.match(r'^JOBS\s+', line, re.IGNORECASE) for line in lines):
        return 'tws'
    
    # Default to generic
    return 'generic'


def parse_jil_format(content):
    """Parse Autosys JIL format text file"""
    jobs = []
    current_job = None
    
    lines = content.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines and comments
        if not line or line.startswith('#') or line.startswith('/*'):
            continue
        
        # Check for new job definition
        job_match = re.match(r'^insert_job:\s*(\S+)\s*job_type:\s*(\S+)', line, re.IGNORECASE)
        if job_match:
            if current_job:
                jobs.append(current_job)
            current_job = OrderedDict()
            current_job['job_name'] = job_match.group(1)
            current_job['job_type'] = job_match.group(2)
            continue
        
        # Check for simple insert_job
        simple_job_match = re.match(r'^insert_job:\s*(\S+)', line, re.IGNORECASE)
        if simple_job_match:
            if current_job:
                jobs.append(current_job)
            current_job = OrderedDict()
            current_job['job_name'] = simple_job_match.group(1)
            continue
        
        # Parse key-value pairs
        if current_job is not None:
            # Try colon separator first
            kv_match = re.match(r'^(\w+):\s*(.*)$', line)
            if kv_match:
                key = kv_match.group(1).lower()
                value = kv_match.group(2).strip()
                current_job[key] = value
    
    # Don't forget the last job
    if current_job:
        jobs.append(current_job)
    
    return {'JOBS': jobs}


def parse_control_m_format(content):
    """Parse BMC Control-M text format"""
    result = {
        'SMART_FOLDER': [],
        'FOLDER': [],
        'JOB': [],
        'VARIABLE': [],
        'INCOND': [],
        'OUTCOND': []
    }
    
    current_block = None
    current_type = None
    parent_folder = None
    
    lines = content.strip().split('\n')
    
    for line in lines:
        original_line = line
        line = line.strip()
        
        # Skip empty lines and comments
        if not line or line.startswith('#') or line.startswith('/*'):
            continue
        
        # Check for block start patterns
        smart_folder_match = re.match(r'^SMART_FOLDER\s+(\S+)', line, re.IGNORECASE)
        folder_match = re.match(r'^FOLDER\s+(\S+)', line, re.IGNORECASE)
        job_match = re.match(r'^JOB\s+(\S+)', line, re.IGNORECASE)
        
        if smart_folder_match:
            if current_block and current_type:
                result[current_type].append(current_block)
            current_block = OrderedDict()
            current_block['SMART_FOLDER_NAME'] = smart_folder_match.group(1)
            current_type = 'SMART_FOLDER'
            parent_folder = smart_folder_match.group(1)
            continue
            
        if folder_match:
            if current_block and current_type:
                result[current_type].append(current_block)
            current_block = OrderedDict()
            current_block['FOLDER_NAME'] = folder_match.group(1)
            if parent_folder:
                current_block['PARENT_FOLDER'] = parent_folder
            current_type = 'FOLDER'
            continue
            
        if job_match:
            if current_block and current_type:
                result[current_type].append(current_block)
            current_block = OrderedDict()
            current_block['JOBNAME'] = job_match.group(1)
            if parent_folder:
                current_block['FOLDER_NAME'] = parent_folder
            current_type = 'JOB'
            continue
        
        # Parse attributes within blocks
        if current_block is not None:
            # Try different attribute patterns
            attr_match = re.match(r'^(\w+)\s+(.+)$', line)
            if attr_match:
                key = attr_match.group(1).upper()
                value = attr_match.group(2).strip()
                
                # Handle special cases for conditions
                if key in ('INCOND', 'IN-COND'):
                    if 'INCOND' not in result:
                        result['INCOND'] = []
                    cond_entry = OrderedDict()
                    cond_entry['JOB_NAME'] = current_block.get('JOBNAME', '')
                    cond_entry['CONDITION'] = value
                    result['INCOND'].append(cond_entry)
                elif key in ('OUTCOND', 'OUT-COND'):
                    if 'OUTCOND' not in result:
                        result['OUTCOND'] = []
                    cond_entry = OrderedDict()
                    cond_entry['JOB_NAME'] = current_block.get('JOBNAME', '')
                    cond_entry['CONDITION'] = value
                    result['OUTCOND'].append(cond_entry)
                else:
                    current_block[key] = value
    
    # Don't forget the last block
    if current_block and current_type:
        result[current_type].append(current_block)
    
    # Clean up empty lists
    result = {k: v for k, v in result.items() if v}
    
    return result


def parse_esp_format(content):
    """
    Parse CA ESP (CA Workload Automation ESP) format
    Extracts: MEMBER, APPL (Application), JOB, LINUX_JOB, NT_JOB, VARIABLES, etc.
    Also extracts cancelled jobs from comments and metadata.
    """
    result = {
        'MEMBER': [],           # MEMBER NAME blocks
        'APPLICATION': [],      # APPL definitions
        'JOB': [],              # All jobs (JOB, LINUX_JOB, NT_JOB)
        'CANCELLED_JOB': [],    # Cancelled/commented out jobs
        'VARIABLE': [],         # Variable definitions
        'DEPENDENCY': [],       # AFTER, RELEASE dependencies
        'SCHEDULE': [],         # RUN schedules
        'APPLEND': [],          # Application end markers
        'METADATA': [],         # CONTACT, OWNER, EVENT, CALENDAR info
    }
    
    current_member = None
    current_appl = None
    current_job = None
    current_proc_type = None
    in_job_section = False
    line_number = 0
    
    # Collect metadata for current member
    current_metadata = OrderedDict()
    
    lines = content.split('\n')
    total_lines = len(lines)
    
    # Progress tracking
    progress_interval = max(1, total_lines // 10)
    
    i = 0
    while i < len(lines):
        line = lines[i]
        original_line = line
        
        # Remove trailing 8-digit numbers (mainframe line numbers)
        line = re.sub(r'\s*\d{8}\s*$', '', line)
        line = line.strip()
        
        # Progress display
        if i % progress_interval == 0:
            print(f"  Parsing: {i:,}/{total_lines:,} lines ({100*i//total_lines}%)")
        
        i += 1
        line_number += 1
        
        # Skip empty lines
        if not line:
            continue
        
        # ========== Extract metadata from comments ==========
        if line.startswith('/*'):
            # Extract CONTACT info
            contact_match = re.search(r'CONTACT\s*:?\s*(.+?)(?:\*\/|$)', line, re.IGNORECASE)
            if contact_match:
                contact_info = contact_match.group(1).strip()
                if contact_info and current_member:
                    current_metadata['CONTACT'] = current_metadata.get('CONTACT', '') + ' ' + contact_info
            
            # Extract OWNER info
            owner_match = re.search(r'OWNER\s+(?:IS\s+)?(.+?)(?:TEL|$)', line, re.IGNORECASE)
            if owner_match:
                owner_info = owner_match.group(1).strip()
                if owner_info and current_member:
                    if 'OWNER' not in current_metadata:
                        current_metadata['OWNER'] = []
                    current_metadata['OWNER'].append(owner_info)
            
            # Extract EVENT info
            event_match = re.search(r'EVENT\s*:?\s*(\S+)', line, re.IGNORECASE)
            if event_match:
                current_metadata['EVENT'] = event_match.group(1).strip()
            
            # Extract CALENDAR info
            cal_match = re.search(r'CALENDAR\s*:?\s*(\S+)', line, re.IGNORECASE)
            if cal_match:
                current_metadata['CALENDAR'] = cal_match.group(1).strip()
            
            # Extract Agent info
            agent_match = re.search(r'Agent-?\d*\s*:?\s*(\w+)\s*:?\s*IP\.?(\S+)?', line, re.IGNORECASE)
            if agent_match:
                if 'AGENTS' not in current_metadata:
                    current_metadata['AGENTS'] = []
                agent_info = agent_match.group(1)
                if agent_match.group(2):
                    agent_info += f" (IP: {agent_match.group(2)})"
                current_metadata['AGENTS'].append(agent_info)
            
            # ========== Extract CANCELLED JOBS from comments ==========
            # Pattern: /*  JOB JOBNAME ... CAN dd/mm/yy */
            cancelled_match = re.search(r'/\*\s*(JOB|LINUX_JOB|NT_JOB)\s+(\S+).*?(CAN(?:CEL)?\s*(\d{2}/\d{2}/\d{2,4}))?', line, re.IGNORECASE)
            if cancelled_match and 'CAN' in line.upper():
                cancelled_job = OrderedDict()
                cancelled_job['JOB_NAME'] = cancelled_match.group(2)
                cancelled_job['JOB_TYPE'] = cancelled_match.group(1).upper()
                cancelled_job['MEMBER_NAME'] = current_member
                cancelled_job['APPL_NAME'] = current_appl
                
                # Extract cancel date
                can_date_match = re.search(r'CAN(?:CEL)?\s*(\d{2}/\d{2}/\d{2,4})', line, re.IGNORECASE)
                if can_date_match:
                    cancelled_job['CANCEL_DATE'] = can_date_match.group(1)
                
                # Try to extract other attributes from following comment lines
                j = i
                while j < len(lines) and j < i + 10:
                    next_line = re.sub(r'\s*\d{8}\s*$', '', lines[j]).strip()
                    if next_line.startswith('/*') and 'ENDJOB' not in next_line.upper():
                        # Extract MEMBER
                        member_match = re.search(r'MEMBER\s+(\S+)', next_line, re.IGNORECASE)
                        if member_match:
                            cancelled_job['MEMBER'] = member_match.group(1)
                        # Extract SUBAPPL
                        subappl_match = re.search(r'SUBAPPL\s+(\S+)', next_line, re.IGNORECASE)
                        if subappl_match:
                            cancelled_job['SUBAPPL'] = subappl_match.group(1)
                        # Extract RUN
                        run_match = re.search(r'RUN\s+(.+?)(?:\*\/|$)', next_line, re.IGNORECASE)
                        if run_match:
                            cancelled_job['RUN'] = run_match.group(1).strip()
                        # Extract AFTER
                        after_match = re.search(r'AFTER\s+(.+?)(?:\*\/|$)', next_line, re.IGNORECASE)
                        if after_match:
                            cancelled_job['AFTER'] = after_match.group(1).strip()
                        # Extract RELEASE
                        release_match = re.search(r'RELEASE\s+(.+?)(?:\*\/|$)', next_line, re.IGNORECASE)
                        if release_match:
                            cancelled_job['RELEASE'] = release_match.group(1).strip()
                    elif 'ENDJOB' in next_line.upper():
                        break
                    j += 1
                
                result['CANCELLED_JOB'].append(cancelled_job)
            
            continue
        
        # Handle continuation lines (ending with -)
        while line.endswith('-') and i < len(lines):
            next_line = re.sub(r'\s*\d{8}\s*$', '', lines[i]).strip()
            line = line[:-1] + next_line
            i += 1
            line_number += 1
        
        # ========== MEMBER NAME ==========
        member_match = re.match(r'^MEMBER\s+NAME\s+(\S+)', line, re.IGNORECASE)
        if member_match:
            # Save previous job if exists
            if current_job:
                result['JOB'].append(current_job)
                current_job = None
            
            # Save previous member's metadata
            if current_metadata and current_member:
                meta_entry = OrderedDict()
                meta_entry['MEMBER_NAME'] = current_member
                meta_entry['APPL_NAME'] = current_appl
                for key, value in current_metadata.items():
                    if isinstance(value, list):
                        meta_entry[key] = '; '.join(value)
                    else:
                        meta_entry[key] = str(value).strip()
                result['METADATA'].append(meta_entry)
            
            # Reset for new member
            current_member = member_match.group(1)
            current_metadata = OrderedDict()
            current_appl = None
            in_job_section = False
            
            member_entry = OrderedDict()
            member_entry['MEMBER_NAME'] = current_member
            member_entry['LINE_NUMBER'] = line_number
            result['MEMBER'].append(member_entry)
            continue
        
        # ========== PROC TYPE (xxxPROC:ESPPROC) ==========
        proc_match = re.match(r'^(\w+):ESPPROC', line, re.IGNORECASE)
        if proc_match:
            current_proc_type = proc_match.group(1)
            # Update the last member with proc type
            if result['MEMBER']:
                result['MEMBER'][-1]['PROC_TYPE'] = current_proc_type
            continue
        
        # ========== APPL (Application) ==========
        appl_match = re.match(r'^APPL\s+(\S+)(.*)?$', line, re.IGNORECASE)
        if appl_match:
            current_appl = appl_match.group(1)
            appl_options = appl_match.group(2).strip() if appl_match.group(2) else ''
            
            appl_entry = OrderedDict()
            appl_entry['APPL_NAME'] = current_appl
            appl_entry['MEMBER_NAME'] = current_member
            appl_entry['OPTIONS'] = appl_options
            result['APPLICATION'].append(appl_entry)
            continue
        
        # ========== JOB: section start ==========
        if re.match(r'^JOB:\s*$', line, re.IGNORECASE):
            in_job_section = True
            continue
        
        # ========== END: section end ==========
        if re.match(r'^END:\s*$', line, re.IGNORECASE):
            if current_job:
                result['JOB'].append(current_job)
                current_job = None
            in_job_section = False
            continue
        
        # ========== EXIT ==========
        if re.match(r'^EXIT\s*$', line, re.IGNORECASE):
            if current_job:
                result['JOB'].append(current_job)
                current_job = None
            continue
        
        # ========== STEPEXIT ==========
        if re.match(r'^STEPEXIT:', line, re.IGNORECASE):
            if current_job:
                result['JOB'].append(current_job)
                current_job = None
            continue
        
        # ========== Job definitions (JOB, LINUX_JOB, NT_JOB) ==========
        job_match = re.match(r'^(JOB|LINUX_JOB|NT_JOB|APPLEND)\s+(\S+)', line, re.IGNORECASE)
        if job_match:
            # Save previous job
            if current_job:
                result['JOB'].append(current_job)
            
            job_type = job_match.group(1).upper()
            job_name = job_match.group(2)
            
            current_job = OrderedDict()
            current_job['JOB_NAME'] = job_name
            current_job['JOB_TYPE'] = job_type
            current_job['MEMBER_NAME'] = current_member
            current_job['APPL_NAME'] = current_appl
            
            # APPLEND goes to separate sheet
            if job_type == 'APPLEND':
                applend_entry = OrderedDict()
                applend_entry['APPLEND_NAME'] = job_name
                applend_entry['MEMBER_NAME'] = current_member
                applend_entry['APPL_NAME'] = current_appl
                result['APPLEND'].append(applend_entry)
            continue
        
        # ========== ENDJOB ==========
        if re.match(r'^ENDJOB\s*$', line, re.IGNORECASE):
            if current_job:
                result['JOB'].append(current_job)
                current_job = None
            continue
        
        # ========== Variable definitions (VAR = value) ==========
        var_match = re.match(r'^([\w@#$]+)\s*=\s*[\'"]?(.+?)[\'"]?\s*$', line)
        if var_match and not in_job_section:
            var_entry = OrderedDict()
            var_entry['VAR_NAME'] = var_match.group(1)
            var_entry['VAR_VALUE'] = var_match.group(2)
            var_entry['MEMBER_NAME'] = current_member
            result['VARIABLE'].append(var_entry)
            continue
        
        # ========== Job attributes ==========
        if current_job is not None:
            # MEMBER attribute
            attr_match = re.match(r'^MEMBER\s+(\S+)', line, re.IGNORECASE)
            if attr_match:
                current_job['MEMBER'] = attr_match.group(1)
                continue
            
            # SUBAPPL attribute
            attr_match = re.match(r'^SUBAPPL\s+(\S+)', line, re.IGNORECASE)
            if attr_match:
                current_job['SUBAPPL'] = attr_match.group(1)
                continue
            
            # RUN schedule
            attr_match = re.match(r'^RUN\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['RUN'] = attr_match.group(1).strip()
                
                # Also add to SCHEDULE sheet
                sched_entry = OrderedDict()
                sched_entry['JOB_NAME'] = current_job.get('JOB_NAME', '')
                sched_entry['SCHEDULE'] = attr_match.group(1).strip()
                sched_entry['MEMBER_NAME'] = current_member
                result['SCHEDULE'].append(sched_entry)
                continue
            
            # DELAYSUB
            attr_match = re.match(r'^DELAYSUB\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['DELAYSUB'] = attr_match.group(1).strip()
                continue
            
            # AFTER dependencies
            attr_match = re.match(r'^AFTER\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                deps = attr_match.group(1).strip()
                current_job['AFTER'] = deps
                
                # Parse individual dependencies
                dep_list = re.findall(r'[\w@#$]+', deps)
                for dep in dep_list:
                    dep_entry = OrderedDict()
                    dep_entry['JOB_NAME'] = current_job.get('JOB_NAME', '')
                    dep_entry['DEPENDS_ON'] = dep
                    dep_entry['DEP_TYPE'] = 'AFTER'
                    dep_entry['MEMBER_NAME'] = current_member
                    result['DEPENDENCY'].append(dep_entry)
                continue
            
            # RELEASE
            attr_match = re.match(r'^RELEASE\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                releases = attr_match.group(1).strip()
                current_job['RELEASE'] = releases
                
                # Parse individual releases
                rel_list = re.findall(r'[\w@#$]+', releases)
                for rel in rel_list:
                    dep_entry = OrderedDict()
                    dep_entry['JOB_NAME'] = current_job.get('JOB_NAME', '')
                    dep_entry['RELEASES'] = rel
                    dep_entry['DEP_TYPE'] = 'RELEASE'
                    dep_entry['MEMBER_NAME'] = current_member
                    result['DEPENDENCY'].append(dep_entry)
                continue
            
            # AGENT
            attr_match = re.match(r'^AGENT\s+(\S+)', line, re.IGNORECASE)
            if attr_match:
                current_job['AGENT'] = attr_match.group(1)
                continue
            
            # SCRIPTNAME
            attr_match = re.match(r'^SCRIPTNAME\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['SCRIPTNAME'] = attr_match.group(1).strip()
                continue
            
            # CMDNAME
            attr_match = re.match(r'^CMDNAME\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['CMDNAME'] = attr_match.group(1).strip()
                continue
            
            # ARGS
            attr_match = re.match(r'^ARGS\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['ARGS'] = attr_match.group(1).strip()
                continue
            
            # USER
            attr_match = re.match(r'^USER\s+(\S+)', line, re.IGNORECASE)
            if attr_match:
                current_job['USER'] = attr_match.group(1)
                continue
            
            # CCFAIL
            attr_match = re.match(r'^CCFAIL\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['CCFAIL'] = attr_match.group(1).strip()
                continue
            
            # NOTIFY
            attr_match = re.match(r'^NOTIFY\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['NOTIFY'] = attr_match.group(1).strip()
                continue
            
            # NOTWITH
            attr_match = re.match(r'^NOTWITH\s+(.+)$', line, re.IGNORECASE)
            if attr_match:
                current_job['NOTWITH'] = attr_match.group(1).strip()
                continue
            
            # Generic attribute (KEY VALUE)
            generic_match = re.match(r'^([A-Z_][A-Z0-9_]*)\s+(.+)$', line, re.IGNORECASE)
            if generic_match:
                key = generic_match.group(1).upper()
                value = generic_match.group(2).strip()
                if key not in current_job:
                    current_job[key] = value
                continue
    
    # Save last job
    if current_job:
        result['JOB'].append(current_job)
    
    # Save last member's metadata
    if current_metadata and current_member:
        meta_entry = OrderedDict()
        meta_entry['MEMBER_NAME'] = current_member
        meta_entry['APPL_NAME'] = current_appl
        for key, value in current_metadata.items():
            if isinstance(value, list):
                meta_entry[key] = '; '.join(value)
            else:
                meta_entry[key] = str(value).strip()
        result['METADATA'].append(meta_entry)
    
    # Clean up empty lists
    result = {k: v for k, v in result.items() if v}
    
    # Print summary
    print(f"  Parsing complete: {total_lines:,} lines processed")
    print(f"  Summary:")
    for key, value in result.items():
        print(f"    - {key}: {len(value):,} records")
    
    return result


def parse_generic_format(content):
    """Parse generic mainframe job text format"""
    jobs = []
    current_job = None
    
    lines = content.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines and comments
        if not line or line.startswith('#') or line.startswith('/*') or line.startswith('//'):
            continue
        
        # Check for job start patterns
        job_match = re.match(r'^(?:JOB|JOBNAME|NAME)\s*[:\s=]\s*(\S+)', line, re.IGNORECASE)
        if job_match:
            if current_job:
                jobs.append(current_job)
            current_job = OrderedDict()
            current_job['JOB_NAME'] = job_match.group(1)
            continue
        
        # Parse key-value pairs
        if current_job is not None:
            # Try different separators
            for pattern in [r'^(\w+)\s*:\s*(.*)$', r'^(\w+)\s*=\s*(.*)$', r'^(\w+)\s+(.+)$']:
                kv_match = re.match(pattern, line)
                if kv_match:
                    key = kv_match.group(1).upper()
                    value = kv_match.group(2).strip().strip('"\'')
                    current_job[key] = value
                    break
    
    # Don't forget the last job
    if current_job:
        jobs.append(current_job)
    
    return {'JOBS': jobs}


def parse_multiline_values(content):
    """Handle multiline values enclosed in quotes or continuation characters"""
    # Replace continuation lines
    content = re.sub(r'\\\s*\n\s*', ' ', content)
    
    # Handle multiline quoted strings
    lines = []
    in_multiline = False
    current_line = ''
    
    for line in content.split('\n'):
        if in_multiline:
            current_line += ' ' + line.strip()
            if '"' in line or "'" in line:
                in_multiline = False
                lines.append(current_line)
                current_line = ''
        else:
            quote_count = line.count('"') + line.count("'")
            if quote_count % 2 == 1:  # Odd number of quotes
                in_multiline = True
                current_line = line
            else:
                lines.append(line)
    
    if current_line:
        lines.append(current_line)
    
    return '\n'.join(lines)


def parse_mainframe_text(content, format_type=None):
    """Main parsing function that routes to appropriate parser"""
    
    # Pre-process content (but not for ESP - it handles continuation itself)
    if format_type != 'esp':
        content = parse_multiline_values(content)
    
    # Auto-detect format if not specified
    if format_type is None:
        format_type = detect_format(content)
    
    print(f"Detected format: {format_type}")
    
    # Route to appropriate parser
    if format_type == 'jil':
        return parse_jil_format(content)
    elif format_type == 'esp':
        return parse_esp_format(content)
    elif format_type == 'control_m':
        return parse_control_m_format(content)
    else:
        return parse_generic_format(content)


def dict_to_dataframes(parsed_dict):
    """Convert parsed dictionary to list of DataFrames for Excel export"""
    df_list = []
    
    for sheet_name, records in parsed_dict.items():
        if records:  # Only create sheet if there are records
            df = pd.DataFrame(records)
            # Clean column names
            df.columns = df.columns.str.replace('@', '', regex=False)
            df.columns = df.columns.str.replace('-', '_', regex=False)
            df_list.append((sheet_name, df))
    
    return df_list


def load_text_file(file_path):
    """Load text file with multiple encoding attempts"""
    encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            print(f"File loaded successfully with encoding: {encoding}")
            return content
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"Error: File not found - {file_path}")
            return None
    
    # Last resort: replace errors
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        print("File loaded with error replacement")
        return content
    except Exception as e:
        print(f"Error loading file: {e}")
        return None


def create_excel_large(output_file, df_list, use_csv_fallback=False):
    """
    Create Excel file optimized for large datasets
    Uses xlsxwriter engine which is faster than openpyxl
    Falls back to CSV if data is too large
    """
    total_rows = sum(len(df) for _, df in df_list)
    print(f"\nTotal records to export: {total_rows:,}")
    
    # Check if we should use CSV instead
    if use_csv_fallback or total_rows > MAX_ROWS_PER_SHEET * 10:
        print("Data too large for Excel, exporting to CSV files...")
        return create_csv_files(output_file, df_list)
    
    try:
        # Use xlsxwriter engine - much faster for large files
        print(f"Creating Excel file with xlsxwriter engine...")
        start_time = datetime.now()
        
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            for sheet_name, df in df_list:
                if len(df) == 0:
                    continue
                    
                # Split into multiple sheets if too large
                if len(df) > MAX_ROWS_PER_SHEET:
                    num_parts = (len(df) // MAX_ROWS_PER_SHEET) + 1
                    print(f"  {sheet_name}: {len(df):,} rows -> splitting into {num_parts} sheets")
                    
                    for i in range(num_parts):
                        start_idx = i * MAX_ROWS_PER_SHEET
                        end_idx = min((i + 1) * MAX_ROWS_PER_SHEET, len(df))
                        part_df = df.iloc[start_idx:end_idx]
                        part_name = f"{sheet_name[:25]}_{i+1}" if len(sheet_name) > 25 else f"{sheet_name}_{i+1}"
                        part_df.to_excel(writer, sheet_name=part_name[:31], index=False)
                        print(f"    Part {i+1}: {len(part_df):,} rows")
                else:
                    print(f"  {sheet_name}: {len(df):,} rows")
                    # Truncate sheet name to 31 chars (Excel limit)
                    safe_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=safe_name, index=False)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\nExcel file created successfully in {elapsed:.1f} seconds")
        print(f"Output: {output_file}")
        return True
        
    except ImportError:
        print("xlsxwriter not installed, trying openpyxl...")
        return create_excel_openpyxl(output_file, df_list)
    except Exception as e:
        print(f"Excel creation failed: {e}")
        print("Falling back to CSV export...")
        return create_csv_files(output_file, df_list)


def create_excel_openpyxl(output_file, df_list):
    """
    Fallback to openpyxl with chunked writing for large data
    """
    try:
        print("Using openpyxl engine (slower for large files)...")
        start_time = datetime.now()
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet_name, df in df_list:
                if len(df) == 0:
                    continue
                
                # Limit rows for openpyxl to prevent memory issues
                if len(df) > 100000:
                    print(f"  Warning: {sheet_name} has {len(df):,} rows, limiting to 100,000")
                    df = df.head(100000)
                
                safe_name = sheet_name[:31]
                print(f"  Writing {sheet_name}: {len(df):,} rows...")
                df.to_excel(writer, sheet_name=safe_name, index=False)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\nExcel file created in {elapsed:.1f} seconds")
        return True
        
    except Exception as e:
        print(f"openpyxl failed: {e}")
        return False


def create_csv_files(base_output_file, df_list):
    """
    Export to multiple CSV files (fastest option for very large data)
    """
    base_name = os.path.splitext(base_output_file)[0]
    output_dir = os.path.dirname(base_output_file) or '.'
    
    print(f"\nExporting to CSV files...")
    start_time = datetime.now()
    created_files = []
    
    for sheet_name, df in df_list:
        if len(df) == 0:
            continue
        
        csv_file = os.path.join(output_dir, f"{base_name}_{sheet_name}.csv")
        print(f"  {sheet_name}: {len(df):,} rows -> {os.path.basename(csv_file)}")
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')  # utf-8-sig for Excel compatibility
        created_files.append(csv_file)
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nCSV files created in {elapsed:.1f} seconds")
    print(f"Created {len(created_files)} files")
    return True


def convert_text_to_excel(input_path, output_excel=None, output_json=None, format_type=None, output_format='excel'):
    """
    Main function to convert mainframe job text file to Excel/CSV
    
    Args:
        input_path: Path to input text file
        output_excel: Path for output Excel file (optional)
        output_json: Path for output JSON file (optional)
        format_type: Force specific format ('jil', 'control_m', 'generic') (optional)
        output_format: 'excel', 'csv', or 'auto' (auto will choose based on data size)
    
    Returns:
        Tuple of (parsed_dict, df_list) or None if error
    """
    # Load the text file
    print(f"\nLoading file: {input_path}")
    content = load_text_file(input_path)
    if content is None:
        return None
    
    print(f"File size: {len(content):,} characters")
    
    # Parse the content
    print("Parsing content...")
    parsed_dict = parse_mainframe_text(content, format_type)
    
    # Generate output filenames if not provided
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    if output_json is None:
        output_json = f"{base_name}_output.json"
    if output_excel is None:
        output_excel = f"{base_name}_output.xlsx"
    
    # Save JSON output
    print(f"\nSaving JSON: {output_json}")
    createJson(output_json, parsed_dict)
    
    # Convert to DataFrames
    df_list = dict_to_dataframes(parsed_dict)
    
    if not df_list:
        print("Warning: No data to export")
        return parsed_dict, df_list
    
    # Determine output format
    total_rows = sum(len(df) for _, df in df_list)
    
    if output_format == 'auto':
        if total_rows > 500000:
            output_format = 'csv'
            print(f"Auto-selected CSV format due to large data size ({total_rows:,} rows)")
        else:
            output_format = 'excel'
    
    # Export based on format
    if output_format == 'csv':
        create_csv_files(output_excel, df_list)
    else:
        create_excel_large(output_excel, df_list)
    
    return parsed_dict, df_list


def interactive_mode():
    """Interactive mode for command-line usage"""
    print("=" * 60)
    print("Mainframe Job Text to Excel/CSV Converter")
    print("=" * 60)
    print("\nSupported input formats:")
    print("  - Autosys JIL")
    print("  - CA ESP (CA Workload Automation)")
    print("  - BMC Control-M")
    print("  - Generic key-value format")
    print()
    
    # Get input file path
    input_path = input("Enter the path of the text file: ").strip()
    if not input_path:
        print("Error: No input file specified")
        return
    
    # Remove quotes if present
    input_path = input_path.strip('"\'')
    
    if not os.path.exists(input_path):
        print(f"Error: File not found - {input_path}")
        return
    
    # Show file size
    file_size = os.path.getsize(input_path)
    print(f"\nFile size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
    
    # Ask for input format type
    print("\nInput format options:")
    print("  1. Auto-detect (default)")
    print("  2. Autosys JIL")
    print("  3. CA ESP (CA Workload Automation)")
    print("  4. BMC Control-M")
    print("  5. Generic")
    
    format_choice = input("\nSelect input format (1-5) [1]: ").strip() or '1'
    format_map = {'1': None, '2': 'jil', '3': 'esp', '4': 'control_m', '5': 'generic'}
    format_type = format_map.get(format_choice)
    
    # Ask for output format
    print("\nOutput format options:")
    print("  1. Excel (.xlsx) - recommended for < 500K rows")
    print("  2. CSV files - fastest, best for large data")
    print("  3. Auto (choose based on data size)")
    
    output_choice = input("\nSelect output format (1-3) [3]: ").strip() or '3'
    output_format_map = {'1': 'excel', '2': 'csv', '3': 'auto'}
    output_format = output_format_map.get(output_choice, 'auto')
    
    # Ask for output file names (optional)
    output_excel = input("\nOutput filename (press Enter for default): ").strip() or None
    output_json = input("Output JSON filename (press Enter for default): ").strip() or None
    
    # Convert
    print("\n" + "-" * 60)
    result = convert_text_to_excel(input_path, output_excel, output_json, format_type, output_format)
    
    if result:
        parsed_dict, df_list = result
        print("\n" + "=" * 60)
        print("Conversion completed successfully!")
        print("=" * 60)
        
        # Print summary
        print("\nSummary:")
        total_records = 0
        for sheet_name, df in df_list:
            print(f"  - {sheet_name}: {len(df):,} records")
            total_records += len(df)
        print(f"\nTotal: {total_records:,} records")
    else:
        print("\nConversion failed!")


def main():
    """Main entry point"""
    if len(sys.argv) > 1:
        # Command-line mode
        input_path = sys.argv[1]
        output_excel = sys.argv[2] if len(sys.argv) > 2 else None
        output_json = sys.argv[3] if len(sys.argv) > 3 else None
        format_type = sys.argv[4] if len(sys.argv) > 4 else None
        
        convert_text_to_excel(input_path, output_excel, output_json, format_type)
    else:
        # Interactive mode
        interactive_mode()


if __name__ == '__main__':
    main()
