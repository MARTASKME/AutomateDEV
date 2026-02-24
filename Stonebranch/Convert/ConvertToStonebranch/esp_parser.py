"""
ESP Parser - Parse CA ESP (Workload Automation ESP) format files
"""

import re
import os
from collections import OrderedDict
from datetime import datetime


class ESPParser:
    """Parser for CA ESP mainframe job scheduler format"""
    
    def __init__(self, content=None, file_path=None):
        """
        Initialize parser with content or file path
        
        Args:
            content: Raw text content to parse
            file_path: Path to file to load and parse
        """
        self.content = content
        self.file_path = file_path
        self.result = None
        
        if file_path and not content:
            self.content = self._load_file(file_path)
    
    def _load_file(self, file_path):
        """Load file with multiple encoding attempts"""
        encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    return f.read()
            except UnicodeDecodeError:
                continue
            except FileNotFoundError:
                raise FileNotFoundError(f"File not found: {file_path}")
        
        # Last resort
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read()
    
    def parse(self):
        """
        Parse ESP content and return structured data
        
        Returns:
            dict: Parsed data with MEMBER, APPLICATION, JOB, etc.
        """
        if not self.content:
            raise ValueError("No content to parse")
        
        self.result = {
            'MEMBER': [],
            'APPLICATION': [],
            'JOB': [],
            'CANCELLED_JOB': [],
            'VARIABLE': [],
            'DEPENDENCY': [],
            'SCHEDULE': [],
            'APPLEND': [],
            'METADATA': [],
        }
        
        current_member = None
        current_appl = None
        current_job = None
        current_proc_type = None
        in_job_section = False
        current_metadata = OrderedDict()
        
        lines = self.content.split('\n')
        total_lines = len(lines)
        
        print(f"Parsing {total_lines:,} lines...")
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Remove trailing 8-digit numbers
            line = re.sub(r'\s*\d{8}\s*$', '', line).strip()
            
            i += 1
            
            if not line:
                continue
            
            # ===== Comments and Metadata =====
            if line.startswith('/*'):
                # Extract metadata
                self._extract_metadata(line, current_metadata)
                
                # Extract cancelled jobs
                if 'CAN' in line.upper():
                    cancelled = self._extract_cancelled_job(line, lines, i, current_member, current_appl)
                    if cancelled:
                        self.result['CANCELLED_JOB'].append(cancelled)
                continue
            
            # Handle continuation lines
            while line.endswith('-') and i < len(lines):
                next_line = re.sub(r'\s*\d{8}\s*$', '', lines[i]).strip()
                line = line[:-1] + next_line
                i += 1
            
            # ===== MEMBER NAME =====
            member_match = re.match(r'^MEMBER\s+NAME\s+(\S+)', line, re.IGNORECASE)
            if member_match:
                # Save previous
                if current_job:
                    self.result['JOB'].append(current_job)
                    current_job = None
                if current_metadata and current_member:
                    self._save_metadata(current_metadata, current_member, current_appl)
                
                current_member = member_match.group(1)
                current_metadata = OrderedDict()
                current_appl = None
                in_job_section = False
                
                self.result['MEMBER'].append({
                    'MEMBER_NAME': current_member,
                    'PROC_TYPE': None
                })
                continue
            
            # ===== PROC TYPE =====
            proc_match = re.match(r'^(\w+):ESPPROC', line, re.IGNORECASE)
            if proc_match:
                current_proc_type = proc_match.group(1)
                if self.result['MEMBER']:
                    self.result['MEMBER'][-1]['PROC_TYPE'] = current_proc_type
                continue
            
            # ===== APPL =====
            appl_match = re.match(r'^APPL\s+(\S+)(.*)?$', line, re.IGNORECASE)
            if appl_match:
                current_appl = appl_match.group(1)
                options = appl_match.group(2).strip() if appl_match.group(2) else ''
                
                self.result['APPLICATION'].append({
                    'APPL_NAME': current_appl,
                    'MEMBER_NAME': current_member,
                    'OPTIONS': options,
                    'PROC_TYPE': current_proc_type
                })
                continue
            
            # ===== JOB: section =====
            if re.match(r'^JOB:\s*$', line, re.IGNORECASE):
                in_job_section = True
                continue
            
            # ===== END: =====
            if re.match(r'^END:\s*$', line, re.IGNORECASE):
                if current_job:
                    self.result['JOB'].append(current_job)
                    current_job = None
                in_job_section = False
                continue
            
            # ===== EXIT =====
            if re.match(r'^EXIT\s*$', line, re.IGNORECASE):
                if current_job:
                    self.result['JOB'].append(current_job)
                    current_job = None
                continue
            
            # ===== Job definitions =====
            job_match = re.match(r'^(JOB|LINUX_JOB|NT_JOB|APPLEND)\s+(\S+)', line, re.IGNORECASE)
            if job_match:
                if current_job:
                    self.result['JOB'].append(current_job)
                
                job_type = job_match.group(1).upper()
                job_name = job_match.group(2)
                
                current_job = OrderedDict()
                current_job['JOB_NAME'] = job_name
                current_job['JOB_TYPE'] = job_type
                current_job['MEMBER_NAME'] = current_member
                current_job['APPL_NAME'] = current_appl
                current_job['PROC_TYPE'] = current_proc_type
                
                if job_type == 'APPLEND':
                    self.result['APPLEND'].append({
                        'APPLEND_NAME': job_name,
                        'MEMBER_NAME': current_member,
                        'APPL_NAME': current_appl
                    })
                continue
            
            # ===== ENDJOB =====
            if re.match(r'^ENDJOB\s*$', line, re.IGNORECASE):
                if current_job:
                    self.result['JOB'].append(current_job)
                    current_job = None
                continue
            
            # ===== Variables =====
            var_match = re.match(r'^([\w@#$]+)\s*=\s*[\'"]?(.+?)[\'"]?\s*$', line)
            if var_match and not in_job_section and current_job is None:
                self.result['VARIABLE'].append({
                    'VAR_NAME': var_match.group(1),
                    'VAR_VALUE': var_match.group(2),
                    'MEMBER_NAME': current_member
                })
                continue
            
            # ===== Job attributes =====
            if current_job is not None:
                self._parse_job_attribute(line, current_job, current_member)
        
        # Save final items
        if current_job:
            self.result['JOB'].append(current_job)
        if current_metadata and current_member:
            self._save_metadata(current_metadata, current_member, current_appl)
        
        # Clean up empty lists
        self.result = {k: v for k, v in self.result.items() if v}
        
        self._print_summary()
        return self.result
    
    def _extract_metadata(self, line, metadata):
        """Extract metadata from comment lines"""
        # CONTACT
        match = re.search(r'CONTACT\s*:?\s*(.+?)(?:\*\/|$)', line, re.IGNORECASE)
        if match:
            metadata['CONTACT'] = metadata.get('CONTACT', '') + ' ' + match.group(1).strip()
        
        # OWNER
        match = re.search(r'OWNER\s+(?:IS\s+)?(.+?)(?:TEL|$)', line, re.IGNORECASE)
        if match:
            if 'OWNER' not in metadata:
                metadata['OWNER'] = []
            metadata['OWNER'].append(match.group(1).strip())
        
        # EVENT
        match = re.search(r'EVENT\s*:?\s*(\S+)', line, re.IGNORECASE)
        if match:
            metadata['EVENT'] = match.group(1).strip()
        
        # CALENDAR
        match = re.search(r'CALENDAR\s*:?\s*(\S+)', line, re.IGNORECASE)
        if match:
            metadata['CALENDAR'] = match.group(1).strip()
        
        # AGENT
        match = re.search(r'Agent-?\d*\s*:?\s*(\w+)', line, re.IGNORECASE)
        if match:
            if 'AGENTS' not in metadata:
                metadata['AGENTS'] = []
            metadata['AGENTS'].append(match.group(1))
    
    def _extract_cancelled_job(self, line, lines, i, current_member, current_appl):
        """Extract cancelled job from comment"""
        match = re.search(r'/\*\s*(JOB|LINUX_JOB|NT_JOB)\s+(\S+)', line, re.IGNORECASE)
        if not match:
            return None
        
        job = OrderedDict()
        job['JOB_NAME'] = match.group(2)
        job['JOB_TYPE'] = match.group(1).upper()
        job['MEMBER_NAME'] = current_member
        job['APPL_NAME'] = current_appl
        
        # Extract cancel date
        date_match = re.search(r'CAN(?:CEL)?\s*(\d{2}/\d{2}/\d{2,4})', line, re.IGNORECASE)
        if date_match:
            job['CANCEL_DATE'] = date_match.group(1)
        
        return job
    
    def _save_metadata(self, metadata, member_name, appl_name):
        """Save metadata entry"""
        if not metadata:
            return
        
        entry = OrderedDict()
        entry['MEMBER_NAME'] = member_name
        entry['APPL_NAME'] = appl_name
        
        for key, value in metadata.items():
            if isinstance(value, list):
                entry[key] = '; '.join(value)
            else:
                entry[key] = str(value).strip()
        
        self.result['METADATA'].append(entry)
    
    def _parse_job_attribute(self, line, job, current_member):
        """Parse job attribute from line"""
        # MEMBER
        match = re.match(r'^MEMBER\s+(\S+)', line, re.IGNORECASE)
        if match:
            job['MEMBER'] = match.group(1)
            return
        
        # SUBAPPL
        match = re.match(r'^SUBAPPL\s+(\S+)', line, re.IGNORECASE)
        if match:
            job['SUBAPPL'] = match.group(1)
            return
        
        # RUN
        match = re.match(r'^RUN\s+(.+)$', line, re.IGNORECASE)
        if match:
            job['RUN'] = match.group(1).strip()
            self.result['SCHEDULE'].append({
                'JOB_NAME': job.get('JOB_NAME', ''),
                'SCHEDULE': match.group(1).strip(),
                'MEMBER_NAME': current_member
            })
            return
        
        # DELAYSUB
        match = re.match(r'^DELAYSUB\s+(.+)$', line, re.IGNORECASE)
        if match:
            job['DELAYSUB'] = match.group(1).strip()
            return
        
        # AFTER
        match = re.match(r'^AFTER\s+(.+)$', line, re.IGNORECASE)
        if match:
            deps = match.group(1).strip()
            job['AFTER'] = deps
            
            for dep in re.findall(r'[\w@#$]+', deps):
                self.result['DEPENDENCY'].append({
                    'JOB_NAME': job.get('JOB_NAME', ''),
                    'DEPENDS_ON': dep,
                    'DEP_TYPE': 'AFTER',
                    'MEMBER_NAME': current_member
                })
            return
        
        # RELEASE
        match = re.match(r'^RELEASE\s+(.+)$', line, re.IGNORECASE)
        if match:
            releases = match.group(1).strip()
            job['RELEASE'] = releases
            
            for rel in re.findall(r'[\w@#$]+', releases):
                self.result['DEPENDENCY'].append({
                    'JOB_NAME': job.get('JOB_NAME', ''),
                    'RELEASES': rel,
                    'DEP_TYPE': 'RELEASE',
                    'MEMBER_NAME': current_member
                })
            return
        
        # AGENT
        match = re.match(r'^AGENT\s+(\S+)', line, re.IGNORECASE)
        if match:
            job['AGENT'] = match.group(1)
            return
        
        # SCRIPTNAME
        match = re.match(r'^SCRIPTNAME\s+(.+)$', line, re.IGNORECASE)
        if match:
            job['SCRIPTNAME'] = match.group(1).strip()
            return
        
        # CMDNAME
        match = re.match(r'^CMDNAME\s+(.+)$', line, re.IGNORECASE)
        if match:
            job['CMDNAME'] = match.group(1).strip()
            return
        
        # ARGS
        match = re.match(r'^ARGS\s+(.+)$', line, re.IGNORECASE)
        if match:
            job['ARGS'] = match.group(1).strip()
            return
        
        # USER
        match = re.match(r'^USER\s+(\S+)', line, re.IGNORECASE)
        if match:
            job['USER'] = match.group(1)
            return
        
        # Generic attribute
        match = re.match(r'^([A-Z_][A-Z0-9_]*)\s+(.+)$', line, re.IGNORECASE)
        if match:
            key = match.group(1).upper()
            value = match.group(2).strip()
            if key not in job:
                job[key] = value
    
    def _print_summary(self):
        """Print parsing summary"""
        print("\nParsing Summary:")
        for key, value in self.result.items():
            print(f"  {key}: {len(value):,} records")


def parse_esp_file(file_path):
    """Convenience function to parse ESP file"""
    parser = ESPParser(file_path=file_path)
    return parser.parse()


def parse_esp_content(content):
    """Convenience function to parse ESP content"""
    parser = ESPParser(content=content)
    return parser.parse()
