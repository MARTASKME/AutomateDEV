"""
Stonebranch Converter - Convert parsed ESP data to Stonebranch UAC format
"""

import re
import json
from collections import OrderedDict
from datetime import datetime
from config import (
    STONEBRANCH_TASK_TYPES,
    ESP_TO_CRON_MAPPING,
    DEFAULT_AGENT_MAPPING,
    DEFAULT_BUSINESS_SERVICE,
    DEFAULT_CREDENTIAL,
    DEFAULT_RUNTIME_DIR,
    DEFAULT_AGENT,
    WORKFLOW_PREFIX,
    TASK_PREFIX,
)


class StonebranchConverter:
    """Convert ESP jobs to Stonebranch UAC tasks and workflows"""
    
    def __init__(self, esp_data, agent_mapping=None):
        """
        Initialize converter
        
        Args:
            esp_data: Parsed ESP data from ESPParser
            agent_mapping: Dict mapping ESP agents to Stonebranch agents
        """
        self.esp_data = esp_data
        self.agent_mapping = agent_mapping or DEFAULT_AGENT_MAPPING
        
        self.tasks = []
        self.workflows = []
        self.triggers = []
        self.variables = []
        self.credentials = []
    
    def convert(self):
        """
        Convert all ESP data to Stonebranch format
        
        Returns:
            dict: Converted data with tasks, workflows, triggers
        """
        print("\nConverting to Stonebranch format...")
        
        # Convert jobs to tasks
        if 'JOB' in self.esp_data:
            self._convert_jobs()
        
        # Convert applications to workflows
        if 'APPLICATION' in self.esp_data:
            self._convert_applications()
        
        # Convert variables
        if 'VARIABLE' in self.esp_data:
            self._convert_variables()
        
        # Create triggers from schedules
        if 'SCHEDULE' in self.esp_data:
            self._create_triggers()
        
        result = {
            'tasks': self.tasks,
            'workflows': self.workflows,
            'triggers': self.triggers,
            'variables': self.variables,
        }
        
        self._print_summary()
        return result
    
    def _convert_jobs(self):
        """Convert ESP jobs to Stonebranch tasks"""
        jobs = self.esp_data.get('JOB', [])
        dependencies = self.esp_data.get('DEPENDENCY', [])
        
        # Build dependency map
        dep_map = {}
        for dep in dependencies:
            job_name = dep.get('JOB_NAME', '')
            if job_name not in dep_map:
                dep_map[job_name] = {'after': [], 'release': []}
            
            if dep.get('DEP_TYPE') == 'AFTER':
                dep_map[job_name]['after'].append(dep.get('DEPENDS_ON', ''))
            elif dep.get('DEP_TYPE') == 'RELEASE':
                dep_map[job_name]['release'].append(dep.get('RELEASES', ''))
        
        for job in jobs:
            task = self._create_task(job, dep_map)
            self.tasks.append(task)
        
        print(f"  Converted {len(self.tasks)} jobs to tasks")
    
    def _create_task(self, job, dep_map):
        """Create Stonebranch task from ESP job"""
        job_name = job.get('JOB_NAME', '')
        job_type = job.get('JOB_TYPE', 'JOB')
        
        # Determine task type
        task_type = STONEBRANCH_TASK_TYPES.get(job_type, 'taskUnix')
        
        # Build task
        task = OrderedDict()
        task['type'] = task_type
        task['name'] = self._sanitize_name(job_name)
        task['summary'] = f"Migrated from ESP: {job.get('MEMBER_NAME', '')} / {job.get('APPL_NAME', '')}"
        
        # Business service
        task['opswiseGroups'] = [DEFAULT_BUSINESS_SERVICE]
        
        # Agent
        esp_agent = job.get('AGENT', '')
        task['agent'] = self.agent_mapping.get(esp_agent, esp_agent or DEFAULT_AGENT)
        
        # Credentials
        if job.get('USER'):
            task['credentials'] = job.get('USER')
        
        # Command/Script
        if task_type == 'taskLinux' or task_type == 'taskUnix':
            task['command'] = self._build_command(job)
        elif task_type == 'taskWindows':
            task['command'] = self._build_windows_command(job)
        
        # Runtime directory
        if job.get('SCRIPTNAME'):
            script_dir = '/'.join(job.get('SCRIPTNAME', '').split('/')[:-1])
            if script_dir:
                task['runtimeDir'] = script_dir
        
        # Add original ESP attributes as custom fields
        task['espAttributes'] = {
            'MEMBER_NAME': job.get('MEMBER_NAME', ''),
            'APPL_NAME': job.get('APPL_NAME', ''),
            'PROC_TYPE': job.get('PROC_TYPE', ''),
            'SUBAPPL': job.get('SUBAPPL', ''),
            'RUN': job.get('RUN', ''),
            'DELAYSUB': job.get('DELAYSUB', ''),
            'AFTER': job.get('AFTER', ''),
            'RELEASE': job.get('RELEASE', ''),
        }
        
        # Dependencies (for reference)
        if job_name in dep_map:
            task['espDependencies'] = dep_map[job_name]
        
        return task
    
    def _build_command(self, job):
        """Build Unix/Linux command from job"""
        scriptname = job.get('SCRIPTNAME', '')
        cmdname = job.get('CMDNAME', '')
        args = job.get('ARGS', '')
        
        if scriptname:
            cmd = scriptname
            if args:
                cmd += f" {args}"
            return cmd
        elif cmdname:
            cmd = cmdname
            if args:
                cmd += f" {args}"
            return cmd
        
        return ""
    
    def _build_windows_command(self, job):
        """Build Windows command from job"""
        cmdname = job.get('CMDNAME', '')
        args = job.get('ARGS', '')
        
        if cmdname:
            cmd = cmdname
            if args:
                cmd += f" {args}"
            return cmd
        
        return ""
    
    def _convert_applications(self):
        """Convert ESP applications to Stonebranch workflows"""
        applications = self.esp_data.get('APPLICATION', [])
        jobs = self.esp_data.get('JOB', [])
        dependencies = self.esp_data.get('DEPENDENCY', [])
        
        for appl in applications:
            workflow = self._create_workflow(appl, jobs, dependencies)
            self.workflows.append(workflow)
        
        print(f"  Converted {len(self.workflows)} applications to workflows")
    
    def _create_workflow(self, appl, all_jobs, all_deps):
        """Create Stonebranch workflow from ESP application"""
        appl_name = appl.get('APPL_NAME', '')
        member_name = appl.get('MEMBER_NAME', '')
        
        # Find jobs in this application
        appl_jobs = [j for j in all_jobs if j.get('APPL_NAME') == appl_name]
        
        # Build workflow
        workflow = OrderedDict()
        workflow['type'] = 'taskWorkflow'
        workflow['name'] = f"{WORKFLOW_PREFIX}{self._sanitize_name(appl_name)}"
        workflow['summary'] = f"Workflow for {appl_name} (Member: {member_name})"
        workflow['opswiseGroups'] = [DEFAULT_BUSINESS_SERVICE]
        
        # Build workflow vertices (tasks)
        vertices = []
        for i, job in enumerate(appl_jobs):
            vertex = {
                'task': {
                    'value': self._sanitize_name(job.get('JOB_NAME', ''))
                },
                'vertexX': 100 + (i % 5) * 200,
                'vertexY': 100 + (i // 5) * 150,
            }
            vertices.append(vertex)
        
        workflow['workflowVertices'] = vertices
        
        # Build workflow edges (dependencies)
        edges = []
        appl_job_names = {j.get('JOB_NAME', '') for j in appl_jobs}
        
        for dep in all_deps:
            job_name = dep.get('JOB_NAME', '')
            depends_on = dep.get('DEPENDS_ON', '')
            
            # Only include dependencies within this application
            if job_name in appl_job_names and depends_on in appl_job_names:
                edge = {
                    'sourceId': {
                        'value': self._sanitize_name(depends_on)
                    },
                    'targetId': {
                        'value': self._sanitize_name(job_name)
                    },
                    'condition': {
                        'value': 'Success'
                    }
                }
                edges.append(edge)
        
        workflow['workflowEdges'] = edges
        
        # ESP attributes
        workflow['espAttributes'] = {
            'MEMBER_NAME': member_name,
            'APPL_NAME': appl_name,
            'OPTIONS': appl.get('OPTIONS', ''),
            'PROC_TYPE': appl.get('PROC_TYPE', ''),
        }
        
        return workflow
    
    def _convert_variables(self):
        """Convert ESP variables to Stonebranch variables"""
        esp_vars = self.esp_data.get('VARIABLE', [])
        
        for var in esp_vars:
            stonebranch_var = {
                'name': var.get('VAR_NAME', ''),
                'value': var.get('VAR_VALUE', ''),
                'description': f"Migrated from ESP: {var.get('MEMBER_NAME', '')}",
            }
            self.variables.append(stonebranch_var)
        
        print(f"  Converted {len(self.variables)} variables")
    
    def _create_triggers(self):
        """Create Stonebranch triggers from ESP schedules"""
        schedules = self.esp_data.get('SCHEDULE', [])
        
        # Group by schedule to avoid duplicates
        schedule_map = {}
        for sched in schedules:
            schedule_str = sched.get('SCHEDULE', '')
            if schedule_str not in schedule_map:
                schedule_map[schedule_str] = []
            schedule_map[schedule_str].append(sched.get('JOB_NAME', ''))
        
        for schedule_str, job_names in schedule_map.items():
            trigger = self._create_trigger(schedule_str, job_names)
            if trigger:
                self.triggers.append(trigger)
        
        print(f"  Created {len(self.triggers)} triggers")
    
    def _create_trigger(self, schedule_str, job_names):
        """Create Stonebranch trigger from ESP schedule"""
        # Try to map to cron expression
        schedule_upper = schedule_str.upper().strip()
        cron = ESP_TO_CRON_MAPPING.get(schedule_upper)
        
        if not cron:
            # Try partial match
            for key, value in ESP_TO_CRON_MAPPING.items():
                if key in schedule_upper:
                    cron = value
                    break
        
        if not cron:
            # Default to midnight
            cron = '0 0 * * *'
        
        trigger = {
            'type': 'triggerTime',
            'name': f"TRIG_{self._sanitize_name(schedule_str[:30])}",
            'summary': f"Schedule: {schedule_str}",
            'time': {
                'type': 'cron',
                'cron': cron,
            },
            'espSchedule': schedule_str,
            'espJobs': job_names[:10],  # Limit for readability
        }
        
        return trigger
    
    def _sanitize_name(self, name):
        """Sanitize name for Stonebranch"""
        if not name:
            return "UNNAMED"
        
        # Replace special characters
        name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
        
        # Ensure starts with letter or underscore
        if name and name[0].isdigit():
            name = '_' + name
        
        return name[:64]  # Max length
    
    def _print_summary(self):
        """Print conversion summary"""
        print("\nConversion Summary:")
        print(f"  Tasks: {len(self.tasks)}")
        print(f"  Workflows: {len(self.workflows)}")
        print(f"  Triggers: {len(self.triggers)}")
        print(f"  Variables: {len(self.variables)}")
    
    def to_json(self, indent=2):
        """Export to JSON"""
        return json.dumps({
            'tasks': self.tasks,
            'workflows': self.workflows,
            'triggers': self.triggers,
            'variables': self.variables,
        }, indent=indent, default=str)
    
    def save_json(self, output_path):
        """Save to JSON file"""
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(self.to_json())
        print(f"\nSaved to: {output_path}")


def convert_esp_to_stonebranch(esp_data, agent_mapping=None):
    """Convenience function to convert ESP data to Stonebranch"""
    converter = StonebranchConverter(esp_data, agent_mapping)
    return converter.convert()
