"""
Workflow Builder - Build Stonebranch workflows with proper dependencies
"""

import json
from collections import OrderedDict, defaultdict


class WorkflowBuilder:
    """Build Stonebranch workflow definitions with dependency handling"""
    
    def __init__(self, name, summary=""):
        """
        Initialize workflow builder
        
        Args:
            name: Workflow name
            summary: Workflow description
        """
        self.name = name
        self.summary = summary
        self.tasks = OrderedDict()  # task_name -> task_config
        self.dependencies = []  # list of (source, target, condition)
        self.variables = {}
    
    def add_task(self, task_name, task_type='taskLinux', **kwargs):
        """
        Add task to workflow
        
        Args:
            task_name: Name of the task
            task_type: Stonebranch task type
            **kwargs: Additional task properties
        """
        self.tasks[task_name] = {
            'name': task_name,
            'type': task_type,
            **kwargs
        }
        return self
    
    def add_dependency(self, source_task, target_task, condition='Success'):
        """
        Add dependency between tasks
        
        Args:
            source_task: Predecessor task name
            target_task: Successor task name
            condition: Dependency condition (Success, Failed, Finished, etc.)
        """
        self.dependencies.append({
            'source': source_task,
            'target': target_task,
            'condition': condition
        })
        return self
    
    def add_variable(self, name, value, description=""):
        """Add workflow variable"""
        self.variables[name] = {
            'name': name,
            'value': value,
            'description': description
        }
        return self
    
    def build(self):
        """
        Build workflow definition
        
        Returns:
            dict: Stonebranch workflow definition
        """
        workflow = OrderedDict()
        workflow['type'] = 'taskWorkflow'
        workflow['name'] = self.name
        workflow['summary'] = self.summary
        
        # Calculate positions for tasks
        positions = self._calculate_positions()
        
        # Build vertices
        vertices = []
        for task_name, task_config in self.tasks.items():
            pos = positions.get(task_name, (100, 100))
            vertex = {
                'task': {
                    'value': task_name
                },
                'vertexX': pos[0],
                'vertexY': pos[1],
            }
            vertices.append(vertex)
        
        workflow['workflowVertices'] = vertices
        
        # Build edges
        edges = []
        for dep in self.dependencies:
            edge = {
                'sourceId': {
                    'value': dep['source']
                },
                'targetId': {
                    'value': dep['target']
                },
                'condition': {
                    'value': dep['condition']
                }
            }
            edges.append(edge)
        
        workflow['workflowEdges'] = edges
        
        # Variables
        if self.variables:
            workflow['variables'] = list(self.variables.values())
        
        return workflow
    
    def _calculate_positions(self):
        """Calculate visual positions for tasks based on dependencies"""
        # Build adjacency list
        successors = defaultdict(list)
        predecessors = defaultdict(list)
        
        for dep in self.dependencies:
            successors[dep['source']].append(dep['target'])
            predecessors[dep['target']].append(dep['source'])
        
        # Find start tasks (no predecessors)
        all_tasks = set(self.tasks.keys())
        start_tasks = [t for t in all_tasks if t not in predecessors]
        
        if not start_tasks:
            start_tasks = list(all_tasks)[:1]  # Fallback
        
        # BFS to assign levels
        levels = {}
        queue = [(t, 0) for t in start_tasks]
        visited = set()
        
        while queue:
            task, level = queue.pop(0)
            if task in visited:
                continue
            visited.add(task)
            levels[task] = max(levels.get(task, 0), level)
            
            for succ in successors.get(task, []):
                queue.append((succ, level + 1))
        
        # Assign remaining tasks
        for task in all_tasks:
            if task not in levels:
                levels[task] = 0
        
        # Group by level
        level_groups = defaultdict(list)
        for task, level in levels.items():
            level_groups[level].append(task)
        
        # Calculate positions
        positions = {}
        x_spacing = 200
        y_spacing = 150
        
        for level, tasks in level_groups.items():
            x = 100 + level * x_spacing
            for i, task in enumerate(tasks):
                y = 100 + i * y_spacing
                positions[task] = (x, y)
        
        return positions
    
    def to_json(self, indent=2):
        """Export to JSON"""
        return json.dumps(self.build(), indent=indent, default=str)


class WorkflowBuilderFromESP:
    """Build Stonebranch workflows from parsed ESP data"""
    
    def __init__(self, esp_data):
        """
        Initialize with parsed ESP data
        
        Args:
            esp_data: Output from ESPParser.parse()
        """
        self.esp_data = esp_data
        self.workflows = []
    
    def build_all(self):
        """Build workflows for all applications"""
        applications = self.esp_data.get('APPLICATION', [])
        jobs = self.esp_data.get('JOB', [])
        dependencies = self.esp_data.get('DEPENDENCY', [])
        
        for appl in applications:
            workflow = self._build_workflow(appl, jobs, dependencies)
            if workflow:
                self.workflows.append(workflow)
        
        return self.workflows
    
    def _build_workflow(self, appl, all_jobs, all_deps):
        """Build single workflow from application"""
        appl_name = appl.get('APPL_NAME', '')
        member_name = appl.get('MEMBER_NAME', '')
        
        # Find jobs in this application
        appl_jobs = [j for j in all_jobs if j.get('APPL_NAME') == appl_name]
        
        if not appl_jobs:
            return None
        
        builder = WorkflowBuilder(
            name=f"WF_{appl_name}",
            summary=f"Workflow for {appl_name} (Member: {member_name})"
        )
        
        # Add tasks
        for job in appl_jobs:
            job_name = job.get('JOB_NAME', '')
            job_type = job.get('JOB_TYPE', 'JOB')
            
            task_type = {
                'JOB': 'taskUnix',
                'LINUX_JOB': 'taskLinux',
                'NT_JOB': 'taskWindows',
                'APPLEND': 'taskManual'
            }.get(job_type, 'taskUnix')
            
            builder.add_task(
                job_name,
                task_type=task_type,
                agent=job.get('AGENT', ''),
                command=job.get('SCRIPTNAME', '') or job.get('CMDNAME', ''),
                args=job.get('ARGS', ''),
            )
        
        # Add dependencies
        appl_job_names = {j.get('JOB_NAME', '') for j in appl_jobs}
        
        for dep in all_deps:
            if dep.get('DEP_TYPE') != 'AFTER':
                continue
            
            job_name = dep.get('JOB_NAME', '')
            depends_on = dep.get('DEPENDS_ON', '')
            
            if job_name in appl_job_names and depends_on in appl_job_names:
                builder.add_dependency(depends_on, job_name)
        
        return builder.build()
    
    def to_json(self, indent=2):
        """Export all workflows to JSON"""
        return json.dumps(self.workflows, indent=indent, default=str)
    
    def save_json(self, output_path):
        """Save workflows to JSON file"""
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(self.to_json())
        print(f"Saved {len(self.workflows)} workflows to: {output_path}")
