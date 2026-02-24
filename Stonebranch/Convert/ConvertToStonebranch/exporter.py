"""
Exporter - Export converted data to various formats
"""

import os
import json
import csv
from datetime import datetime


class Exporter:
    """Export conversion results to various formats"""
    
    def __init__(self, converted_data, output_dir="."):
        """
        Initialize exporter
        
        Args:
            converted_data: Output from StonebranchConverter
            output_dir: Directory for output files
        """
        self.data = converted_data
        self.output_dir = output_dir
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def export_all(self, base_name="conversion"):
        """Export all formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # JSON exports
        self.export_json(f"{base_name}_{timestamp}.json")
        self.export_tasks_json(f"{base_name}_tasks_{timestamp}.json")
        self.export_workflows_json(f"{base_name}_workflows_{timestamp}.json")
        
        # CSV exports
        self.export_tasks_csv(f"{base_name}_tasks_{timestamp}.csv")
        self.export_workflows_csv(f"{base_name}_workflows_{timestamp}.csv")
        
        print(f"\nAll files exported to: {self.output_dir}")
    
    def export_json(self, filename):
        """Export all data to single JSON file"""
        path = os.path.join(self.output_dir, filename)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, default=str)
        print(f"  Exported: {filename}")
        return path
    
    def export_tasks_json(self, filename):
        """Export tasks to JSON file"""
        path = os.path.join(self.output_dir, filename)
        tasks = self.data.get('tasks', [])
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(tasks, f, indent=2, default=str)
        print(f"  Exported: {filename} ({len(tasks)} tasks)")
        return path
    
    def export_workflows_json(self, filename):
        """Export workflows to JSON file"""
        path = os.path.join(self.output_dir, filename)
        workflows = self.data.get('workflows', [])
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(workflows, f, indent=2, default=str)
        print(f"  Exported: {filename} ({len(workflows)} workflows)")
        return path
    
    def export_tasks_csv(self, filename):
        """Export tasks to CSV file"""
        path = os.path.join(self.output_dir, filename)
        tasks = self.data.get('tasks', [])
        
        if not tasks:
            return None
        
        # Flatten task data for CSV
        rows = []
        for task in tasks:
            row = {
                'name': task.get('name', ''),
                'type': task.get('type', ''),
                'summary': task.get('summary', ''),
                'agent': task.get('agent', ''),
                'command': task.get('command', ''),
                'credentials': task.get('credentials', ''),
            }
            
            # Add ESP attributes
            esp_attrs = task.get('espAttributes', {})
            for key, value in esp_attrs.items():
                row[f'esp_{key}'] = value
            
            rows.append(row)
        
        # Write CSV
        if rows:
            fieldnames = list(rows[0].keys())
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  Exported: {filename} ({len(rows)} rows)")
        
        return path
    
    def export_workflows_csv(self, filename):
        """Export workflows to CSV file"""
        path = os.path.join(self.output_dir, filename)
        workflows = self.data.get('workflows', [])
        
        if not workflows:
            return None
        
        # Flatten workflow data for CSV
        rows = []
        for wf in workflows:
            row = {
                'name': wf.get('name', ''),
                'type': wf.get('type', ''),
                'summary': wf.get('summary', ''),
                'num_vertices': len(wf.get('workflowVertices', [])),
                'num_edges': len(wf.get('workflowEdges', [])),
            }
            
            # Add ESP attributes
            esp_attrs = wf.get('espAttributes', {})
            for key, value in esp_attrs.items():
                row[f'esp_{key}'] = value
            
            rows.append(row)
        
        # Write CSV
        if rows:
            fieldnames = list(rows[0].keys())
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  Exported: {filename} ({len(rows)} rows)")
        
        return path
    
    def export_stonebranch_import(self, filename):
        """
        Export in Stonebranch import format
        This format can be directly imported via UAC API
        """
        path = os.path.join(self.output_dir, filename)
        
        import_data = {
            'version': '7.0.0',
            'exportTime': datetime.now().isoformat(),
            'tasks': self.data.get('tasks', []),
            'workflows': self.data.get('workflows', []),
            'triggers': self.data.get('triggers', []),
            'variables': self.data.get('variables', []),
        }
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(import_data, f, indent=2, default=str)
        
        print(f"  Exported Stonebranch import file: {filename}")
        return path
    
    def export_summary_report(self, filename):
        """Export summary report"""
        path = os.path.join(self.output_dir, filename)
        
        tasks = self.data.get('tasks', [])
        workflows = self.data.get('workflows', [])
        triggers = self.data.get('triggers', [])
        variables = self.data.get('variables', [])
        
        report = []
        report.append("=" * 60)
        report.append("ESP to Stonebranch Conversion Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 60)
        report.append("")
        report.append("SUMMARY")
        report.append("-" * 40)
        report.append(f"  Tasks:     {len(tasks):,}")
        report.append(f"  Workflows: {len(workflows):,}")
        report.append(f"  Triggers:  {len(triggers):,}")
        report.append(f"  Variables: {len(variables):,}")
        report.append("")
        
        # Task types breakdown
        report.append("TASK TYPES")
        report.append("-" * 40)
        task_types = {}
        for task in tasks:
            t = task.get('type', 'unknown')
            task_types[t] = task_types.get(t, 0) + 1
        for t, count in sorted(task_types.items()):
            report.append(f"  {t}: {count:,}")
        report.append("")
        
        # Workflows
        if workflows:
            report.append("WORKFLOWS")
            report.append("-" * 40)
            for wf in workflows[:20]:  # First 20
                name = wf.get('name', '')
                vertices = len(wf.get('workflowVertices', []))
                edges = len(wf.get('workflowEdges', []))
                report.append(f"  {name}: {vertices} tasks, {edges} dependencies")
            if len(workflows) > 20:
                report.append(f"  ... and {len(workflows) - 20} more")
            report.append("")
        
        report.append("=" * 60)
        
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        print(f"  Exported summary report: {filename}")
        return path
