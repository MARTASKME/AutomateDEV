"""
Stonebranch XML Exporter - Export to Stonebranch XML Unload format
Compatible with UAC bulk import
"""

import os
import uuid
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from collections import OrderedDict


class StonebranchXMLExporter:
    """Export converted data to Stonebranch XML Unload format"""
    
    def __init__(self, esp_data, converted_data=None, output_dir="."):
        """
        Initialize exporter
        
        Args:
            esp_data: Parsed ESP data from ESPParser
            converted_data: Converted data from StonebranchConverter (optional)
            output_dir: Directory for output files
        """
        self.esp_data = esp_data
        self.converted_data = converted_data
        self.output_dir = output_dir
        self.unload_version = "7.7.0.0"
        self.xct_version = "7.9.2"
        
        # ID mappings - use deterministic IDs based on names with unique prefixes
        self.task_ids = {}      # task_name -> sys_id
        self.workflow_ids = {}  # workflow_name -> sys_id
        self.vertex_ids = {}    # (workflow_name, task_name) -> vertex_id (number)
        self.vertex_sys_ids = {} # (workflow_name, task_name) -> sys_id
        self.edge_sys_ids = {}  # (workflow_name, source, target) -> sys_id
        self.credential_ids = {}
        self.agent_cluster_ids = {}
        self.variable_ids = {}  # var_name -> sys_id
        
        # Global set to track ALL generated IDs across all object types
        self._all_generated_ids = set()
        
        # Counter for unique ID generation
        self._id_counter = 0
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def _generate_id(self, seed=None):
        """Generate unique UUID for sys_id - guaranteed unique across ALL objects"""
        import hashlib
        
        if seed:
            # Create deterministic ID from seed string
            hash_obj = hashlib.md5(seed.encode('utf-8'))
            base_id = hash_obj.hexdigest()
        else:
            # Fallback to UUID
            base_id = uuid.uuid4().hex
        
        # Ensure uniqueness - if ID already used, append counter
        final_id = base_id
        suffix = 0
        while final_id in self._all_generated_ids:
            suffix += 1
            hash_obj = hashlib.md5(f"{seed}_{suffix}".encode('utf-8'))
            final_id = hash_obj.hexdigest()
        
        self._all_generated_ids.add(final_id)
        return final_id
    
    def _get_timestamp(self):
        """Get current timestamp"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def _prettify_xml(self, elem):
        """Return pretty-printed XML string"""
        rough_string = ET.tostring(elem, encoding='unicode')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="")
    
    def _create_unload_root(self):
        """Create root unload element"""
        root = ET.Element("unload")
        root.set("unload_date", self._get_timestamp())
        root.set("unload_version", self.unload_version)
        root.set("xct_version", self.xct_version)
        return root
    
    def _add_common_fields(self, elem, name, sys_id, sys_class_name):
        """Add common fields to task/workflow element with sys_id"""
        timestamp = self._get_timestamp()
        
        ET.SubElement(elem, "name").text = name
        ET.SubElement(elem, "sys_id").text = sys_id
        ET.SubElement(elem, "opswise_groups").text = ""
        ET.SubElement(elem, "sys_class_name").text = sys_class_name
        ET.SubElement(elem, "version").text = "1"
        ET.SubElement(elem, "sys_updated_by").text = "ops.admin"
        ET.SubElement(elem, "sys_created_by").text = "ops.admin"
        ET.SubElement(elem, "sys_updated_on").text = timestamp
        ET.SubElement(elem, "sys_created_on").text = timestamp
        ET.SubElement(elem, "change_history").text = ""
    
    def _add_task_common_fields(self, elem, task_type=4):
        """Add common task fields"""
        ET.SubElement(elem, "type").text = str(task_type)
        ET.SubElement(elem, "avg_run_time").text = ""
        ET.SubElement(elem, "checksum").text = ""
        ET.SubElement(elem, "cp_duration").text = ""
        ET.SubElement(elem, "cp_duration_unit").text = "2"
        ET.SubElement(elem, "credentials").text = ""
        ET.SubElement(elem, "credentials_var").text = ""
        ET.SubElement(elem, "credentials_var_check").text = ""
        
        # Early/Late finish fields
        for prefix in ["ef", "lf"]:
            ET.SubElement(elem, f"{prefix}_day_constraint").text = "0"
            ET.SubElement(elem, f"{prefix}_duration").text = "00:00:00:00"
            ET.SubElement(elem, f"{prefix}_enabled").text = ""
            ET.SubElement(elem, f"{prefix}_nth_amount").text = "5"
            ET.SubElement(elem, f"{prefix}_offset_duration").text = ""
            ET.SubElement(elem, f"{prefix}_offset_duration_unit").text = "2"
            ET.SubElement(elem, f"{prefix}_offset_percentage").text = "0"
            ET.SubElement(elem, f"{prefix}_offset_type").text = "1"
            ET.SubElement(elem, f"{prefix}_time").text = ""
            ET.SubElement(elem, f"{prefix}_type").text = "1"
        
        ET.SubElement(elem, "exclusive_with_self").text = "false"
        ET.SubElement(elem, "exec_counter").text = ""
        ET.SubElement(elem, "execution_restriction").text = "0"
        ET.SubElement(elem, "first_run").text = ""
        ET.SubElement(elem, "hold_resources").text = ""
        ET.SubElement(elem, "last_run").text = ""
        ET.SubElement(elem, "last_run_time").text = ""
        ET.SubElement(elem, "log_level").text = "0"
        
        # Late start fields
        ET.SubElement(elem, "ls_day_constraint").text = "0"
        ET.SubElement(elem, "ls_duration").text = "00:00:00:00"
        ET.SubElement(elem, "ls_enabled").text = ""
        ET.SubElement(elem, "ls_nth_amount").text = "5"
        ET.SubElement(elem, "ls_time").text = ""
        ET.SubElement(elem, "ls_type").text = "1"
        
        ET.SubElement(elem, "max_run_time").text = ""
        ET.SubElement(elem, "min_run_time").text = ""
        ET.SubElement(elem, "res_priority").text = ""
        ET.SubElement(elem, "resolve_name_immediately").text = "true"
        ET.SubElement(elem, "restriction_period").text = "0"
        ET.SubElement(elem, "retry_indefinitely").text = ""
        ET.SubElement(elem, "retry_interval").text = "60"
        ET.SubElement(elem, "retry_maximum").text = ""
        ET.SubElement(elem, "retry_suppress_failure").text = ""
        
        # Restriction period fields
        ET.SubElement(elem, "rp_after_date").text = ""
        ET.SubElement(elem, "rp_after_time").text = ""
        ET.SubElement(elem, "rp_before_date").text = ""
        ET.SubElement(elem, "rp_before_time").text = ""
        ET.SubElement(elem, "rp_date_list").text = ""
        
        ET.SubElement(elem, "run_count").text = ""
        ET.SubElement(elem, "run_time").text = ""
        ET.SubElement(elem, "start_held").text = "true"
        ET.SubElement(elem, "start_held_reason").text = ""
        ET.SubElement(elem, "summary").text = ""
        ET.SubElement(elem, "time_zone_pref").text = "0"
        
        # Time window fields
        ET.SubElement(elem, "tw_delay_amount").text = ""
        ET.SubElement(elem, "tw_delay_duration").text = "00:00:00:00"
        ET.SubElement(elem, "tw_delay_type").text = "0"
        ET.SubElement(elem, "tw_wait_amount").text = ""
        ET.SubElement(elem, "tw_wait_day_constraint").text = "0"
        ET.SubElement(elem, "tw_wait_duration").text = "00:00:00:00"
        ET.SubElement(elem, "tw_wait_time").text = "00:00"
        ET.SubElement(elem, "tw_wait_type").text = "0"
        ET.SubElement(elem, "tw_workflow_only").text = "0"
        
        ET.SubElement(elem, "universal_template_id").text = ""
        ET.SubElement(elem, "user_duration").text = ""
    
    def export_all(self, prefix="ops"):
        """Export all XML files"""
        print(f"\nExporting to Stonebranch XML format...")
        print(f"Output directory: {self.output_dir}")
        
        # Pre-generate IDs for all objects
        self._generate_all_ids()
        
        # Export each type
        files = []
        
        # Tasks
        unix_file = self.export_unix_tasks(f"{prefix}_task_unix.xml")
        if unix_file:
            files.append(unix_file)
        
        windows_file = self.export_windows_tasks(f"{prefix}_task_windows.xml")
        if windows_file:
            files.append(windows_file)
        
        # Workflows
        wf_file = self.export_workflows(f"{prefix}_task_workflow.xml")
        if wf_file:
            files.append(wf_file)
        
        vertex_file = self.export_workflow_vertices(f"{prefix}_task_workflow_vertex.xml")
        if vertex_file:
            files.append(vertex_file)
        
        edge_file = self.export_workflow_edges(f"{prefix}_task_workflow_edge.xml")
        if edge_file:
            files.append(edge_file)
        
        # Variables
        var_file = self.export_variables(f"{prefix}_variable.xml")
        if var_file:
            files.append(var_file)
        
        # Credentials
        cred_file = self.export_credentials(f"{prefix}_credentials.xml")
        if cred_file:
            files.append(cred_file)
        
        print(f"\nExported {len(files)} XML files")
        return files
    
    def _generate_all_ids(self):
        """Pre-generate deterministic IDs for all objects"""
        jobs = self.esp_data.get('JOB', [])
        applications = self.esp_data.get('APPLICATION', [])
        variables = self.esp_data.get('VARIABLE', [])
        
        # Generate task IDs - deterministic based on task name
        for job in jobs:
            job_name = job.get('JOB_NAME', '')
            if job_name and job_name not in self.task_ids:
                self.task_ids[job_name] = self._generate_id(f"task:{job_name}")
        
        # Generate variable IDs
        for var in variables:
            var_name = var.get('VAR_NAME', '')
            if var_name and var_name not in self.variable_ids:
                self.variable_ids[var_name] = self._generate_id(f"var:{var_name}")
        
        # Generate workflow IDs and vertex IDs
        for appl in applications:
            appl_name = appl.get('APPL_NAME', '')
            wf_name = f"{appl_name}_WF" if appl_name else ''
            
            if wf_name and wf_name not in self.workflow_ids:
                self.workflow_ids[wf_name] = self._generate_id(f"workflow:{wf_name}")
                
                # Find jobs in this application
                appl_jobs = [j for j in jobs if j.get('APPL_NAME') == appl_name]
                
                # Assign vertex IDs (start from 5000 for first, then 2, 3, 4...)
                for i, job in enumerate(appl_jobs):
                    job_name = job.get('JOB_NAME', '')
                    vertex_id = 5000 if i == 0 else (i + 1)
                    self.vertex_ids[(wf_name, job_name)] = vertex_id
                    # Generate unique sys_id for each vertex
                    self.vertex_sys_ids[(wf_name, job_name)] = self._generate_id(f"vertex:{wf_name}:{job_name}")
    
    def export_unix_tasks(self, filename):
        """Export Unix/Linux tasks"""
        jobs = self.esp_data.get('JOB', [])
        unix_jobs = [j for j in jobs if j.get('JOB_TYPE') in ('JOB', 'LINUX_JOB')]
        
        if not unix_jobs:
            return None
        
        root = self._create_unload_root()
        
        # Deduplicate unix jobs by name
        seen_jobs = set()
        unique_jobs = []
        for job in unix_jobs:
            job_name = job.get('JOB_NAME', '')
            if job_name and job_name not in seen_jobs:
                seen_jobs.add(job_name)
                unique_jobs.append(job)
        
        for job in unique_jobs:
            job_name = job.get('JOB_NAME', '')
            sys_id = self._generate_id(f"unix_task:{job_name}")
            
            task = ET.SubElement(root, "ops_task_unix")
            task.set("action", "INSERT_OR_UPDATE")
            
            self._add_common_fields(task, job_name, sys_id, "ops_task_unix")
            self._add_task_common_fields(task, task_type=4)
            
            # Unix specific fields
            ET.SubElement(task, "parameters").text = job.get('ARGS', '')
            ET.SubElement(task, "custom_field1").text = ""
            ET.SubElement(task, "custom_field2").text = ""
            ET.SubElement(task, "override_instance_wait").text = "0"
            ET.SubElement(task, "enforce_variables").text = "false"
            ET.SubElement(task, "lock_variables").text = "false"
            ET.SubElement(task, "res_priority_var").text = ""
            
            # Agent fields
            ET.SubElement(task, "agent").text = ""
            ET.SubElement(task, "agent_cluster").text = job.get('AGENT', '')
            ET.SubElement(task, "agent_cluster_var").text = ""
            ET.SubElement(task, "agent_cluster_var_check").text = "false"
            ET.SubElement(task, "agent_var").text = ""
            ET.SubElement(task, "agent_var_check").text = "false"
            ET.SubElement(task, "broadcast_cluster").text = ""
            ET.SubElement(task, "broadcast_cluster_var").text = ""
            ET.SubElement(task, "broadcast_cluster_var_check").text = "false"
            
            # Command fields
            scriptname = job.get('SCRIPTNAME', '')
            cmdname = job.get('CMDNAME', '')
            
            ET.SubElement(task, "command").text = cmdname or scriptname or "/bin/sh"
            ET.SubElement(task, "command_or_script").text = "1"  # 1=command, 2=script
            ET.SubElement(task, "environment").text = ""
            ET.SubElement(task, "exit_code_output").text = ""
            ET.SubElement(task, "exit_code_processing").text = "1"
            ET.SubElement(task, "exit_code_text").text = ""
            ET.SubElement(task, "exit_codes").text = "0"
            ET.SubElement(task, "output_failure_only").text = "false"
            ET.SubElement(task, "output_return_file").text = ""
            ET.SubElement(task, "output_return_nline").text = "100"
            ET.SubElement(task, "output_return_sline").text = "1"
            ET.SubElement(task, "output_return_text").text = ""
            ET.SubElement(task, "output_return_type").text = "1"
            ET.SubElement(task, "output_type").text = "1"
            ET.SubElement(task, "retry_exit_codes").text = ""
            ET.SubElement(task, "run_as_sudo").text = "false"
            ET.SubElement(task, "runtime_dir").text = ""
            ET.SubElement(task, "script").text = ""
            ET.SubElement(task, "wait_for_output").text = "false"
        
        # Write file
        path = os.path.join(self.output_dir, filename)
        self._write_xml(root, path)
        print(f"  Exported: {filename} ({len(unix_jobs)} tasks)")
        return path
    
    def export_windows_tasks(self, filename):
        """Export Windows tasks"""
        jobs = self.esp_data.get('JOB', [])
        win_jobs = [j for j in jobs if j.get('JOB_TYPE') == 'NT_JOB']
        
        if not win_jobs:
            return None
        
        root = self._create_unload_root()
        
        # Deduplicate windows jobs by name
        seen_jobs = set()
        unique_jobs = []
        for job in win_jobs:
            job_name = job.get('JOB_NAME', '')
            if job_name and job_name not in seen_jobs:
                seen_jobs.add(job_name)
                unique_jobs.append(job)
        
        for job in unique_jobs:
            job_name = job.get('JOB_NAME', '')
            sys_id = self._generate_id(f"win_task:{job_name}")
            
            task = ET.SubElement(root, "ops_task_windows")
            task.set("action", "INSERT_OR_UPDATE")
            
            self._add_common_fields(task, job_name, sys_id, "ops_task_windows")
            self._add_task_common_fields(task, task_type=5)
            
            # Windows specific fields
            cmdname = job.get('CMDNAME', '')
            args = job.get('ARGS', '')
            
            ET.SubElement(task, "command").text = cmdname or "cmd.exe"
            ET.SubElement(task, "parameters").text = args
            ET.SubElement(task, "agent").text = ""
            ET.SubElement(task, "agent_cluster").text = job.get('AGENT', '')
            ET.SubElement(task, "exit_codes").text = "0"
        
        path = os.path.join(self.output_dir, filename)
        self._write_xml(root, path)
        print(f"  Exported: {filename} ({len(win_jobs)} tasks)")
        return path
    
    def export_workflows(self, filename):
        """Export workflows"""
        applications = self.esp_data.get('APPLICATION', [])
        
        if not applications:
            return None
        
        root = self._create_unload_root()
        
        # Deduplicate applications by name
        seen_appls = set()
        unique_appls = []
        for appl in applications:
            appl_name = appl.get('APPL_NAME', '')
            if appl_name and appl_name not in seen_appls:
                seen_appls.add(appl_name)
                unique_appls.append(appl)
        
        for appl in unique_appls:
            appl_name = appl.get('APPL_NAME', '')
            member_name = appl.get('MEMBER_NAME', '')
            wf_name = f"{appl_name}_WF"
            sys_id = self._generate_id(f"workflow:{wf_name}")
            
            wf = ET.SubElement(root, "ops_task_workflow")
            wf.set("action", "INSERT_OR_UPDATE")
            
            self._add_common_fields(wf, wf_name, sys_id, "ops_task_workflow")
            self._add_task_common_fields(wf, task_type=1)
            
            # Update summary
            for child in wf:
                if child.tag == "summary":
                    child.text = f"dsn={member_name}.{appl_name}"
                    break
            
            # Workflow specific fields
            ET.SubElement(wf, "first_run_task_id").text = ""
            ET.SubElement(wf, "layout_option").text = "0"
            ET.SubElement(wf, "skip_on_failure").text = "false"
        
        path = os.path.join(self.output_dir, filename)
        self._write_xml(root, path)
        print(f"  Exported: {filename} ({len(applications)} workflows)")
        return path
    
    def export_workflow_vertices(self, filename):
        """Export workflow vertices (tasks in workflows)"""
        applications = self.esp_data.get('APPLICATION', [])
        jobs = self.esp_data.get('JOB', [])
        
        if not applications:
            return None
        
        root = self._create_unload_root()
        vertex_count = 0
        
        # Track exported vertices to avoid duplicates
        exported_vertices = set()
        
        for appl in applications:
            appl_name = appl.get('APPL_NAME', '')
            wf_name = f"{appl_name}_WF"
            workflow_id = self.workflow_ids.get(wf_name, '')
            
            # Find jobs in this application
            appl_jobs = [j for j in jobs if j.get('APPL_NAME') == appl_name]
            
            for job in appl_jobs:
                job_name = job.get('JOB_NAME', '')
                
                # Skip if already exported this vertex
                vertex_key = (wf_name, job_name)
                if vertex_key in exported_vertices:
                    continue
                exported_vertices.add(vertex_key)
                
                vertex_id = self.vertex_ids.get((wf_name, job_name), 1)
                
                vertex = ET.SubElement(root, "ops_task_workflow_vertex")
                vertex.set("action", "INSERT_OR_UPDATE")
                
                sys_id = self._generate_id(f"vertex:{wf_name}:{job_name}")
                self._add_common_fields(vertex, job_name, sys_id, "ops_task_workflow_vertex")
                
                ET.SubElement(vertex, "cp_duration_count").text = "0"
                ET.SubElement(vertex, "cp_duration_total").text = "0"
                ET.SubElement(vertex, "duration_count").text = "0"
                ET.SubElement(vertex, "duration_total").text = "0"
                ET.SubElement(vertex, "ignore").text = ""
                ET.SubElement(vertex, "start_offset_count").text = "0"
                ET.SubElement(vertex, "start_offset_total").text = "0"
                # Use name references instead of sys_id references
                ET.SubElement(vertex, "task").text = job_name
                ET.SubElement(vertex, "vertex_id").text = str(vertex_id)
                ET.SubElement(vertex, "workflow").text = wf_name
                ET.SubElement(vertex, "condition_expression").text = ""
                
                vertex_count += 1
        
        path = os.path.join(self.output_dir, filename)
        self._write_xml(root, path)
        print(f"  Exported: {filename} ({vertex_count} vertices)")
        return path
    
    def export_workflow_edges(self, filename):
        """Export workflow edges (dependencies)"""
        applications = self.esp_data.get('APPLICATION', [])
        jobs = self.esp_data.get('JOB', [])
        dependencies = self.esp_data.get('DEPENDENCY', [])
        
        if not dependencies:
            return None
        
        root = self._create_unload_root()
        edge_count = 0
        
        # Track exported edges to avoid duplicates
        exported_edges = set()
        
        for appl in applications:
            appl_name = appl.get('APPL_NAME', '')
            wf_name = f"{appl_name}_WF"
            workflow_id = self.workflow_ids.get(wf_name, '')
            
            # Find jobs in this application
            appl_job_names = {j.get('JOB_NAME', '') for j in jobs if j.get('APPL_NAME') == appl_name}
            
            # Find dependencies within this application
            for dep in dependencies:
                if dep.get('DEP_TYPE') != 'AFTER':
                    continue
                
                job_name = dep.get('JOB_NAME', '')
                depends_on = dep.get('DEPENDS_ON', '')
                
                if job_name in appl_job_names and depends_on in appl_job_names:
                    # Skip if already exported this edge
                    edge_key = (wf_name, depends_on, job_name)
                    if edge_key in exported_edges:
                        continue
                    exported_edges.add(edge_key)
                    
                    source_vertex = self.vertex_ids.get((wf_name, depends_on), 0)
                    target_vertex = self.vertex_ids.get((wf_name, job_name), 0)
                    
                    edge_name = f"{source_vertex}D{target_vertex}"
                    
                    edge = ET.SubElement(root, "ops_task_workflow_edge")
                    edge.set("action", "INSERT_OR_UPDATE")
                    
                    sys_id = self._generate_id(f"edge:{wf_name}:{depends_on}:{job_name}")
                    self._add_common_fields(edge, edge_name, sys_id, "ops_task_workflow_edge")
                    
                    ET.SubElement(edge, "edge_condition").text = "SUCCESS"
                    ET.SubElement(edge, "ignore").text = ""
                    ET.SubElement(edge, "source_id").text = str(source_vertex)
                    ET.SubElement(edge, "target_id").text = str(target_vertex)
                    # Use name reference instead of sys_id
                    ET.SubElement(edge, "workflow").text = wf_name
                    ET.SubElement(edge, "description").text = ""
                    
                    edge_count += 1
        
        if edge_count == 0:
            return None
        
        path = os.path.join(self.output_dir, filename)
        self._write_xml(root, path)
        print(f"  Exported: {filename} ({edge_count} edges)")
        return path
    
    def export_variables(self, filename):
        """Export variables"""
        variables = self.esp_data.get('VARIABLE', [])
        
        if not variables:
            return None
        
        root = self._create_unload_root()
        
        # Deduplicate variables by name - keep first occurrence
        seen_vars = set()
        unique_vars = []
        for var in variables:
            var_name = var.get('VAR_NAME', '')
            if var_name and var_name not in seen_vars:
                seen_vars.add(var_name)
                unique_vars.append(var)
        
        for var in unique_vars:
            var_name = var.get('VAR_NAME', '')
            var_value = var.get('VAR_VALUE', '')
            
            elem = ET.SubElement(root, "ops_variable")
            elem.set("action", "INSERT_OR_UPDATE")
            
            sys_id = self._generate_id(f"variable:{var_name}")
            self._add_common_fields(elem, var_name, sys_id, "ops_variable")
            
            ET.SubElement(elem, "description").text = f"Migrated from ESP: {var.get('MEMBER_NAME', '')}"
            ET.SubElement(elem, "value").text = var_value
        
        path = os.path.join(self.output_dir, filename)
        self._write_xml(root, path)
        print(f"  Exported: {filename} ({len(unique_vars)} variables)")
        return path
    
    def export_credentials(self, filename):
        """Export credentials (placeholder)"""
        # Extract unique users from jobs
        jobs = self.esp_data.get('JOB', [])
        users = set()
        
        for job in jobs:
            user = job.get('USER', '')
            if user:
                users.add(user)
        
        if not users:
            return None
        
        root = self._create_unload_root()
        
        for user in users:
            elem = ET.SubElement(root, "ops_credentials")
            elem.set("action", "INSERT_OR_UPDATE")
            
            sys_id = self._generate_id(f"credential:{user}")
            self._add_common_fields(elem, user, sys_id, "ops_credentials")
            
            ET.SubElement(elem, "description").text = f"Migrated from ESP"
            ET.SubElement(elem, "runtime_user").text = user
            ET.SubElement(elem, "runtime_password").text = ""  # Needs to be set manually
        
        path = os.path.join(self.output_dir, filename)
        self._write_xml(root, path)
        print(f"  Exported: {filename} ({len(users)} credentials)")
        return path
    
    def _write_xml(self, root, path):
        """Write XML to file"""
        # Add XML declaration
        xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n'
        xml_string += ET.tostring(root, encoding='unicode')
        
        # Format with proper indentation
        try:
            from xml.dom import minidom
            dom = minidom.parseString(xml_string)
            formatted = dom.toprettyxml(indent="", encoding=None)
            # Remove extra declaration added by minidom
            lines = formatted.split('\n')
            if lines[0].startswith('<?xml'):
                lines = lines[1:]
            xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + '\n'.join(lines)
        except:
            pass
        
        with open(path, 'w', encoding='utf-8') as f:
            f.write(xml_string)


def export_to_stonebranch_xml(esp_data, output_dir):
    """Convenience function to export ESP data to Stonebranch XML"""
    exporter = StonebranchXMLExporter(esp_data, output_dir=output_dir)
    return exporter.export_all()
