"""
Main entry point for ESP to Stonebranch Conversion Tool
Python implementation similar to xpressconversiontool2
"""

import os
import sys
import argparse
from datetime import datetime

# Add current directory to path for local imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from esp_parser import ESPParser, parse_esp_file
from stonebranch_converter import StonebranchConverter, convert_esp_to_stonebranch
from workflow_builder import WorkflowBuilderFromESP
from exporter import Exporter
from config import LOG_LEVEL


def convert_file(input_path, output_dir=None, agent_mapping=None):
    """
    Convert ESP file to Stonebranch format
    
    Args:
        input_path: Path to ESP source file
        output_dir: Output directory (default: same as input)
        agent_mapping: Dict mapping ESP agents to Stonebranch agents
    
    Returns:
        dict: Converted data
    """
    print("=" * 60)
    print("ESP to Stonebranch Conversion Tool")
    print("=" * 60)
    print(f"\nInput file: {input_path}")
    
    if not os.path.exists(input_path):
        print(f"Error: File not found - {input_path}")
        return None
    
    # Set output directory
    if output_dir is None:
        output_dir = os.path.dirname(input_path) or "."
    
    print(f"Output directory: {output_dir}")
    
    # Step 1: Parse ESP file
    print("\n" + "-" * 40)
    print("Step 1: Parsing ESP file...")
    print("-" * 40)
    
    parser = ESPParser(file_path=input_path)
    esp_data = parser.parse()
    
    if not esp_data:
        print("Error: Failed to parse ESP file")
        return None
    
    # Step 2: Convert to Stonebranch format
    print("\n" + "-" * 40)
    print("Step 2: Converting to Stonebranch format...")
    print("-" * 40)
    
    converter = StonebranchConverter(esp_data, agent_mapping)
    converted_data = converter.convert()
    
    # Step 3: Export results
    print("\n" + "-" * 40)
    print("Step 3: Exporting results...")
    print("-" * 40)
    
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    exporter = Exporter(converted_data, output_dir)
    
    # Export all formats
    exporter.export_all(base_name)
    exporter.export_stonebranch_import(f"{base_name}_stonebranch_import.json")
    exporter.export_summary_report(f"{base_name}_report.txt")
    
    print("\n" + "=" * 60)
    print("Conversion completed successfully!")
    print("=" * 60)
    
    return converted_data


def interactive_mode():
    """Interactive mode for command-line usage"""
    print("=" * 60)
    print("ESP to Stonebranch Conversion Tool")
    print("Python Implementation v1.0")
    print("=" * 60)
    print("\nThis tool converts CA ESP (Workload Automation ESP) files")
    print("to Stonebranch UAC format (tasks, workflows, triggers).")
    print()
    
    # Get input file
    input_path = input("Enter the path to ESP source file: ").strip()
    if not input_path:
        print("Error: No input file specified")
        return
    
    input_path = input_path.strip('"\'')
    
    if not os.path.exists(input_path):
        print(f"Error: File not found - {input_path}")
        return
    
    # Get output directory
    default_output = os.path.dirname(input_path) or "."
    output_dir = input(f"Output directory [{default_output}]: ").strip() or default_output
    
    # Run conversion
    convert_file(input_path, output_dir)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="ESP to Stonebranch Conversion Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py input.txt
  python main.py input.txt -o ./output
  python main.py input.txt -o ./output --agent-map agent_mapping.json
        """
    )
    
    parser.add_argument(
        'input_file',
        nargs='?',
        help='ESP source file to convert'
    )
    
    parser.add_argument(
        '-o', '--output',
        dest='output_dir',
        help='Output directory for converted files'
    )
    
    parser.add_argument(
        '--agent-map',
        dest='agent_map_file',
        help='JSON file with agent name mappings'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    if args.input_file:
        # Command-line mode
        agent_mapping = None
        if args.agent_map_file:
            import json
            with open(args.agent_map_file) as f:
                agent_mapping = json.load(f)
        
        convert_file(args.input_file, args.output_dir, agent_mapping)
    else:
        # Interactive mode
        interactive_mode()


if __name__ == '__main__':
    main()
