#!/usr/bin/env python
"""
Run script for ESP to Stonebranch Conversion Tool
Use this script to run the conversion tool from command line
"""

import os
import sys

# Add current directory to path for local imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from esp_parser import ESPParser
from stonebranch_converter import StonebranchConverter
from stonebranch_xml_exporter import StonebranchXMLExporter
from exporter import Exporter


def run_conversion(input_file, output_dir=None, output_format='xml'):
    """
    Run the complete conversion process
    
    Args:
        input_file: Path to ESP source file
        output_dir: Output directory (optional)
        output_format: 'xml' for Stonebranch XML, 'json' for JSON format
    """
    print("=" * 60)
    print("ESP to Stonebranch Conversion Tool")
    print("=" * 60)
    
    if not os.path.exists(input_file):
        print(f"Error: File not found - {input_file}")
        return None
    
    if output_dir is None:
        output_dir = os.path.dirname(input_file) or "."
    
    print(f"\nInput:  {input_file}")
    print(f"Output: {output_dir}")
    print(f"Format: {output_format.upper()}")
    
    # Parse
    print("\n[1/3] Parsing ESP file...")
    parser = ESPParser(file_path=input_file)
    esp_data = parser.parse()
    
    if output_format == 'xml':
        # Export to Stonebranch XML format
        print("\n[2/3] Converting to Stonebranch XML format...")
        xml_exporter = StonebranchXMLExporter(esp_data, output_dir=output_dir)
        
        print("\n[3/3] Exporting XML files...")
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        xml_exporter.export_all(prefix=f"ops_{base_name}")
    else:
        # Export to JSON format
        print("\n[2/3] Converting to Stonebranch format...")
        converter = StonebranchConverter(esp_data)
        converted = converter.convert()
        
        print("\n[3/3] Exporting JSON files...")
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        exporter = Exporter(converted, output_dir)
        exporter.export_all(base_name)
        exporter.export_stonebranch_import(f"{base_name}_stonebranch_import.json")
        exporter.export_summary_report(f"{base_name}_report.txt")
    
    print("\n" + "=" * 60)
    print("Conversion completed!")
    print("=" * 60)
    
    return esp_data


def main():
    """Main entry point"""
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else None
        output_format = sys.argv[3] if len(sys.argv) > 3 else 'xml'
        run_conversion(input_file, output_dir, output_format)
    else:
        print("ESP to Stonebranch Conversion Tool")
        print("-" * 40)
        print()
        
        input_file = input("Enter ESP source file path: ").strip().strip('"\'')
        
        if not input_file:
            print("Error: No file specified")
            return
        
        output_dir = input("Output directory (press Enter for default): ").strip() or None
        
        print("\nOutput format:")
        print("  1. Stonebranch XML (for bulk import)")
        print("  2. JSON")
        format_choice = input("Choose format [1]: ").strip() or "1"
        output_format = 'json' if format_choice == '2' else 'xml'
        
        run_conversion(input_file, output_dir, output_format)


if __name__ == '__main__':
    main()
