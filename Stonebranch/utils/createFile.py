import json
import pandas as pd
from io import StringIO
from pathlib import Path


def _get_full_path(filename, output_path=None):
    """Helper function to construct full file path"""
    if output_path:
        Path(output_path).mkdir(parents=True, exist_ok=True)
        return Path(output_path) / filename
    return Path(filename)


# Create JSON file
def createJson(filename, data, show_response=True, indent=4, output_path=None):
    try:
        full_path = _get_full_path(filename, output_path)
        with open(full_path, 'w') as file:
            json.dump(data, file, indent=indent)
        if show_response:
            print(f"{full_path} created successfully")
    except Exception as e:
        print(f"Error creating {filename}: {e}")

# Create XML file
def createXml(filename, data, show_response=True, output_path=None):
    try:
        full_path = _get_full_path(filename, output_path)
        with open(full_path, 'w') as file:
            file.write(data)
        if show_response:
            print(f"{full_path} created successfully")
    except Exception as e:
        print(f"Error creating {filename}: {e}")

# Create text file
def createText(filename, data, show_response=True, output_path=None):
    try:
        full_path = _get_full_path(filename, output_path)
        with open(full_path, 'wb') as file:
            file.write(data)
        if show_response:
            print(f"{full_path} created successfully")
    except Exception as e:
        print(f"Error creating {filename}: {e}")

# Create Excel file
def createExcel(outputfile, *data, output_path=None):
    try:
        full_path = _get_full_path(outputfile, output_path)
        with pd.ExcelWriter(full_path) as writer:
            for sheetname, df in data:
                df.to_excel(writer, sheet_name=sheetname, index=False)
        print(f"{full_path} created successfully")
    except Exception as e:
        print(f"Error creating {outputfile}: {e}")

# Prepare output file based on the format
def prepareOutputFile(data_response, filename, format_str, sheetname='Sheet', output_path=None):
    if format_str == "csv":
        data = pd.read_csv(StringIO(data_response.text))
        createExcel(f"{filename}.xlsx", (sheetname, data), output_path=output_path)
    elif format_str == "json":
        data = data_response.json()
        createJson(f"{filename}.json", data, output_path=output_path)
    elif format_str == "xml":
        data = data_response.text
        createXml(f"{filename}.xml", data, output_path=output_path)
        
# Create folder
def createFolder(foldername, output_path=None):
    try:
        if output_path:
            full_path = Path(output_path) / foldername
        else:
            full_path = Path(foldername)
        full_path.mkdir(parents=True, exist_ok=True)
        print(f"{full_path} folder created successfully")
    except Exception as e:
        print(f"Error creating {foldername} folder: {e}")
        

# Create PNG image file
def createImagePng(filename, image, show_response=True, output_path=None):
    try:
        full_path = _get_full_path(filename, output_path)
        with open(full_path, 'wb') as file:
            file.write(image)
        if show_response:
            print(f"{full_path} created successfully")
    except Exception as e:
        print(f"Error creating {filename}: {e}")