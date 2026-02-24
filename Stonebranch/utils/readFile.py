import json
import pandas as pd
from pathlib import Path


def loadJson(filename='config.json', parent_dir_level=3, *dirs):
    repo_root = Path(__file__).resolve()
    for _ in range(parent_dir_level):
        repo_root = repo_root.parent
    if dirs:
        for dir in dirs:
            repo_root = repo_root.joinpath(dir)
    json_path = repo_root / filename
    try:
        with open(json_path, 'r') as file:
            config_data = json.load(file)
        print(f"JSON file loaded successfully")
        return config_data
    except Exception as e:
        print(f"Error loading {json_path}: {e}")
        return None

def loadText(filename='config.txt', parent_dir_level=3, *dirs):
    repo_root = Path(__file__).resolve()
    for _ in range(parent_dir_level):
        repo_root = repo_root.parent
    if dirs:
        for dir in dirs:
            repo_root = repo_root.joinpath(dir)
    text_path = repo_root / filename
    try:
        with open(text_path, 'r') as file:
            text_data = file.read()
        print(f"Text file loaded successfully")
        return text_data
    except Exception as e:
        print(f"Error loading {text_path}: {e}")
        return None


def readCSV(filename='config.csv', parent_dir_level=3, *dirs):
    repo_root = Path(__file__).resolve()
    for _ in range(parent_dir_level):
        repo_root = repo_root.parent
    if dirs:
        for dir in dirs:
            repo_root = repo_root.joinpath(dir)
    csv_path = repo_root / filename
    
    try:
        df = pd.read_csv(csv_path)
        print(f"CSV file loaded successfully")
        return df
    except Exception as e:
        print(f"Error loading {csv_path}: {e}")
        return None
    
    
def readFolderTextFiles(folder_path, *file_names):
    folder = Path(folder_path)
    files = {}
    not_found_files = []
    
    for file_name in file_names:
        file_path = folder / file_name
        try:
            with open(file_path, 'r') as file:
                files[file_name] = file.readlines()
        except FileNotFoundError:
            not_found_files.append(file_name)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            not_found_files.append(file_name)
    
    if files:
        print(f"Text files loaded successfully ({len(files)} files loaded)")
        if not_found_files:
            print(f"Files not found: {not_found_files}")
        return files
    else:
        print(f"Error: No files found in {folder_path}")
        return None
    
    
# Consolidate similar functions
def loadFile(filename, file_type='text', parent_dir_level=3, *dirs):
    """Unified file loader for JSON, Text, and CSV"""
    repo_root = Path(__file__).resolve()
    for _ in range(parent_dir_level):
        repo_root = repo_root.parent
    if dirs:
        for dir in dirs:
            repo_root = repo_root.joinpath(dir)
    file_path = repo_root / filename
    
    try:
        if file_type == 'json':
            with open(file_path, 'r') as file:
                return json.load(file)
        elif file_type == 'csv':
            return pd.read_csv(file_path)
        else:  # text
            with open(file_path, 'r') as file:
                return file.read()
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None
    
    
def readFolderJSONFiles(folder_path, *file_names):
    folder = Path(folder_path)
    json_files = {}
    not_found_files = []
    
    # If no file names provided, find all JSON files in the folder
    if not file_names:
        file_names = [f.name for f in folder.glob('*.json')]
    
    for file_name in file_names:
        file_path = folder / file_name
        try:
            with open(file_path, 'r') as file:
                json_files[file_name] = json.load(file)
        except FileNotFoundError:
            not_found_files.append(file_name)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            not_found_files.append(file_name)
    
    if json_files:
        print(f"JSON files loaded successfully ({len(json_files)} files loaded)")
        if not_found_files:
            print(f"Files not found: {not_found_files}")
        return json_files
    else:
        print(f"Error: No files found in {folder_path}")
        return None