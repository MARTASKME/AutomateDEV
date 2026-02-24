import sys
import os
import xmltodict
import json
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.createFile import createJson, createExcel
from utils.readFile import loadText



#XML_PATH = './Stonebranch/Excel_BMC/XmlConvert/XML-NONPROD-CTE2-13-08-2024.xml'
JSON_OUTPUT_FILE = 'xJsonOutput.json'
EXCEL_OUTPUT_FILE = 'xJsonOutput.xlsx'

ROOT_KEY = 'DEFTABLE'
JSON_COLUMN_CHAR = '@'
EXPAND_COLUMN = ["@JOBNAME", "@FOLDER_NAME"]

SHEETNAME_LIST = ["SMART_FOLDER","FOLDER","JOB","VARIABLE"]


def prepareXJson(json_dict):
    xjson_dict_source = json_dict[ROOT_KEY]
    xjson_dict = {}
    for key, value in xjson_dict_source.items():
        if not key.startswith(JSON_COLUMN_CHAR):
            xjson_dict[key] = value
    return xjson_dict


def recursiveFlattenSheetDict(xjson_value, sheet_dict = None, key_column = None, deep_parent = None):
    
    if sheet_dict is None:
        sheet_dict = {}
    if deep_parent is None:
        deep_parent = {}
    
    if isinstance(xjson_value, dict):
        row_data = {}
        for key, value in xjson_value.items():
            if key in EXPAND_COLUMN:
                deep_parent[key] = value
                
        for key, value in xjson_value.items():
            if not key.startswith(JSON_COLUMN_CHAR):
                if key not in sheet_dict:
                    sheet_dict[key] = []

                sheet_dict = recursiveFlattenSheetDict(value, sheet_dict, key, deep_parent)
            else:
                row_data[key] = value
        
        if key_column is not None:
            if any(key in xjson_value for key in EXPAND_COLUMN) and not all(key in xjson_value for key in EXPAND_COLUMN):
                deep_parent = {}
            for key, value in deep_parent.items():
                if key not in row_data:
                    row_data[key] = value
            sheet_dict[key_column].append(row_data)
            
    elif isinstance(xjson_value, list):
        for value in xjson_value:
            sheet_dict = recursiveFlattenSheetDict(value, sheet_dict, key_column, deep_parent)
            
    return sheet_dict


def insertList(list, index, value):
    if index < len(list):
        list.insert(index, value)
    else:
        list.append(value)
    return list


def XJsonToDataFrame(xjson_dict, reorder = True):
    
    sheet_dict = recursiveFlattenSheetDict(xjson_dict)
    if reorder:
        ordered_keys = ['SMART_FOLDER', 'FOLDER'] + [key for key in sheet_dict if key not in ['SMART_FOLDER', 'FOLDER']]
        reordered_dict = {key: sheet_dict[key] for key in ordered_keys}
        sheet_dict = reordered_dict.copy()
    df_list = []
    for key, value in sheet_dict.items():
        df = pd.DataFrame(value)
        df.columns = df.columns.str.replace('@', '', regex=False)
        df_list.append((key, df))
    return df_list




def main():
    xml_path = input("Enter the path of the XML file: ")
    XML_text = loadText(xml_path)
    json_data = xmltodict.parse(XML_text)
    
    xjson_dict = prepareXJson(json_data)
    createJson(JSON_OUTPUT_FILE, xjson_dict)
    
    df_list = XJsonToDataFrame(xjson_dict)
    createExcel(EXCEL_OUTPUT_FILE, *df_list)

if __name__ == '__main__':
    main()