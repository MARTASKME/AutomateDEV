import sys
import os
import json
import copy
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.createFile import createJson
from utils.createFile import createExcel
from utils.readFile import readFolderJSONFiles

JSON_FOLDER = "D:\\Tidlor\\Task_Clone\\Workflow_05\\Workspace\\"
OUTPUT_JSON_FOLDER = "D:\\Tidlor\\Task_Clone\\Workflow_05\\Created_JSON\\"

OUTPUT_EXCEL_FILENAME = "C:\\Dev\\AutomateDEV\\Stonebranch\\JSON\\EditValueAdv\\Created_JSON_workflow_05_Log.xlsx"
OUTPUT_EXCEL_SHEETNAME = "Created_JSON_Log"

REPLACE_TARGET = "AAGI"

REPLACE_WORD_LIST = [
    "AXA",
    "BKI",
    "CUB",
    "DHP",
    "ERGO",
    "KPI",
    "LMG",
    "MSI",
    "MTI",
    "SAF",
    "SOM",
    "TNI",
    "TVI",
    "VIB"
]

REPLACE_JSON_FLAG = True

SPECIAL_CASE_DICT = {
    # Remove
    "DEL_SYSID": True,
    # Change field and clear original value
    "AGENT_TO_AGENTVAR": False,
    "CRED_TO_CREDVAR": False,
    "FTP_AGENT_TO_FTP_AGENTVAR": False,
    "FTP_CRED_TO_FTP_CREDVAR": False,
    "PASSPHRASE_TO_PASSPHRASEVAR": True,
    # Clean up
    "CLEAR_EMAIL_NOTIFICATION": False,
    "CLEAR_BUSINESS_SERVICE": False
}


def prepareReplacedJSON(json_data_dict, target_word, replace_word_list):

    created_json_dict = {}
    
    for replace_word in replace_word_list:
        for json_name, json_data in json_data_dict.items():
            modified_json = copy.deepcopy(json_data)
            json_str = json.dumps(modified_json)
            json_str_replaced = json_str.replace(target_word, replace_word)
            modified_json_replaced = json.loads(json_str_replaced)
            new_json_name = f"{json_name.replace(target_word, replace_word)}"
            created_json_dict[new_json_name] = modified_json_replaced
    
    return created_json_dict

def prepareSpecialCases(json_data_dict, special_case_dict):
    new_json_data_dict = {}
    
    
    def delete_nested_field(data, field_to_delete):
        if isinstance(data, dict):
            return {k: delete_nested_field(v, field_to_delete) for k, v in data.items() if k != field_to_delete}
        elif isinstance(data, list):
            return [delete_nested_field(item, field_to_delete) for item in data]
        else:
            return data
    
    
    # Helper function to replace field1 value to field2 and clear field1 value
    def replace_fieldA_with_fieldB(data, field1, field2, sub_field1=None, sub_field2=None):
        if isinstance(data, dict):
            new_dict = {}
            field1_value = data.get(field1, None)
            if sub_field1 and isinstance(field1_value, dict):
                field1_value = field1_value.get(sub_field1, None)
            for k, v in data.items():
                if k == field1:
                    if sub_field1 and isinstance(v, dict) and sub_field1 in v:
                        v[sub_field1] = None  # Clear the value of the specified sub-field in field1
                        new_dict[k] = v
                    else:
                        v = None  # Clear the value of field1 if sub_field1 is not specified or doesn't exist
                elif k == field2 and field1_value is not None:
                    if sub_field2 and isinstance(v, dict) and field1_value is not None and sub_field2 in v:
                        v[sub_field2] = field1_value  # Set the value of the specified sub-field in field2 to the value of field1
                        new_dict[k] = v
                    else:
                        new_dict[k] = field1_value  # Set the value of field2 to the value of field1 if sub_field2 is not specified
                else:
                    new_dict[k] = replace_fieldA_with_fieldB(v, field1, field2, sub_field1, sub_field2)
            # If field2 doesn't exist in original data, add it
            if field2 not in data and field1_value is not None:
                new_dict[field2] = field1_value
            return new_dict
        elif isinstance(data, list):
            return [replace_fieldA_with_fieldB(item, field1, field2, sub_field1, sub_field2) for item in data]
        else:
            return data

    
    # Helper function to clear the value of a specified field
    def clear_field_value(data, field):
        if isinstance(data, dict):
            new_dict = {}
            for k, v in data.items():
                if k == field:
                    if isinstance(v, list):
                        new_dict[k] = []  # Clear the value of the specified field if it's a list
                    elif isinstance(v, dict):
                        new_dict[k] = {}  # Clear the value of the specified field if it's a dict
                    else:
                        new_dict[k] = None  # Clear the value of the specified field if it's a primitive type
                else:
                    new_dict[k] = clear_field_value(v, field)
            return new_dict
        elif isinstance(data, list):
            return [clear_field_value(item, field) for item in data]
        else:
            return data

    for json_name, json_data in json_data_dict.items():
        ## Delete all nestedsysId if the case is specified in the special_case_dict
        if special_case_dict.get("DEL_SYSID", False):
            json_data = delete_nested_field(json_data, "sysId")
        
        # Move value in "agent" to "agentVar" and clear "agent" value if the case is specified in the special_case_dict
        if special_case_dict.get("AGENT_TO_AGENTVAR", False):
            json_data = replace_fieldA_with_fieldB(json_data, "agent", "agentVar")
        
        # Move value in "credentials" to "credentialsVar" and clear "credentials" value if the case is specified in the special_case_dict
        if special_case_dict.get("CRED_TO_CREDVAR", False):
            json_data = replace_fieldA_with_fieldB(json_data, "credentials", "credentialsVar")
        
            
        # Move value in "ftpAgent" to "ftpAgentVar" and clear "ftpAgent" value if the case is specified in the special_case_dict
        if special_case_dict.get("FTP_AGENT_TO_FTP_AGENTVAR", False):
            json_data = replace_fieldA_with_fieldB(json_data, "primaryBrokerRef", "primaryBroker")
            json_data = replace_fieldA_with_fieldB(json_data, "secondaryBrokerRef", "secondaryBroker")
            
        # Move value in "ftpCredentials" to "ftpCredentialsVar" and clear "ftpCredentials" value if the case is specified in the special_case_dict
        if special_case_dict.get("FTP_CRED_TO_FTP_CREDVAR", False):
            json_data = replace_fieldA_with_fieldB(json_data, "primaryCredentials", "primaryCredVar")
            json_data = replace_fieldA_with_fieldB(json_data, "secondaryCredentials", "secondaryCredVar")
            
        # Move value in "passphrase" to "passphraseVar" and clear "passphrase" value if the case is specified in the special_case_dict
        if special_case_dict.get("PASSPHRASE_TO_PASSPHRASEVAR", False):
            json_data = replace_fieldA_with_fieldB(json_data, "credentialField1", "credentialVarField1", "value", "value")
        
        # Clear value in "emailNotifications" if the case is specified in the special_case_dict
        if special_case_dict.get("CLEAR_EMAIL_NOTIFICATION", False):
            json_data = clear_field_value(json_data, "emailNotifications")
        
        # Clear value in "businessService" if the case is specified in the special_case_dict
        if special_case_dict.get("CLEAR_BUSINESS_SERVICE", False):
            json_data = clear_field_value(json_data, "opswiseGroups")
        new_json_data_dict[json_name] = json_data
            
    return new_json_data_dict


def createMultiJSON(json_data_dict, output_path=None):
    created_json_log = []
    
    for json_name, json_data_list in json_data_dict.items():
        output_filename = f"{json_name}"
        createJson(output_filename, json_data_list, output_path=output_path)
        created_json_log.append({
            "filename": output_filename,
            "status": "Created",
            "details_log": json_data_list
        })
        print(f"Created JSON file: {output_filename}")
    df_created_log = pd.DataFrame(created_json_log)
    return df_created_log

def main():
    
    json_data_dict = readFolderJSONFiles(JSON_FOLDER)
    print(f"Read {len(json_data_dict)} JSON files from {JSON_FOLDER}")
    prepare_json_dict = prepareReplacedJSON(json_data_dict, REPLACE_TARGET, REPLACE_WORD_LIST)
    prepare_json_dict = prepareSpecialCases(prepare_json_dict, SPECIAL_CASE_DICT)
    print(f"Prepared replaced JSON data for {len(prepare_json_dict)} sets")
    for json_name in prepare_json_dict.keys():
        print(f"Ready to create JSON file: {json_name}")
    df_created_json_log = createMultiJSON(prepare_json_dict, output_path=OUTPUT_JSON_FOLDER)
    createExcel(OUTPUT_EXCEL_FILENAME, (OUTPUT_EXCEL_SHEETNAME, df_created_json_log))

if __name__ == "__main__":
    main()