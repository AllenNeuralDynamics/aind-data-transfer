import datetime
import json
from pathlib import Path
from typing import Union
import xml.etree.ElementTree as ET

def read_json(json_path: Union[Path, str]) -> dict: 
    with open(json_path) as f:
        return json.load(f)

def read_log_file(log_path: str) -> dict: 
    with open(log_path, 'r') as f: 
        lines = f.readlines()

    log_dict = {}
    log_dict['tiles']: list[dict] = []
    for i, line in enumerate(lines): 
        line = line.replace("\'", "\"")
        tmp = json.loads(f'{line}')
        
        # Only specific lines matter:
        if i == 0: 
            log_dict['session_start_time'] = datetime.datetime.fromisoformat(tmp['created_strftime'])
        if 'local_storage_directory' in tmp.keys():
            tmp.pop('name')
            tmp.pop('msg')
            tmp.pop('levelname')
            tmp.pop('created')
            tmp.pop('created_strftime')
            log_dict = {**log_dict, **tmp}
        if 'file_name' in tmp.keys():
            tmp.pop('name')
            tmp.pop('msg')
            tmp.pop('levelname')
            tmp.pop('created')
            tmp.pop('created_strftime')
            log_dict['tiles'].append(tmp)
        if i == len(lines) - 1: 
            log_dict['session_end_time'] = datetime.datetime.fromisoformat(tmp['created_strftime'])

    return log_dict

def write_xml(tree: ET.ElementTree, path: Union[Path, str]) -> None: 
    ET.indent(tree, space="\t", level=0)
    tree.write(path, encoding="utf-8")