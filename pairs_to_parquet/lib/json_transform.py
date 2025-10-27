import json
import logging

from pairtools.lib import headerops



# header + (json) -> json
def header_to_json_dict(header: str, field_names: list) -> dict:
    """    
    From header extract necesary fields and create a dictionary with header types as keys and their JSON strings as values.

    Parameters
    -------
    header (str): The initial header string to process.
    field_names (list): A list of header types to extract.
        
    Returns:
    -------
    dict: A dictionary where keys are header fields, and values are JSON strings of the extracted fields.
    """
    header_json_dict = {}
    header_rest = header
    for field_name in field_names:
        fields, header_rest = headerops.extract_fields(header_rest, field_name, True)
        if field_name == "columns":
            fields_in_type=fields[0].split()
        elif field_name == "chromsize":
            fields_in_type={k:int(v) for k,v in [i.split(' ') for i in fields]}
        elif field_name == "samheader":
            fields_in_type = fields
        elif field_name=="sorted" or field_name=="shape" or field_name=="genome_assembly":
            fields_in_type = fields[0]
        else:
            fields_in_type = fields
        
        json_header_string = json.dumps(fields_in_type)
        json_check=json.loads(json_header_string)
        header_json_dict[field_name] = json_header_string

    if header_rest:
        fields_in_type = header_rest[0]
        json_header_string = json.dumps(fields_in_type)
        header_json_dict["format"] = json_header_string

    return header_json_dict

# json
def json_dict_to_json_str(header_json_dict: dict) -> str:
    """
    Generate a formatted metadata string from a dictionary.

    Parameters
    -------
    header_json_dict (dict): A dictionary containing key-value pairs to format.

    Returns
    -------
    str: A string where each key-value pair is formatted as `key: 'escaped_value'`,
             joined by a comma and newline.
    """
    json_str = {k.lstrip('# ').replace(':', '').replace(' ', ''):v for k,v in header_json_dict.items()}
    # json_check=json.loads(json_str["samheader"])
    # print(repr(f"Json check kv samheader: {json_check}"))
    return json_str


# json
def decode_and_parse_json(blob_value): 
    """
    Decodes a string with escape sequences and attempts to parse it as JSON.
    
    Parameters
    -------
    blob_value (str): The string to decode and parse.
        
    Returns
    -------
    dict | list | str: Parsed JSON object or error message.
    """
    try:
        # Decode escape sequences (e.g., \x22 -> "
        decoded_str = blob_value.encode("utf-8").decode("unicode_escape").replace('\\\\', '\\')

    except UnicodeDecodeError as e:
        return f"Unicode decode error: {e}, original value: {blob_value}"

    try:
        # Parse the decoded string as JSON
        return json.loads(decoded_str)
    except json.JSONDecodeError as e:
        # Log or handle the error case
        
        error_message = (
            f"JSONDecodeError: {e.msg} at line {e.lineno}, column {e.colno}. "
            )
        logging.error(error_message)
        return error_message
