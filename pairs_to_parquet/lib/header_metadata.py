

def extract_field_names(header):
    """
    Extract unique header types from a list of strings that start with '#' or '##'.

    Parameters
    -------
    header (list): List of strings to process.

    Returns:
    -------
    List of unique 'field_names' values.
    """

    field_names = set()  # To store unique values
    
    for line in header:
        field_name = line.split()[0].lstrip("#")[:-1]  # Remove # and last character
        if field_name!='': 
            field_names.add(field_name)

    # Return the unique values as a sorted list
    return list(field_names)


def extract_sorted_chromosome_field(chromsizes):
    """
    Extract chromosomes from chromsizes dict, sort them in lexicographic order and return as a tuple

    Parameters
    -------
    chromsizes (dict): dictionary in the form {chr1: 195471971, ...}

    Returns:
    -------
    tuple of chromosomes: ('chr1', 'chr2', 'chr3'...)
    """
    return tuple(sorted(chromsizes.keys()))


def metadata_dict_to_header_list(metadata_dict):
    """
    Converts a decoded Parquet metadata dictionary into a list of formatted header lines.

    Parameters
    -------
    metadata_dict (dict): keys are the field names, and each value is a decoded Python object from the JSON-formatted metadata value.

    Returns:
    -------
    header (list)
    """
    header =[]
    basic_field_names=["format",  "sorted", "shape", "genome_assembly", "chromsize", "samheader"]
    for key in basic_field_names:
        lines=[]
        values=metadata_dict[key]
        if key == "chromsize": #chromsize: chr1 195471971
            lines=["#"+key+": " +chr+" "+str(size) for chr, size in values.items()]
        elif key == "samheader":
            lines=["#"+key+": " +value for value in values]
        elif key=="sorted" or key=="shape" or key=="genome_assembly":
            lines=["#"+key+": " +values]
        elif key=="format": 
            lines=[values]
        header.extend(lines)

    # No more keys are expected, if there exists possibility of custom keys, please update

    lines=["#columns: "+ " ".join(metadata_dict["columns"])]
    header.extend(lines)
    return header


