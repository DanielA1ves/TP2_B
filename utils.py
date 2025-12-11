import re

def sanitize_tag(name: str) -> str:
    """
    Sanitizes a string to be a valid XML tag name.
    Replaces invalid characters with underscores.
    Ensures the name starts with a valid start character.
    """
    # Replace invalid characters (anything not alphanumeric, underscore, hyphen, dot)
    # XML tags cannot contain spaces, slashes, etc.
    sanitized = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', str(name))
    
    # Ensure it doesn't start with a number, hyphen, or dot (though dot is technically allowed in some positions, simplified here)
    # XML names must start with a letter or underscore.
    if not sanitized or not re.match(r'^[a-zA-Z_]', sanitized):
        sanitized = f"_{sanitized}"
        
    return sanitized

def get_unique_tag_map(columns: list) -> dict:
    """
    Returns a dict mapping original column names to unique sanitized tag names.
    Resolves collisions by appending numbers.
    """
    seen = {}
    mapping = {}
    for col in columns:
        base = sanitize_tag(col)
        tag = base
        count = 1
        while tag in seen:
            tag = f"{base}_{count}"
            count += 1
        seen[tag] = True
        mapping[col] = tag
    return mapping

def clean_xml_value(value) -> str:
    """
    Removes invalid XML 1.0 characters from value string.
    """
    s = str(value)
    # Remove control characters 0-31 except 9(\t), 10(\n), 13(\r)
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', s)
