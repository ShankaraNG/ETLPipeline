import os

def load_properties():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(current_dir, '..', 'config', 'configuration.properties')
    properties = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        properties[key.strip()] = value.strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {config_file}")
    
    return properties
