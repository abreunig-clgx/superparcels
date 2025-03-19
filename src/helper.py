import logging
import click

def parse_to_str_list(ctx, param, value):
    if not value:
        return None
    
    result = []
    if value[0].endswith(','):
        raise click.BadParameter("Invalid format. -- NO SPACES! eg. st1,st2,st3")
    
    
    
    for item in value[0].split(','):
        result.append(str(item))

    return result

def parse_to_int_list(ctx, param, value):
    if not value:
        return [200] # default distance
    
    
    result = []
    if value[0].endswith(','):
        raise click.BadParameter("Invalid format. -- NO SPACES! eg. st1,st2,st3")
    
    
    
    for item in value[0].split(','):
        result.append(int(item))

    return result
    

def setup_logger():
    """
    Set up and return a configured logger.

    This function creates a logger using Python's logging module, sets its level to INFO, defines a formatter that 
    includes the timestamp, process name, log level, and message, and attaches a StreamHandler to output logs to 
    the console.

    Returns:
        logging.Logger: A configured logger instance.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) # debug hard set for running within pytest debugger
    
    formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    return logger