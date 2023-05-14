# Constants and helpers

def dir_tags(path: str)-> list[str]:
    if len(path) < 2:
        return []
    return [t.lstrip('.') for t in path.strip('/').split('/')]

def file_tags(path: str) -> list[str]:
    path = path[:path.rindex('/')]
    if len(path) < 2:
        return []
    return [t.lstrip('.') for t in path.strip('/').split('/')]

def file_name(path: str) -> bool: 
    return path.split('/')[-1].strip('.')
