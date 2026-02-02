import ast
import sys
import importlib.util

PACKAGE_MAPPINGS = {
    "sklearn": "scikit-learn",
    "cv2": "opencv-python",
    "PIL": "Pillow", 
    "yaml": "PyYAML"
}

def get_missing_imports(code: str) -> list[str]:
    """
    Fast scan to see if code needs packages we don't have yet.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []

    required_modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                required_modules.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                required_modules.add(node.module.split('.')[0])

    missing = []
    stdlib = sys.stdlib_module_names 

    for module in required_modules:
        if module in stdlib: 
            continue
        # Check if it exists in the CURRENT environment (filesystem)
        if importlib.util.find_spec(module) is None:
            install_name = PACKAGE_MAPPINGS.get(module, module)
            missing.append(install_name)

    return list(set(missing))