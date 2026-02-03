import ast
import sys
import importlib.util
import logging

log = logging.getLogger(__name__)

# --- EXPANDED MAPPINGS ---
PACKAGE_MAPPINGS = {
    # Data Science / Image
    "sklearn": "scikit-learn",
    "cv2": "opencv-python",
    "PIL": "Pillow", 
    "skimage": "scikit-image",
    
    # Utilities
    "yaml": "PyYAML",
    "bs4": "beautifulsoup4",
    "dotenv": "python-dotenv",
    "dateutil": "python-dateutil",
    "requests": "requests",
    "httpx": "httpx",
    
    # PDF / Text
    "pdfminer": "pdfminer.six",
    "fitz": "pymupdf",
    "docx": "python-docx",
    "pptx": "python-pptx"
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
        # log.info(f"- required module: {module}")
        if module in stdlib:
            # log.info(f"{module} found in stdlib") 
            continue
            
        # Check if it exists in the CURRENT environment
        if importlib.util.find_spec(module) is None:
            # Check our mapping, otherwise assume PackageName == ImportName
            install_name = PACKAGE_MAPPINGS.get(module)
            if not install_name:
                raise ValueError(f"Package '{module}' is not approved for auto-installation.")
            missing.append(install_name)
    if missing:
        log.info(f"missing: {missing}")
    return list(set(missing))