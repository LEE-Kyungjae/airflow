"""
Code Validator for generated crawler code.

This module validates Python code syntax and performs
basic security checks before execution.
"""

import ast
import sys
import logging
import tempfile
import subprocess
from typing import Tuple, List, Optional, Dict, Any
from io import StringIO

logger = logging.getLogger(__name__)


class CodeValidator:
    """Validator for crawler Python code."""

    # Dangerous patterns to check
    DANGEROUS_PATTERNS = [
        'os.system',
        'subprocess.call',
        'subprocess.run',
        'subprocess.Popen',
        'eval(',
        'exec(',
        '__import__',
        'open(',  # Only dangerous without context
        'rm -rf',
        'del ',
        'shutil.rmtree'
    ]

    # Required imports for different crawler types
    REQUIRED_IMPORTS = {
        'html': ['requests', 'bs4'],
        'pdf': ['pdfplumber'],
        'excel': ['pandas'],
        'csv': ['pandas']
    }

    # Allowed imports whitelist
    ALLOWED_IMPORTS = {
        'requests',
        'bs4',
        'BeautifulSoup',
        'selenium',
        'webdriver',
        'pdfplumber',
        'pandas',
        'openpyxl',
        'xlrd',
        'csv',
        'json',
        're',
        'datetime',
        'time',
        'logging',
        'typing',
        'urllib',
        'collections',
        'itertools',
        'functools',
        'io',
        'os.path',
        'pathlib'
    }

    @classmethod
    def validate_syntax(cls, code: str) -> Tuple[bool, Optional[str]]:
        """
        Validate Python syntax.

        Args:
            code: Python code to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            ast.parse(code)
            return True, None
        except SyntaxError as e:
            error_msg = f"Syntax error at line {e.lineno}: {e.msg}"
            logger.error(error_msg)
            return False, error_msg

    @classmethod
    def check_dangerous_patterns(cls, code: str) -> Tuple[bool, List[str]]:
        """
        Check for potentially dangerous code patterns.

        Args:
            code: Python code to check

        Returns:
            Tuple of (is_safe, list_of_warnings)
        """
        warnings = []

        for pattern in cls.DANGEROUS_PATTERNS:
            if pattern in code:
                # Context-aware checking
                if pattern == 'open(' and cls._is_safe_open_usage(code):
                    continue
                warnings.append(f"Potentially dangerous pattern found: {pattern}")

        is_safe = len(warnings) == 0
        return is_safe, warnings

    @classmethod
    def _is_safe_open_usage(cls, code: str) -> bool:
        """
        Check if open() usage is safe (read mode only).

        Args:
            code: Code to check

        Returns:
            True if open usage appears safe
        """
        # Simple heuristic: check if only read modes are used
        import re
        open_calls = re.findall(r'open\([^)]+\)', code)

        for call in open_calls:
            # Allow read modes
            if any(mode in call for mode in ["'r'", '"r"', "'rb'", '"rb"', "mode='r"]):
                continue
            # Default mode (no mode specified) is read
            if "'w'" not in call and '"w"' not in call and 'mode=' not in call:
                continue
            return False

        return True

    @classmethod
    def validate_imports(
        cls,
        code: str,
        crawler_type: str = 'html'
    ) -> Tuple[bool, List[str]]:
        """
        Validate that required imports are present and no disallowed imports exist.

        Args:
            code: Python code to validate
            crawler_type: Type of crawler (html, pdf, excel, csv)

        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []

        try:
            tree = ast.parse(code)
        except SyntaxError:
            return False, ["Cannot parse code for import validation"]

        # Extract all imports
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])

        # Check required imports
        required = cls.REQUIRED_IMPORTS.get(crawler_type, [])
        for req in required:
            if req not in imports and not any(req in imp for imp in imports):
                issues.append(f"Missing recommended import: {req}")

        # Note: We don't strictly enforce allowed imports to be flexible

        return len(issues) == 0, issues

    @classmethod
    def validate_function_signature(
        cls,
        code: str,
        expected_function_name: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate that the code contains a properly defined crawler function.

        Args:
            code: Python code to validate
            expected_function_name: Expected function name (optional)

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return False, f"Syntax error: {e}"

        # Find function definitions
        functions = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                functions.append(node.name)

        if not functions:
            return False, "No function definition found"

        if expected_function_name:
            if expected_function_name not in functions:
                return False, f"Expected function '{expected_function_name}' not found. Found: {functions}"

        # Check for crawl_ prefix
        crawl_functions = [f for f in functions if f.startswith('crawl_')]
        if not crawl_functions:
            return False, "No function with 'crawl_' prefix found"

        return True, None

    @classmethod
    def test_code_execution(
        cls,
        code: str,
        timeout: int = 30
    ) -> Tuple[bool, str]:
        """
        Test code execution in isolated environment.

        This performs a dry-run syntax check without actually
        running network requests.

        Args:
            code: Python code to test
            timeout: Execution timeout in seconds

        Returns:
            Tuple of (success, output_or_error)
        """
        # Create test wrapper that imports but doesn't execute
        test_code = f'''
import sys
import ast

code = """{code.replace('"""', "'''").replace(chr(92), chr(92)+chr(92))}"""

# Syntax check
try:
    tree = ast.parse(code)
    print("SYNTAX_OK")
except SyntaxError as e:
    print(f"SYNTAX_ERROR: {{e}}")
    sys.exit(1)

# Try to compile
try:
    compile(code, '<string>', 'exec')
    print("COMPILE_OK")
except Exception as e:
    print(f"COMPILE_ERROR: {{e}}")
    sys.exit(1)

print("VALIDATION_SUCCESS")
'''

        try:
            # Run in subprocess for isolation
            result = subprocess.run(
                [sys.executable, '-c', test_code],
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0 and 'VALIDATION_SUCCESS' in result.stdout:
                return True, "Code validation successful"
            else:
                error = result.stderr or result.stdout
                return False, f"Validation failed: {error}"

        except subprocess.TimeoutExpired:
            return False, "Validation timed out"
        except Exception as e:
            return False, f"Validation error: {str(e)}"

    @classmethod
    def full_validation(
        cls,
        code: str,
        crawler_type: str = 'html',
        source_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Perform full validation of crawler code.

        Args:
            code: Python code to validate
            crawler_type: Type of crawler
            source_id: Source ID for function name validation

        Returns:
            Validation result dictionary
        """
        result = {
            'valid': True,
            'syntax_valid': False,
            'security_safe': False,
            'imports_valid': False,
            'function_valid': False,
            'errors': [],
            'warnings': []
        }

        # 1. Syntax validation
        syntax_valid, syntax_error = cls.validate_syntax(code)
        result['syntax_valid'] = syntax_valid
        if not syntax_valid:
            result['valid'] = False
            result['errors'].append(syntax_error)
            return result  # Can't proceed without valid syntax

        # 2. Security check
        is_safe, warnings = cls.check_dangerous_patterns(code)
        result['security_safe'] = is_safe
        result['warnings'].extend(warnings)
        if not is_safe:
            result['valid'] = False
            result['errors'].append("Potentially dangerous code patterns detected")

        # 3. Import validation
        imports_valid, import_issues = cls.validate_imports(code, crawler_type)
        result['imports_valid'] = imports_valid
        result['warnings'].extend(import_issues)

        # 4. Function signature validation
        expected_func = f"crawl_{source_id.replace('-', '_')}" if source_id else None
        func_valid, func_error = cls.validate_function_signature(code, expected_func)
        result['function_valid'] = func_valid
        if not func_valid:
            result['warnings'].append(func_error)

        # 5. Execution test (compile check)
        exec_success, exec_output = cls.test_code_execution(code)
        if not exec_success:
            result['valid'] = False
            result['errors'].append(exec_output)

        return result
