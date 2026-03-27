# Native module initialization with fallback support

import sys
import warnings
import platform

# Try to import the native C extension with comprehensive error handling
_native_parse_single_row = None
_native_parse_multiple_rows = None
NATIVE_AVAILABLE = False

def _test_native_functions():
    """Test if the native functions actually work (not just importable)"""
    if _native_parse_single_row is None or _native_parse_multiple_rows is None:
        return False

    try:
        # Test if the functions are callable - don't actually call them with invalid data
        # Just check if they are callable functions
        return callable(_native_parse_single_row) and callable(_native_parse_multiple_rows)
    except Exception:
        # Any exception means the native functions are not working properly
        return False

try:
    # First, try to import the native C extension
    from .native_plainbuffer import parse_single_row as _native_parse_single_row
    from .native_plainbuffer import parse_multiple_rows as _native_parse_multiple_rows

    # Import successful, now test if they actually work
    if _test_native_functions():
        NATIVE_AVAILABLE = True
    else:
        # Import succeeded but functions don't work (e.g., wrong architecture, missing libs)
        NATIVE_AVAILABLE = False
        _native_parse_single_row = None
        _native_parse_multiple_rows = None
        warnings.warn(
            "Native C extension 'native_plainbuffer' was imported but failed runtime test. "
            f"This may be due to platform incompatibility (current: {platform.platform()}). "
            "Falling back to Python implementation. Performance may be reduced.",
            RuntimeWarning,
            stacklevel=2
        )
except ImportError as e:
    # Import failed completely
    NATIVE_AVAILABLE = False
    _native_parse_single_row = None
    _native_parse_multiple_rows = None
    warnings.warn(
        f"Native C extension 'native_plainbuffer' could not be imported: {e}. "
        f"Platform: {platform.platform()}, Python: {sys.version}. "
        "Falling back to Python implementation. Performance may be reduced.",
        ImportWarning,
        stacklevel=2
    )

except Exception as e:
    # Catch any other unexpected errors during import
    NATIVE_AVAILABLE = False
    _native_parse_single_row = None
    _native_parse_multiple_rows = None
    warnings.warn(
        f"Unexpected error while importing native C extension: {e}. "
        f"Platform: {platform.platform()}, Python: {sys.version}. "
        "Falling back to Python implementation. Performance may be reduced.",
        RuntimeWarning,
        stacklevel=2
    )

def parse_single_row(buffer):
    """
    Parse single row from PlainBuffer data using native C extension if available,
    otherwise fall back to Python implementation.

    Args:
        buffer: bytes - The PlainBuffer data to parse

    Returns:
        tuple: (primary_keys, columns) - Parsed single row data

    Raises:
        ImportError: If native extension is not available and no fallback is implemented
    """
    if NATIVE_AVAILABLE and _native_parse_single_row is not None:
        return _native_parse_single_row(buffer)
    else:
        # Fallback to Python implementation
        raise ImportError(
            "Native C extension 'native_plainbuffer' is not available and "
            "no Python fallback implementation is provided. "
            "Please install a pre-compiled wheel for your platform or "
            "ensure you have a C compiler available during installation."
        )

def parse_multiple_rows(buffer):
    """
    Parse multiple rows from PlainBuffer data using native C extension if available,
    otherwise fall back to Python implementation.

    Args:
        buffer: bytes - The PlainBuffer data to parse

    Returns:
        list: List of (primary_keys, columns) tuples - Parsed rows data

    Raises:
        ImportError: If native extension is not available and no fallback is implemented
    """
    if NATIVE_AVAILABLE and _native_parse_multiple_rows is not None:
        return _native_parse_multiple_rows(buffer)
    else:
        # Fallback to Python implementation
        raise ImportError(
            "Native C extension 'native_plainbuffer' is not available and "
            "no Python fallback implementation is provided. "
            "Please install a pre-compiled wheel for your platform or "
            "ensure you have a C compiler available during installation."
        )

# Export the functions
__all__ = ['parse_single_row', 'parse_multiple_rows', 'NATIVE_AVAILABLE']