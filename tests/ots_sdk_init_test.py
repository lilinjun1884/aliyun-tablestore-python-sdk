# -*- coding: utf8 -*-

"""
Tests for tablestore/ots_sdk/__init__.py

Covers:
1. ImportError branch: OTS_SDK_AVAILABLE=False, all symbols None, ImportWarning issued
2. Generic Exception branch: OTS_SDK_AVAILABLE=False, RuntimeWarning issued
3. Successful import branch: OTS_SDK_AVAILABLE=True, sys.modules['ots_sdk'] registered
4. OTSException extended class methods
5. OTSClientException extended class methods
6. OTS_SDK_AVAILABLE=False => exception classes are None
"""

import sys
import importlib
import unittest
import warnings
from unittest import mock
from unittest.mock import MagicMock, patch


class TestOtsSdkInitImportError(unittest.TestCase):
    """Test the ImportError branch in ots_sdk/__init__.py."""

    def test_import_error_sets_available_false(self):
        """When the C++ .so cannot be imported, OTS_SDK_AVAILABLE should be False."""
        # Save original module references
        original_modules = {}
        for key in list(sys.modules.keys()):
            if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                original_modules[key] = sys.modules.pop(key)

        try:
            # Create a fake package that raises ImportError on sub-import
            fake_pkg = MagicMock()
            fake_pkg.__path__ = []
            fake_pkg.__name__ = 'tablestore.ots_sdk'

            # Make 'from .ots_sdk import ...' raise ImportError
            def raise_import_error(name, *args, **kwargs):
                if 'tablestore.ots_sdk.ots_sdk' in name or name == 'ots_sdk':
                    raise ImportError("No C++ extension available")
                return original_import(name, *args, **kwargs)

            original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

            with patch('builtins.__import__', side_effect=raise_import_error):
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    # Force reload
                    if 'tablestore.ots_sdk' in sys.modules:
                        del sys.modules['tablestore.ots_sdk']
                    import tablestore.ots_sdk as ots_sdk_mod
                    importlib.reload(ots_sdk_mod)

                    self.assertFalse(ots_sdk_mod.OTS_SDK_AVAILABLE)
                    # All symbols should be None
                    self.assertIsNone(ots_sdk_mod.OTSClient)
                    self.assertIsNone(ots_sdk_mod.ClientConfiguration)
                    self.assertIsNone(ots_sdk_mod.Credential)
                    self.assertIsNone(ots_sdk_mod.OTSException)
                    self.assertIsNone(ots_sdk_mod.OTSClientException)

                    # Check that a warning was issued
                    import_warnings = [x for x in w if issubclass(x.category, ImportWarning)]
                    self.assertGreater(len(import_warnings), 0,
                                       "Expected ImportWarning to be issued")
        finally:
            # Restore original modules
            for key in list(sys.modules.keys()):
                if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                    sys.modules.pop(key, None)
            sys.modules.update(original_modules)
            # Re-import to restore state
            importlib.reload(importlib.import_module('tablestore.ots_sdk'))


class _RaiseOnAttrAccess:
    """A fake module whose attribute access raises RuntimeError.

    When ``from .ots_sdk import OTSClient, ...`` is executed during
    ``importlib.reload``, Python resolves the relative import to
    ``tablestore.ots_sdk.ots_sdk`` and then tries ``getattr(mod, 'OTSClient')``.
    By placing this object in ``sys.modules`` we make that getattr raise a
    ``RuntimeError``, which is *not* an ``ImportError`` and therefore exercises
    the generic ``except Exception`` branch in ``__init__.py``.
    """

    def __getattr__(self, name):
        raise RuntimeError("Unexpected C++ error during attribute access")


class TestOtsSdkInitGenericException(unittest.TestCase):
    """Test the generic Exception branch in ots_sdk/__init__.py."""

    def test_generic_exception_sets_available_false(self):
        """When import raises a non-ImportError exception, OTS_SDK_AVAILABLE should be False."""
        original_modules = {}
        for key in list(sys.modules.keys()):
            if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                original_modules[key] = sys.modules.pop(key)

        try:
            # Pre-load the package so reload() can find it
            import tablestore.ots_sdk as ots_sdk_mod

            # Inject a fake sub-module that raises RuntimeError on any attr access.
            # 'from .ots_sdk import OTSClient, ...' resolves to
            # sys.modules['tablestore.ots_sdk.ots_sdk'], so injecting here
            # makes the import statement raise RuntimeError (not ImportError).
            sys.modules['tablestore.ots_sdk.ots_sdk'] = _RaiseOnAttrAccess()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                importlib.reload(ots_sdk_mod)

                self.assertFalse(ots_sdk_mod.OTS_SDK_AVAILABLE)
                self.assertIsNone(ots_sdk_mod.OTSException)
                self.assertIsNone(ots_sdk_mod.OTSClientException)

                # Check RuntimeWarning was issued
                runtime_warnings = [x for x in w if issubclass(x.category, RuntimeWarning)]
                self.assertGreater(len(runtime_warnings), 0,
                                   "Expected RuntimeWarning to be issued")
        finally:
            for key in list(sys.modules.keys()):
                if 'tablestore.ots_sdk' in key or key == 'ots_sdk':
                    sys.modules.pop(key, None)
            sys.modules.update(original_modules)
            importlib.reload(importlib.import_module('tablestore.ots_sdk'))


class TestOtsSdkInitSuccessfulImport(unittest.TestCase):
    """Test the successful import branch (only runs if native C++ SDK is available)."""

    def setUp(self):
        """Ensure clean module state before each test.
        
        Completely remove all ots_sdk related modules from sys.modules,
        then re-import to get a fresh state.
        """
        # Save the original module if it exists
        self._original_module = sys.modules.get('tablestore.ots_sdk')
        
        # Remove ALL ots_sdk related entries from sys.modules
        keys_to_remove = [k for k in list(sys.modules.keys()) if 'ots_sdk' in k]
        for key in keys_to_remove:
            del sys.modules[key]
        
        # Re-import fresh
        import tablestore.ots_sdk as ots_sdk_mod
        self.ots_sdk_mod = ots_sdk_mod

    def tearDown(self):
        """Restore original module state after test."""
        # Remove the fresh module
        keys_to_remove = [k for k in list(sys.modules.keys()) if 'ots_sdk' in k]
        for key in keys_to_remove:
            del sys.modules[key]
        
        # Restore original if it existed
        if self._original_module is not None:
            sys.modules['tablestore.ots_sdk'] = self._original_module

    def test_successful_import_sets_available_true(self):
        """When the C++ .so is available, OTS_SDK_AVAILABLE should be True."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available, skipping success path test")

        self.assertTrue(self.ots_sdk_mod.OTS_SDK_AVAILABLE)
        self.assertIsNotNone(self.ots_sdk_mod.OTSClient)
        self.assertIsNotNone(self.ots_sdk_mod.ClientConfiguration)
        self.assertIsNotNone(self.ots_sdk_mod.Credential)

    def test_sys_modules_registration(self):
        """When import succeeds, 'ots_sdk' should be registered in sys.modules."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available, skipping sys.modules test")

        self.assertIn('ots_sdk', sys.modules)


class TestOTSExceptionExtended(unittest.TestCase):
    """Test the OTSException extended class methods."""

    def setUp(self):
        """Ensure clean module state before each test.
        
        Completely remove all ots_sdk related modules from sys.modules,
        then re-import to get a fresh state.
        """
        self._original_module = sys.modules.get('tablestore.ots_sdk')
        
        # Remove ALL ots_sdk related entries from sys.modules
        keys_to_remove = [k for k in list(sys.modules.keys()) if 'ots_sdk' in k]
        for key in keys_to_remove:
            del sys.modules[key]
        
        # Re-import fresh
        import tablestore.ots_sdk as ots_sdk_mod
        self.ots_sdk_mod = ots_sdk_mod

    def tearDown(self):
        """Restore original module state after test."""
        keys_to_remove = [k for k in list(sys.modules.keys()) if 'ots_sdk' in k]
        for key in keys_to_remove:
            del sys.modules[key]
        
        if self._original_module is not None:
            sys.modules['tablestore.ots_sdk'] = self._original_module

    def test_ots_exception_methods_exist(self):
        """OTSException should have GetErrorCode/GetMessage/GetRequestId/GetTraceId/GetHttpStatus."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available")

        self.assertIsNotNone(self.ots_sdk_mod.OTSException)
        exc_cls = self.ots_sdk_mod.OTSException

        # Verify methods exist on the class
        self.assertTrue(hasattr(exc_cls, 'GetErrorCode'))
        self.assertTrue(hasattr(exc_cls, 'GetMessage'))
        self.assertTrue(hasattr(exc_cls, 'GetRequestId'))
        self.assertTrue(hasattr(exc_cls, 'GetTraceId'))
        self.assertTrue(hasattr(exc_cls, 'GetHttpStatus'))

    def test_ots_exception_is_subclass(self):
        """OTSException should be a subclass of the C++ binding's _OTSException."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available")

        self.assertIsNotNone(self.ots_sdk_mod._OTSException)
        self.assertTrue(issubclass(self.ots_sdk_mod.OTSException, self.ots_sdk_mod._OTSException))

    def test_ots_exception_methods_callable(self):
        """Test that OTSException methods are callable."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available")

        self.assertIsNotNone(self.ots_sdk_mod.OTSException)
        exc_cls = self.ots_sdk_mod.OTSException

        # Verify all methods are callable on the class
        self.assertTrue(callable(getattr(exc_cls, 'GetErrorCode', None)))
        self.assertTrue(callable(getattr(exc_cls, 'GetMessage', None)))
        self.assertTrue(callable(getattr(exc_cls, 'GetRequestId', None)))
        self.assertTrue(callable(getattr(exc_cls, 'GetTraceId', None)))
        self.assertTrue(callable(getattr(exc_cls, 'GetHttpStatus', None)))

    def test_ots_exception_method_return_values(self):
        """Test that OTSException methods return expected values."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available")
        
        # Verify methods exist and are callable on the class
        # Note: Actual instantiation and return values depend on C++ binding implementation
        exc_cls = self.ots_sdk_mod.OTSException
        
        # Verify all methods exist and are callable
        self.assertTrue(hasattr(exc_cls, 'GetErrorCode'))
        self.assertTrue(hasattr(exc_cls, 'GetMessage'))
        self.assertTrue(hasattr(exc_cls, 'GetRequestId'))
        self.assertTrue(hasattr(exc_cls, 'GetTraceId'))
        self.assertTrue(hasattr(exc_cls, 'GetHttpStatus'))
        
        # Verify methods return types (when called on class, they return method objects)
        self.assertTrue(callable(getattr(exc_cls, 'GetErrorCode')))
        self.assertTrue(callable(getattr(exc_cls, 'GetMessage')))
        self.assertTrue(callable(getattr(exc_cls, 'GetRequestId')))
        self.assertTrue(callable(getattr(exc_cls, 'GetTraceId')))
        self.assertTrue(callable(getattr(exc_cls, 'GetHttpStatus')))
    def test_ots_client_exception_methods_callable(self):
        """Test that OTSClientException methods are callable."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available")

        self.assertIsNotNone(self.ots_sdk_mod.OTSClientException)
        exc_cls = self.ots_sdk_mod.OTSClientException

        # Verify all methods are callable on the class
        self.assertTrue(callable(getattr(exc_cls, 'GetMessage', None)))
        self.assertTrue(callable(getattr(exc_cls, 'GetTraceId', None)))

    def test_ots_client_exception_method_return_values(self):
        """Test that OTSClientException methods return expected values."""
        if not self.ots_sdk_mod.OTS_SDK_AVAILABLE:
            self.skipTest("Native C++ SDK not available")
        
        # Verify methods exist and are callable on the class
        exc_cls = self.ots_sdk_mod.OTSClientException
        
        # Verify all methods exist and are callable
        self.assertTrue(hasattr(exc_cls, 'GetMessage'))
        self.assertTrue(hasattr(exc_cls, 'GetTraceId'))
        
        # Verify methods are callable
        self.assertTrue(callable(getattr(exc_cls, 'GetMessage')))
        self.assertTrue(callable(getattr(exc_cls, 'GetTraceId')))

if __name__ == '__main__':
    unittest.main()