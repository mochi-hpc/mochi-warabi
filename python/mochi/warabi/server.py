"""
Warabi server module.

This module provides the Python interface to Warabi server (provider) functionality.

Example
-------
>>> import mochi.margo
>>> from mochi.warabi.server import Provider
>>>
>>> # Create a PyMargo engine
>>> engine = mochi.margo.Engine("na+sm", mochi.margo.server)
>>>
>>> # Create a Warabi provider with memory backend
>>> provider = Provider(engine, 0, {
...     "target": {
...         "type": "memory"
...     }
... })
>>>
>>> # Or with persistent ABT-IO backend
>>> provider = Provider(engine, 1, {
...     "target": {
...         "type": "abtio",
...         "config": {
...             "path": "/tmp/warabi_data.dat",
...             "create_if_missing": True
...         }
...     }
... })
>>>
>>> # Start serving requests
>>> engine.wait_for_finalize()
"""

import _pywarabi_server

# Re-export compiled classes
Provider = _pywarabi_server.Provider
Exception = _pywarabi_server.Exception

__all__ = [
    'Provider',
    'Exception',
]
