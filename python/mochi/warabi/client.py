"""
Warabi client module.

This module provides the Python interface to Warabi client functionality,
including creating clients, target handles, and performing blob storage operations.

Example
-------
>>> import mochi.margo
>>> from mochi.warabi.client import Client
>>>
>>> # Create a PyMargo engine
>>> engine = mochi.margo.Engine("na+sm", mochi.margo.client)
>>>
>>> # Create a Warabi client
>>> client = Client(engine)
>>>
>>> # Connect to a target
>>> target = client.make_target_handle("na+sm://12345", 0)
>>>
>>> # Create a region and write data
>>> data = b"Hello, Warabi!"
>>> region = target.create_and_write(data, persist=True)
>>>
>>> # Read data back
>>> result = target.read(region, 0, len(data))
>>> assert result == data
"""

import _pywarabi_client

# Re-export compiled classes
Client = _pywarabi_client.Client
TargetHandle = _pywarabi_client.TargetHandle
RegionID = _pywarabi_client.RegionID
AsyncRequest = _pywarabi_client.AsyncRequest
AsyncCreateRequest = _pywarabi_client.AsyncCreateRequest
Exception = _pywarabi_client.Exception

__all__ = [
    'Client',
    'TargetHandle',
    'RegionID',
    'AsyncRequest',
    'AsyncCreateRequest',
    'Exception',
]
