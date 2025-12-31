"""
Integration tests for Warabi Python bindings.

Run with: python -m pytest test_warabi.py
"""

import unittest
import mochi.margo
from mochi.margo import Engine
from mochi.warabi.client import Client, TargetHandle, RegionID, AsyncRequest, AsyncCreateRequest
from mochi.warabi.server import Provider


class TestWarabiMemoryBackend(unittest.TestCase):
    """Test Warabi with memory backend."""

    def setUp(self):
        """Set up test fixtures."""
        # Create PyMargo engine
        self.engine = Engine("na+sm", mochi.margo.server)

        # Create Warabi provider with memory backend
        self.provider = Provider(
            engine=self.engine,
            provider_id=42,
            config={
                "target": {
                    "type": "memory"
                }
            }
        )

        # Create Warabi client
        self.client = Client(engine=self.engine)

        # Create target handle
        self.target = self.client.make_target_handle(
            address=str(self.engine.addr()),
            provider_id=42
        )

    def tearDown(self):
        """Clean up."""
        # Engine cleanup happens automatically
        pass

    def test_create_region(self):
        """Test creating a region."""
        region = self.target.create(size=1024)
        self.assertIsInstance(region, RegionID)

    def test_write_and_read(self):
        """Test writing and reading data."""
        # Create a region
        region = self.target.create(size=1024)

        # Write data
        data = b"Hello, Warabi!"
        self.target.write(region, offset=0, data=data, persist=False)

        # Read data back
        result = self.target.read(region, offset=0, size=len(data))
        self.assertEqual(result, data)

    def test_create_and_write(self):
        """Test combined create and write."""
        data = b"Combined operation!"
        region = self.target.create_and_write(data, persist=False)

        # Read back
        result = self.target.read(region, offset=0, size=len(data))
        self.assertEqual(result, data)

    def test_read_into_buffer(self):
        """Test reading into a pre-allocated buffer."""
        # Create and write
        data = b"Buffer test data"
        region = self.target.create_and_write(data)

        # Read into bytearray
        buffer = bytearray(len(data))
        self.target.read_into(region, offset=0, buffer=buffer)
        self.assertEqual(bytes(buffer), data)

    def test_offset_operations(self):
        """Test reading and writing at offsets."""
        # Create a 1KB region
        region = self.target.create(size=1024)

        # Write data at offset 100
        data1 = b"First chunk"
        self.target.write(region, offset=100, data=data1)

        # Write data at offset 500
        data2 = b"Second chunk"
        self.target.write(region, offset=500, data=data2)

        # Read back both chunks
        result1 = self.target.read(region, offset=100, size=len(data1))
        result2 = self.target.read(region, offset=500, size=len(data2))

        self.assertEqual(result1, data1)
        self.assertEqual(result2, data2)

    def test_erase_region(self):
        """Test erasing a region."""
        # Create and write
        data = b"To be erased"
        region = self.target.create_and_write(data)

        # Erase
        self.target.erase(region)

        # Note: After erase, reading should fail or return error
        # This depends on backend implementation

    def test_persist_operation(self):
        """Test persist operation."""
        # Create a region and write data
        region = self.target.create(size=1024)
        data = b"Data to persist"
        self.target.write(region, offset=0, data=data, persist=False)

        # Explicitly persist
        self.target.persist(region, offset=0, size=len(data))

        # Read back
        result = self.target.read(region, offset=0, size=len(data))
        self.assertEqual(result, data)

    def test_async_operations(self):
        """Test asynchronous operations."""
        # Async create
        create_req = self.target.create_async(size=1024)
        region = create_req.wait()  # wait() returns the RegionID

        # Async write
        data = b"Async data"
        req = self.target.write_async(region, offset=0, data=data, persist=False)
        req.wait()

        # Async read
        buffer = bytearray(len(data))
        req = self.target.read_async(region, offset=0, buffer=buffer)
        req.wait()

        self.assertEqual(bytes(buffer), data)

        # Async persist
        req = self.target.persist_async(region, offset=0, size=len(data))
        req.wait()

        # Async erase
        req = self.target.erase_async(region)
        req.wait()

    def test_region_id_serialization(self):
        """Test RegionID to bytes conversion."""
        region = self.target.create(size=256)

        # Convert to bytes
        region_bytes = region.to_bytes()
        self.assertEqual(len(region_bytes), 16)

        # Create RegionID from bytes
        region2 = RegionID(region_bytes)
        self.assertEqual(region, region2)

    def test_large_data(self):
        """Test with larger data (uses bulk transfer)."""
        # Create 1MB of data
        data = b"X" * (1024 * 1024)
        region = self.target.create_and_write(data, persist=False)

        # Read back
        result = self.target.read(region, offset=0, size=len(data))
        self.assertEqual(result, data)

    def test_eager_thresholds(self):
        """Test setting eager read/write thresholds."""
        # Set thresholds
        self.target.set_eager_write_threshold(4096)
        self.target.set_eager_read_threshold(4096)

        # Normal operations should still work
        data = b"Threshold test"
        region = self.target.create_and_write(data)
        result = self.target.read(region, offset=0, size=len(data))
        self.assertEqual(result, data)

    def test_multiple_regions(self):
        """Test managing multiple regions."""
        regions = []
        for i in range(10):
            data = f"Region {i}".encode()
            region = self.target.create_and_write(data)
            regions.append((region, data))

        # Verify all regions
        for region, expected_data in regions:
            result = self.target.read(region, offset=0, size=len(expected_data))
            self.assertEqual(result, expected_data)


class TestWarabiWithNumpy(unittest.TestCase):
    """Test Warabi with NumPy arrays (if available)."""

    def setUp(self):
        """Set up test fixtures."""
        try:
            import numpy as np
            self.np = np
        except ImportError:
            self.skipTest("NumPy not available")

        # Create PyMargo engine
        self.engine = Engine("na+sm", mochi.margo.server)

        # Create Warabi provider
        self.provider = Provider(
            engine=self.engine,
            provider_id=43,
            config={"target": {"type": "memory"}}
        )

        # Create client and target
        self.client = Client(engine=self.engine)
        self.target = self.client.make_target_handle(
            address=str(self.engine.addr()),
            provider_id=43
        )

    def test_numpy_array_write_read(self):
        """Test writing and reading NumPy arrays."""
        # Create NumPy array
        array = self.np.arange(100, dtype=self.np.float64)

        # Write to Warabi
        region = self.target.create_and_write(array.tobytes())

        # Read back
        result_bytes = self.target.read(region, offset=0, size=array.nbytes)
        result_array = self.np.frombuffer(result_bytes, dtype=self.np.float64)

        self.np.testing.assert_array_equal(array, result_array)

    def test_numpy_read_into(self):
        """Test reading directly into NumPy array."""
        # Create and write data
        array = self.np.arange(50, dtype=self.np.int32)
        region = self.target.create_and_write(array.tobytes())

        # Read into new array
        result_array = self.np.zeros(50, dtype=self.np.int32)
        self.target.read_into(region, offset=0, buffer=result_array)

        self.np.testing.assert_array_equal(array, result_array)


def test_provider_config():
    """Test provider configuration retrieval."""
    engine = Engine("na+sm", mochi.margo.server)

    provider = Provider(
        engine=engine,
        provider_id=44,
        config={
            "target": {
                "type": "memory"
            },
            "transfer_manager": {
                "type": "__default__"
            }
        }
    )

    # Get config back
    config = provider.get_config()
    assert "target" in config
    assert config["target"]["type"] == "memory"


def test_client_config():
    """Test client configuration retrieval."""
    engine = Engine("na+sm", mochi.margo.client)
    client = Client(engine=engine)

    config = client.get_config()
    # Config should be a valid JSON string
    assert isinstance(config, str)


if __name__ == "__main__":
    unittest.main()
