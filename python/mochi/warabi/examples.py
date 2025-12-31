"""
Warabi Python Bindings - Usage Examples

This file demonstrates various usage patterns for the Warabi Python bindings.
"""

import mochi.margo
from mochi.warabi.client import Client
from mochi.warabi.server import Provider


def example_basic_usage():
    """Basic usage example with memory backend."""
    print("=== Basic Usage Example ===\n")

    # Create PyMargo engine in server mode
    engine = mochi.margo.Engine("na+sm", mochi.margo.server)
    print(f"Created engine at address: {engine.addr()}")

    # Create a Warabi provider with memory backend
    provider = Provider(
        engine=engine,
        provider_id=0,
        config={
            "target": {
                "type": "memory"
            }
        }
    )
    print("Created Warabi provider with memory backend")

    # Create a client
    client = Client(engine=engine)
    print("Created Warabi client")

    # Get a handle to the target
    target = client.make_target_handle(
        address=str(engine.addr()),
        provider_id=0
    )
    print("Created target handle\n")

    # Store some data
    data = b"Hello from Warabi Python bindings!"
    print(f"Writing data: {data}")
    region = target.create_and_write(data, persist=False)
    print(f"Created region: {region}\n")

    # Read it back
    result = target.read(region, offset=0, size=len(data))
    print(f"Read data: {result}")
    assert result == data
    print("✓ Data matches!\n")

    # Cleanup
    target.erase(region)
    print("Region erased")


def example_persistent_storage():
    """Example using persistent ABT-IO backend."""
    print("=== Persistent Storage Example ===\n")

    import tempfile
    import os

    # Create a temporary file for storage
    tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".warabi")
    tmpfile.close()
    storage_path = tmpfile.name
    print(f"Using storage file: {storage_path}")

    try:
        # Create engine and provider
        engine = mochi.margo.Engine("na+sm", mochi.margo.server)

        provider = Provider(
            engine=engine,
            provider_id=1,
            config={
                "target": {
                    "type": "abtio",
                    "config": {
                        "path": storage_path,
                        "create_if_missing": True,
                        "override_if_exists": True
                    }
                }
            }
        )
        print("Created provider with ABT-IO backend")

        # Create client and target
        client = Client(engine=engine)
        target = client.make_target_handle(str(engine.addr()), 1)

        # Write persistent data
        data = b"This data is persisted to disk!"
        print(f"Writing persistent data: {data}")
        region = target.create_and_write(data, persist=True)

        # Read it back
        result = target.read(region, offset=0, size=len(data))
        assert result == data
        print("✓ Data successfully persisted and read back!\n")

        # Check file size
        file_size = os.path.getsize(storage_path)
        print(f"Storage file size: {file_size} bytes")

    finally:
        # Cleanup
        if os.path.exists(storage_path):
            os.unlink(storage_path)
            print(f"Cleaned up {storage_path}")


def example_async_operations():
    """Example demonstrating asynchronous operations."""
    print("=== Async Operations Example ===\n")

    engine = mochi.margo.Engine("na+sm", mochi.margo.server)

    provider = Provider(
        engine=engine,
        provider_id=2,
        config={"target": {"type": "memory"}}
    )

    client = Client(engine=engine)
    target = client.make_target_handle(str(engine.addr()), 2)

    # Async create
    print("Creating region asynchronously...")
    create_req = target.create_async(size=4096)
    print("Create request issued (non-blocking)")
    region = create_req.wait()  # wait() returns the RegionID
    print(f"✓ Region created: {region}\n")

    # Async write
    data = b"Async write test data"
    print(f"Writing data asynchronously: {data}")
    req = target.write_async(region, offset=0, data=data, persist=False)
    print("Write request issued (non-blocking)")
    req.wait()
    print("✓ Write completed\n")

    # Async read
    buffer = bytearray(len(data))
    print("Reading data asynchronously...")
    req = target.read_async(region, offset=0, buffer=buffer)
    print("Read request issued (non-blocking)")
    req.wait()
    print(f"✓ Read completed: {bytes(buffer)}\n")

    assert bytes(buffer) == data

    # Async persist
    print("Persisting data asynchronously...")
    req = target.persist_async(region, offset=0, size=len(data))
    print("Persist request issued (non-blocking)")
    req.wait()
    print("✓ Persist completed\n")

    # Async erase
    print("Erasing region asynchronously...")
    req = target.erase_async(region)
    print("Erase request issued (non-blocking)")
    req.wait()
    print("✓ Erase completed\n")

    print("✓ All async operations successful!")


def example_bulk_transfer():
    """Example with large data using bulk transfer."""
    print("=== Bulk Transfer Example ===\n")

    engine = mochi.margo.Engine("na+sm", mochi.margo.server)

    provider = Provider(
        engine=engine,
        provider_id=3,
        config={"target": {"type": "memory"}}
    )

    client = Client(engine=engine)
    target = client.make_target_handle(str(engine.addr()), 3)

    # Create 10MB of data
    size_mb = 10
    data = b"X" * (size_mb * 1024 * 1024)
    print(f"Creating {size_mb}MB of data...")

    # Write using bulk transfer
    print("Writing via RDMA bulk transfer...")
    region = target.create_and_write(data, persist=False)
    print("✓ Write completed")

    # Read back using bulk transfer
    print("Reading via RDMA bulk transfer...")
    result = target.read(region, offset=0, size=len(data))
    print("✓ Read completed")

    assert result == data
    print(f"✓ Successfully transferred {size_mb}MB using bulk operations!")


def example_multiple_regions():
    """Example managing multiple regions."""
    print("=== Multiple Regions Example ===\n")

    engine = mochi.margo.Engine("na+sm", mochi.margo.server)

    provider = Provider(
        engine=engine,
        provider_id=4,
        config={"target": {"type": "memory"}}
    )

    client = Client(engine=engine)
    target = client.make_target_handle(str(engine.addr()), 4)

    # Create multiple regions
    regions = {}
    for i in range(10):
        name = f"object_{i}"
        data = f"Data for {name}".encode()
        region = target.create_and_write(data)
        regions[name] = (region, data)
        print(f"Created {name}: {region}")

    print(f"\n✓ Created {len(regions)} regions\n")

    # Access them in random order
    import random
    names = list(regions.keys())
    random.shuffle(names)

    print("Reading regions in random order:")
    for name in names:
        region, expected_data = regions[name]
        result = target.read(region, offset=0, size=len(expected_data))
        assert result == expected_data
        print(f"  {name}: ✓")

    print("\n✓ All regions verified!")

    # Cleanup
    print("\nCleaning up regions...")
    for name, (region, _) in regions.items():
        target.erase(region)
    print("✓ All regions erased")


def example_with_numpy():
    """Example using NumPy arrays (requires numpy)."""
    print("=== NumPy Integration Example ===\n")

    try:
        import numpy as np
    except ImportError:
        print("NumPy not available, skipping example")
        return

    engine = mochi.margo.Engine("na+sm", mochi.margo.server)

    provider = Provider(
        engine=engine,
        provider_id=5,
        config={"target": {"type": "memory"}}
    )

    client = Client(engine=engine)
    target = client.make_target_handle(str(engine.addr()), 5)

    # Create a NumPy array
    array = np.arange(1000, dtype=np.float64)
    print(f"Created NumPy array: shape={array.shape}, dtype={array.dtype}")

    # Store it in Warabi
    region = target.create_and_write(array.tobytes())
    print(f"Stored array in region: {region}")

    # Read back into a new array
    result_array = np.zeros_like(array)
    target.read_into(region, offset=0, buffer=result_array)
    print("Read array back from storage")

    # Verify
    assert np.array_equal(array, result_array)
    print("✓ Arrays match!")

    # Performance test
    print("\nPerformance test with 100MB array...")
    large_array = np.random.rand(100 * 1024 * 1024 // 8)  # 100MB
    print(f"Array size: {large_array.nbytes / 1024 / 1024:.2f} MB")

    import time
    start = time.time()
    region = target.create_and_write(large_array.tobytes())
    write_time = time.time() - start

    start = time.time()
    result = target.read(region, offset=0, size=large_array.nbytes)
    read_time = time.time() - start

    print(f"Write: {write_time:.3f}s ({large_array.nbytes / 1024 / 1024 / write_time:.2f} MB/s)")
    print(f"Read:  {read_time:.3f}s ({large_array.nbytes / 1024 / 1024 / read_time:.2f} MB/s)")


def example_pipeline_transfer_manager():
    """Example using pipeline transfer manager for better performance."""
    print("=== Pipeline Transfer Manager Example ===\n")

    engine = mochi.margo.Engine("na+sm", mochi.margo.server)

    provider = Provider(
        engine=engine,
        provider_id=6,
        config={
            "target": {
                "type": "memory"
            },
            "transfer_manager": {
                "type": "pipeline",
                "config": {
                    "num_pools": 2,
                    "num_buffers_per_pool": 8,
                    "first_buffer_size": 1024,
                    "buffer_size_multiplier": 2
                }
            }
        }
    )
    print("Created provider with pipeline transfer manager")

    client = Client(engine=engine)
    target = client.make_target_handle(str(engine.addr()), 6)

    # Transfer large data
    data = b"P" * (5 * 1024 * 1024)  # 5MB
    print(f"Transferring {len(data) / 1024 / 1024:.2f}MB with pipeline...")

    region = target.create_and_write(data)
    result = target.read(region, offset=0, size=len(data))

    assert result == data
    print("✓ Pipeline transfer successful!")


if __name__ == "__main__":
    examples = [
        ("Basic Usage", example_basic_usage),
        ("Persistent Storage", example_persistent_storage),
        ("Async Operations", example_async_operations),
        ("Bulk Transfer", example_bulk_transfer),
        ("Multiple Regions", example_multiple_regions),
        ("NumPy Integration", example_with_numpy),
        ("Pipeline Transfer Manager", example_pipeline_transfer_manager),
    ]

    for name, func in examples:
        print("\n" + "=" * 60)
        try:
            func()
        except Exception as e:
            print(f"❌ {name} failed: {e}")
            import traceback
            traceback.print_exc()
        print("=" * 60 + "\n")
