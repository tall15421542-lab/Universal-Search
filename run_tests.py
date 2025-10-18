#!/usr/bin/env python3
"""
Test runner script for Google Drive client tests.

This script provides an easy way to run the test suite with proper
configuration and reporting.
"""

import sys
import subprocess
import os


def run_tests():
    """Run the test suite with pytest."""
    print("Running Google Drive client and Kafka producer tests...")
    print("=" * 50)
    
    # Ensure we're in the correct directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Run pytest with verbose output for all test files
    cmd = [
        sys.executable, "-m", "pytest", 
        "test_drive_client.py", 
        "test_kafka_producer.py",
        "-v", 
        "--tb=short",
        "--color=yes"
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 50)
        print("✅ All tests passed!")
        return 0
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 50)
        print("❌ Some tests failed!")
        return e.returncode
    except FileNotFoundError:
        print("❌ pytest not found. Please install it with: pip install pytest")
        return 1


if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)
