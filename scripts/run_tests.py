#!/usr/bin/env python3
"""
Test runner script for all project tests.

This script provides an easy way to run the complete test suite with proper
configuration and reporting.
"""

import sys
import subprocess
import os
import unittest


def run_streaming_job_tests():
    """Run streaming job tests using unittest."""
    print("Running Drive Streaming Job Tests...")
    print("=" * 50)
    
    # Import streaming job tests
    from tests.test_streaming_job import TestDriveStreamingJob, TestDriveStreamingJobIntegration
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestDriveStreamingJob))
    suite.addTests(loader.loadTestsFromTestCase(TestDriveStreamingJobIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("✅ Streaming job tests passed!")
    else:
        print(f"❌ Streaming job tests failed: {len(result.failures)} failures, {len(result.errors)} errors")
    
    return result.wasSuccessful()


def run_pytest_tests():
    """Run pytest tests for drive client and kafka producer."""
    print("Running Drive Client and Kafka Producer Tests...")
    print("=" * 50)
    
    # Run pytest with verbose output for all test files
    cmd = [
        sys.executable, "-m", "pytest", 
        "tests/test_drive_client.py", 
        "tests/test_kafka_producer.py",
        "-v", 
        "--tb=short",
        "--color=yes"
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 50)
        print("✅ Drive client and Kafka producer tests passed!")
        return True
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 50)
        print(f"❌ Drive client and Kafka producer tests failed!")
        return False
    except FileNotFoundError:
        print("❌ pytest not found. Please install it with: pip install pytest")
        return False


def run_all_tests():
    """Run the complete test suite."""
    print("Running Complete Test Suite...")
    print("=" * 60)
    
    # Ensure we're in the correct directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Run streaming job tests
    streaming_success = run_streaming_job_tests()
    
    print("\n" + "=" * 60)
    
    # Run pytest tests
    pytest_success = run_pytest_tests()
    
    # Final summary
    print("\n" + "=" * 60)
    if streaming_success and pytest_success:
        print("🎉 All tests passed!")
        return 0
    else:
        print("💥 Some tests failed!")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
