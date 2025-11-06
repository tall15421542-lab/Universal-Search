"""
Configuration settings for Apache Flink jobs.
"""

import os
from typing import Dict, Any

# Flink Configuration
FLINK_CONFIG = {
    'checkpoint_interval': int(os.getenv('FLINK_CHECKPOINT_INTERVAL', '60000')),  # milliseconds
    'parser_parallelism': int(os.getenv('FLINK_PARSER_PARALLELISM', '2')),
    'chunker_parallelism': int(os.getenv('FLINK_CHUNKER_PARALLELISM', '4')),
    'state_backend': os.getenv('FLINK_STATE_BACKEND', 'rocksdb'),
}

def get_flink_config() -> Dict[str, Any]:
    """
    Get the Flink configuration.
    
    Returns:
        Dict containing the Flink configuration.
    """
    return FLINK_CONFIG.copy()

