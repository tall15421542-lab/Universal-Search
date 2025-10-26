"""
Configuration settings for storage adapters.
"""

import os
from typing import Dict, Any

# Storage Configuration
STORAGE_CONFIG = {
    'storage_type': os.getenv('STORAGE_TYPE', 'local'),
    'storage_root': os.getenv('STORAGE_ROOT', './storage'),
}

def get_storage_config() -> Dict[str, Any]:
    """
    Get the storage configuration.
    
    Returns:
        Dict containing the storage configuration.
    """
    return STORAGE_CONFIG.copy()

