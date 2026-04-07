"""
Helper utilities for JSON handling with error safety and debugging features.
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

from ..config import Config

logger = logging.getLogger(__name__)


def load_json_safe(
    json_str: str, 
    context_name: str, 
    output_dir: Optional[Union[str, Path]] = None
) -> Any:
    """
    Safely load JSON string with error handling and dumping of failing content.
    
    If JSON parsing fails, the raw content is written to a file for debugging,
    and a neighboring exception info file is created.
    
    Args:
        json_str: The JSON string to parse
        context_name: A descriptive name for the context (used in filenames)
        output_dir: Directory to save error dumps (defaults to config location if None)
        
    Returns:
        The parsed JSON object
        
    Raises:
        json.JSONDecodeError: If parsing fails (after dumping info)
    """
    if not json_str:
        return None
        
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON at line {e.lineno}, column {e.colno}")
        cn = e.colno
        logger.error("content around error: %r", json_str[max(0,cn-100):cn+100])
        # Determine output directory
        if output_dir is None:
            try:
                # Try to get from config
                config = Config()
                # Parse the database URL to find the directory
                db_url = config.database.url
                if db_url.startswith('sqlite:///'):
                    db_path = db_url.replace('sqlite:///', '')
                    # Handle ~ expansion
                    if db_path.startswith('~'):
                        db_path = os.path.expanduser(db_path)
                    
                    output_dir = os.path.dirname(db_path)
                else:
                    # Fallback to home directory
                    output_dir = os.path.expanduser("~")
            except Exception:
                # Absolute fallback
                output_dir = "/tmp"
        
        # Ensure output directory exists
        output_path = Path(output_dir)
        try:
            output_path.mkdir(parents=True, exist_ok=True)
        except Exception:
            # Last ditch effort
            output_path = Path("/tmp")
            
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Clean context name for filename
        clean_context = context_name.replace(" ", "_").replace("/", "_")
        
        # Filenames
        base_name = f"json_error_{clean_context}_{timestamp}"
        dump_file = output_path / f"{base_name}.txt"
        error_file = output_path / f"{base_name}.error"
        
        # Dump content
        try:
            with open(dump_file, 'w', encoding='utf-8') as f:
                f.write(json_str)
                
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Context: {context_name}\n")
                f.write(f"Error: {str(e)}\n\n")
                f.write(f"Line: {e.lineno}, Column: {e.colno}, Char: {e.pos}\n")
                
            logger.error(f"JSON parsing failed. Raw content dumped to {dump_file}")
        except Exception as write_error:
            logger.error(f"Failed to write JSON error dump: {write_error}")
            
        # Re-raise the original exception
        raise e
