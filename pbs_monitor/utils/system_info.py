"""
System detection utilities for PBS Monitor
"""

import os
import socket
from typing import Optional

# Known ALCF systems and their hostname patterns
ALCF_SYSTEMS = {
    'aurora': 'Aurora',
    'polaris': 'Polaris',
    'sophia': 'Sophia',
    'sunspot': 'Sunspot',
    'cooley': 'Cooley',
    'theta': 'Theta',
}


def get_system_name() -> str:
    """
    Detect the HPC system name from hostname or environment.

    Detection order:
    1. PBS_SYSTEM environment variable (user override)
    2. Hostname-based detection for known systems
    3. Falls back to 'Unknown' if detection fails

    Returns:
        System name (e.g., 'Aurora', 'Polaris')
    """
    # Check for explicit environment variable override
    env_system = os.environ.get('PBS_SYSTEM')
    if env_system:
        return env_system

    # Try hostname-based detection
    try:
        hostname = socket.gethostname().lower()

        # Check for known system names in hostname
        for pattern, system_name in ALCF_SYSTEMS.items():
            if pattern in hostname:
                return system_name

        # Aurora compute nodes start with 'x' (e.g., x1234c0s0b0n0)
        if hostname.startswith('x') and len(hostname) > 10:
            return 'Aurora'

    except Exception:
        pass

    return 'Unknown'


def add_system_label(
    ax,
    system_name: Optional[str] = None,
    position: str = 'bottom_right',
    fontsize: int = 20,
    alpha: float = 0.8
) -> None:
    """
    Add a subtle system label to a matplotlib axes.

    Args:
        ax: Matplotlib axes object
        system_name: System name to display (auto-detected if None)
        position: Label position ('bottom_right', 'bottom_left', 'top_right', 'top_left')
        fontsize: Font size for the label
        alpha: Transparency of the label (0-1)
    """
    if system_name is None:
        system_name = get_system_name()

    # Position coordinates (in axes fraction)
    positions = {
        'bottom_right': (0.98, 0.02),
        'bottom_left': (0.02, 0.02),
        'top_right': (0.98, 0.98),
        'top_left': (0.02, 0.98),
    }

    x, y = positions.get(position, positions['bottom_right'])

    # Horizontal alignment based on position
    ha = 'right' if 'right' in position else 'left'
    va = 'top' if 'top' in position else 'bottom'

    ax.text(
        x, y,
        f'System: {system_name}',
        transform=ax.transAxes,
        fontsize=fontsize,
        color='gray',
        alpha=alpha,
        ha=ha,
        va=va,
        style='italic'
    )
