#!/usr/bin/env python3
"""
SDR Host - Client that discovers local SDR devices and connects to SDR Server
Runs on different computers to share SDR information with the central server
"""

import sys
import os
import signal
import subprocess

def check_for_existing_instances():
    """Check for existing sdr_host.py instances and handle them"""
    try:
        # Use ps command to find existing instances
        result = subprocess.run(
            ['ps', 'aux'], 
            capture_output=True, 
            text=True, 
            timeout=5
        )
        
        lines = result.stdout.split('\n')
        existing_pids = []
        current_pid = str(os.getpid())
        
        print(f"🔍 Checking for existing sdr_host.py processes (current PID: {current_pid})...")
        
        for line in lines:
            if 'sdr_host.py' in line and 'python' in line.lower():
                # Skip grep processes
                if 'grep' in line:
                    continue
                    
                # Extract PID (second column)
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[1]
                    if pid != current_pid and pid.isdigit():
                        existing_pids.append(int(pid))
                        print(f"📍 Found existing process: PID {pid}")
                        print(f"   Command: {' '.join(parts[10:])}")
        
        if existing_pids:
            print(f"⚠️  Found {len(existing_pids)} existing sdr_host.py instance(s)")
            print("🛑 Stopping existing instances...")
            
            for pid in existing_pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                    print(f"✅ Stopped process {pid}")
                except ProcessLookupError:
                    print(f"⚠️  Process {pid} already stopped")
                except PermissionError:
                    print(f"❌ Permission denied stopping process {pid}")
            
            # Wait a moment for processes to stop
            import time
            time.sleep(2)
            
            # Verify they're actually stopped
            remaining = []
            result2 = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=5)
            for line in result2.stdout.split('\n'):
                if 'sdr_host.py' in line and 'python' in line.lower() and 'grep' not in line:
                    parts = line.split()
                    if len(parts) >= 2 and parts[1] != current_pid:
                        remaining.append(parts[1])
            
            if remaining:
                print(f"⚠️  Warning: {len(remaining)} processes still running after SIGTERM")
                print("🔥 Force killing remaining processes...")
                for pid in remaining:
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                        print(f"💀 Force killed process {pid}")
                    except:
                        pass
                time.sleep(1)
            
            print("🚀 Starting fresh instance...\n")
        else:
            print("✅ No existing instances found\n")
    
    except Exception as e:
        print(f"⚠️  Warning: Could not check for existing instances: {e}\n")

def get_system_info():
    """Get system information for package installation"""
    import platform
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    elif system == "linux":
        return "linux"
    elif system == "windows":
        return "windows"
    else:
        return "unknown"

def install_package_macos(package_name):
    """Install package on macOS using brew and pip"""
    print(f"🍺 Installing {package_name} on macOS...")
    
    # Special handling for different packages
    if package_name == "psutil":
        # Try pip first for psutil
        try:
            result = subprocess.run([sys.executable, "-m", "pip", "install", "--user", package_name], 
                                  capture_output=True, text=True, check=True)
            print(f"✅ Successfully installed {package_name} via pip")
            return True
        except subprocess.CalledProcessError:
            print(f"⚠️  pip install failed, trying brew...")
            
        # Try brew as fallback
        try:
            subprocess.run(["brew", "install", f"python-{package_name}"], check=True)
            print(f"✅ Successfully installed {package_name} via brew")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print(f"❌ Failed to install {package_name} via brew")
            
    elif package_name == "websockets":
        # websockets is best installed via pip
        try:
            result = subprocess.run([sys.executable, "-m", "pip", "install", "--user", package_name], 
                                  capture_output=True, text=True, check=True)
            print(f"✅ Successfully installed {package_name} via pip")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {package_name}: {e}")
    
    return False

def install_package_linux(package_name):
    """Install package on Linux using system package manager and pip"""
    print(f"🐧 Installing {package_name} on Linux...")
    
    # Try pip first
    try:
        result = subprocess.run([sys.executable, "-m", "pip", "install", "--user", package_name], 
                              capture_output=True, text=True, check=True)
        print(f"✅ Successfully installed {package_name} via pip")
        return True
    except subprocess.CalledProcessError:
        print(f"⚠️  pip install failed, trying system package manager...")
    
    # Try system package managers
    package_managers = [
        (["apt", "update"], ["apt", "install", "-y", f"python3-{package_name}"]),  # Debian/Ubuntu
        (["yum", "check-update"], ["yum", "install", "-y", f"python3-{package_name}"]),  # RHEL/CentOS
        (["dnf", "check-update"], ["dnf", "install", "-y", f"python3-{package_name}"]),  # Fedora
    ]
    
    for update_cmd, install_cmd in package_managers:
        try:
            # Test if package manager exists
            subprocess.run([install_cmd[0], "--version"], capture_output=True, check=True)
            # Run update and install
            subprocess.run(update_cmd, capture_output=True)
            subprocess.run(install_cmd, check=True)
            print(f"✅ Successfully installed {package_name} via {install_cmd[0]}")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    
    print(f"❌ Failed to install {package_name} on Linux")
    return False

def auto_install_package(package_name):
    """Automatically install package based on operating system"""
    system = get_system_info()
    
    print(f"🔧 Auto-installing {package_name} on {system}...")
    
    if system == "macos":
        return install_package_macos(package_name)
    elif system == "linux":
        return install_package_linux(package_name)
    elif system == "windows":
        print("🪟 Windows detected - trying pip install...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "--user", package_name], check=True)
            print(f"✅ Successfully installed {package_name} via pip")
            return True
        except subprocess.CalledProcessError:
            print(f"❌ Failed to install {package_name} on Windows")
            return False
    else:
        print(f"❓ Unknown system '{system}' - trying pip install...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "--user", package_name], check=True)
            print(f"✅ Successfully installed {package_name} via pip")
            return True
        except subprocess.CalledProcessError:
            print(f"❌ Failed to install {package_name}")
            return False

def check_dependencies():
    """Check for required dependencies and auto-install if missing"""
    required_packages = {
        'websockets': 'websockets'
    }
    
    missing_packages = []
    available_features = []
    
    # First pass: check what's missing
    for module_name, package_name in required_packages.items():
        try:
            __import__(module_name)
            print(f"✓ {module_name} is available")
            if module_name == 'websockets':
                available_features.append("WebSocket server communication")
        except ImportError:
            print(f"✗ {module_name} is missing")
            missing_packages.append((module_name, package_name))
    
    if available_features:
        print(f"\n🎯 Available features: {', '.join(available_features)}")
    
    # Auto-install missing packages
    if missing_packages:
        print(f"\n🚀 Auto-installing {len(missing_packages)} missing packages...")
        failed_installs = []
        
        for module_name, package_name in missing_packages:
            if auto_install_package(package_name):
                # Verify installation worked
                try:
                    __import__(module_name)
                    print(f"✓ {module_name} now available")
                    if module_name == 'websockets':
                        available_features.append("WebSocket server communication")
                except ImportError:
                    print(f"⚠️  {module_name} installation may need Python restart")
                    failed_installs.append(package_name)
            else:
                failed_installs.append(package_name)
        
        if failed_installs:
            print(f"\n❌ Failed to auto-install: {', '.join(failed_installs)}")
            print("\n📦 Manual installation required:")
            print(f"   pip install {' '.join(failed_installs)}")
            print("   OR")
            print(f"   python3 -m pip install --user {' '.join(failed_installs)}")
            return False
        else:
            print("\n✅ All packages successfully installed!")
            if available_features:
                print(f"\n🎯 Available features: {', '.join(available_features)}")
    
    print()  # Add spacing
    return True

def install_rtl_sdr_tools_macos():
    """Install RTL-SDR tools on macOS using brew or manual instructions"""
    print("🍺 Installing RTL-SDR tools on macOS...")
    
    # First try to check if brew is available
    try:
        subprocess.run(["brew", "--version"], capture_output=True, check=True)
        print("📦 Homebrew found, installing RTL-SDR tools...")
        
        # Install rtl-sdr package
        result = subprocess.run(["brew", "install", "rtl-sdr"], capture_output=True, text=True, check=True)
        print("✅ Successfully installed RTL-SDR tools via brew")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install RTL-SDR tools via brew: {e}")
        return False
    except FileNotFoundError:
        print("⚠️  Homebrew not found on macOS")
        print("📦 Manual RTL-SDR installation required:")
        print("   Option 1 - Install Homebrew first:")
        print('     /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"')
        print("     brew install rtl-sdr")
        print("   Option 2 - Manual build:")
        print("     Download from: https://github.com/osmocom/rtl-sdr")
        print("     Follow build instructions for macOS")
        print("   Option 3 - Pre-built binaries:")
        print("     Check: https://www.rtl-sdr.com/rtl-sdr-quick-start-guide/")
        return False

def install_rtl_sdr_tools_linux():
    """Install RTL-SDR tools on Linux using system package manager"""
    print("🐧 Installing RTL-SDR tools on Linux...")
    
    # Try different package managers
    package_managers = [
        (["apt", "update"], ["apt", "install", "-y", "rtl-sdr"]),  # Debian/Ubuntu
        (["yum", "check-update"], ["yum", "install", "-y", "rtl-sdr"]),  # RHEL/CentOS
        (["dnf", "check-update"], ["dnf", "install", "-y", "rtl-sdr"]),  # Fedora
    ]
    
    for update_cmd, install_cmd in package_managers:
        try:
            # Test if package manager exists
            subprocess.run([install_cmd[0], "--version"], capture_output=True, check=True)
            print(f"📦 Using {install_cmd[0]} package manager...")
            # Run update and install
            subprocess.run(update_cmd, capture_output=True)
            subprocess.run(install_cmd, check=True)
            print(f"✅ Successfully installed RTL-SDR tools via {install_cmd[0]}")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            continue
    
    print("❌ Failed to install RTL-SDR tools on Linux")
    print("📦 Please install manually:")
    print("   Ubuntu/Debian: sudo apt install rtl-sdr")
    print("   RHEL/CentOS: sudo yum install rtl-sdr")
    print("   Fedora: sudo dnf install rtl-sdr")
    return False

def check_rtl_sdr_tools():
    """Check for RTL-SDR tools and auto-install if missing"""
    print("🔍 Checking for RTL-SDR tools...")
    
    # Check if rtl_test is available
    try:
        result = subprocess.run(["rtl_test", "-t"], capture_output=True, text=True, timeout=5)
        print("✓ RTL-SDR tools are available")
        return True
    except FileNotFoundError:
        print("✗ RTL-SDR tools not found")
    except subprocess.TimeoutExpired:
        print("✓ RTL-SDR tools are available (test timeout is normal)")
        return True
    except Exception as e:
        print(f"⚠️  RTL-SDR tools check failed: {e}")
    
    # Auto-install RTL-SDR tools
    system = get_system_info()
    print(f"🚀 Auto-installing RTL-SDR tools on {system}...")
    
    if system == "macos":
        success = install_rtl_sdr_tools_macos()
    elif system == "linux":
        success = install_rtl_sdr_tools_linux()
    elif system == "windows":
        print("🪟 Windows RTL-SDR installation requires manual setup:")
        print("   1. Download RTL-SDR drivers from https://www.rtl-sdr.com/rtl-sdr-quick-start-guide/")
        print("   2. Install Zadig drivers for your RTL-SDR device")
        success = False
    else:
        print(f"❓ Unknown system '{system}' - manual RTL-SDR installation required")
        success = False
    
    if success:
        # Verify installation
        try:
            subprocess.run(["rtl_test", "-t"], capture_output=True, timeout=5)
            print("✅ RTL-SDR tools installation verified")
            return True
        except:
            print("⚠️  RTL-SDR tools installed but verification failed")
            return True  # Still return true as tools might work
    else:
        print("⚠️  Continuing without RTL-SDR tools - SDR discovery will be limited")
        return False

def check_and_setup_service():
    """Check if sdr_host is set up as a systemd service, create if needed"""
    import os
    import subprocess
    import sys
    
    # Only set up service on Linux systems
    if not sys.platform.startswith('linux'):
        print("🔧 Service setup only available on Linux systems")
        return
    
    # Check if we're running as root (needed for service management)
    if os.geteuid() != 0:
        print("🔧 Service management requires sudo privileges")
        print("💡 To set up as a service, run: sudo python3 sdr_host.py")
        return
    
    service_name = "sdr-host"
    service_file = f"/etc/systemd/system/{service_name}.service"
    script_path = os.path.abspath(__file__)
    
    try:
        # Check if service already exists
        result = subprocess.run(
            ["systemctl", "is-enabled", service_name],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"✅ Service '{service_name}' is already enabled")
            
            # Check if it's running
            status_result = subprocess.run(
                ["systemctl", "is-active", service_name],
                capture_output=True,
                text=True
            )
            
            if status_result.stdout.strip() == "active":
                print(f"🟢 Service '{service_name}' is currently running")
                print("🔄 Restarting service to reload with latest version...")
                subprocess.run(["systemctl", "restart", service_name], check=True)
                print(f"✅ Service '{service_name}' restarted successfully")
                sys.exit(0)
            else:
                print(f"🟡 Service '{service_name}' exists but is not running")
                print("🚀 Starting service...")
                subprocess.run(["systemctl", "start", service_name], check=True)
                print(f"✅ Service '{service_name}' started successfully")
                sys.exit(0)
        
        # Service doesn't exist, create it
        print(f"🔧 Creating systemd service '{service_name}'...")
        
        service_content = f"""[Unit]
Description=SDR Host - Distributed Software Defined Radio Client
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/bin/python3 {script_path}
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=sdr-host

# Environment variables
Environment=PYTHONUNBUFFERED=1

# Security settings
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log /tmp /root/.config

[Install]
WantedBy=multi-user.target
"""
        
        # Write the service file
        with open(service_file, 'w') as f:
            f.write(service_content)
        
        print(f"📝 Created service file: {service_file}")
        
        # Reload systemd
        print("🔄 Reloading systemd daemon...")
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        
        # Enable the service
        print(f"🔧 Enabling service '{service_name}'...")
        subprocess.run(["systemctl", "enable", service_name], check=True)
        
        # Start the service
        print(f"🚀 Starting service '{service_name}'...")
        subprocess.run(["systemctl", "start", service_name], check=True)
        
        print(f"✅ Service '{service_name}' created, enabled, and started successfully!")
        print("\n📋 Service management commands:")
        print(f"   Status:  sudo systemctl status {service_name}")
        print(f"   Stop:    sudo systemctl stop {service_name}")
        print(f"   Restart: sudo systemctl restart {service_name}")
        print(f"   Logs:    sudo journalctl -u {service_name} -f")
        print("\n🎯 The SDR host will now start automatically on boot!")
        print("🔄 Exiting - service is now managed by systemd")
        sys.exit(0)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to set up service: {e}")
    except Exception as e:
        print(f"❌ Error setting up service: {e}")

# Check dependencies and existing instances before importing them
if __name__ == "__main__":
    # Always check for existing instances first
    check_for_existing_instances()
    
    # Skip other checks if running in service mode (dependencies pre-installed)
    service_mode = os.environ.get('SDR_SERVICE_MODE')
    
    if not service_mode:
        # Then check Python dependencies
        if not check_dependencies():
            print("❌ Required Python dependencies not available. Exiting.")
            sys.exit(1)
        
        # Check RTL-SDR tools (non-fatal if missing)
        check_rtl_sdr_tools()
        
        # Check and setup service if needed (disabled for testing)
        # check_and_setup_service()
    else:
        print("🚀 Running in service mode - skipping dependency checks")

import asyncio
import json
import logging
import socket
import subprocess
import threading
import time
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import re

# Optional websockets import with fallback
try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    print("Warning: websockets module not available. WebSocket functionality will be disabled.")
    print("To enable WebSocket features, install with: pip install websockets")
import os

# Configure logging - use /tmp for service mode to avoid read-only filesystem issues
log_file = '/tmp/sdr_host.log' if os.getenv('SDR_SERVICE_MODE') == '1' else 'sdr_host.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_band_from_frequency(frequency_hz: float) -> str:
    """Determine amateur radio band from frequency"""
    freq_mhz = frequency_hz / 1000000
    
    # Amateur radio band mappings (MHz)
    bands = {
        (1.8, 2.0): "160M",
        (3.5, 4.0): "80M", 
        (7.0, 7.3): "40M",
        (14.0, 14.35): "20M",
        (18.068, 18.168): "17M",
        (21.0, 21.45): "15M",
        (24.89, 24.99): "12M",
        (28.0, 29.7): "10M",
        (50.0, 54.0): "6M",
        (144.0, 148.0): "2M",
        (420.0, 450.0): "70CM"
    }
    
    for (low, high), band in bands.items():
        if low <= freq_mhz <= high:
            return band
    
    return f"{freq_mhz:.3f}MHz"

def parse_ft8_decode_line(line: str, sdr_id: str, band: str) -> dict:
    """Parse FT8 decode line and return JSON format
    
    Input: D: FT8 1757487825   2  0.2  222 ~  EA1DFQ K8OCN R-10
    Output: {"sdr_number": "sdr[003]", "frequency": 14074000, "message": "EA1DFQ K8OCN R-10"}
    """
    line = line.strip()
    if not line:
        return None
    
    # Parse the FT8 decode data
    parts = line.split()
    if len(parts) < 8 or not line.startswith('D: FT8'):
        return {
            'sdr_number': f"sdr[{sdr_id}]",
            'frequency': 0,
            'message': line,
            'valid': False
        }
    
    try:
        # Extract the message (everything after the ~ separator)
        if '~' in line:
            message_part = line.split('~', 1)[1].strip()
        else:
            message_part = ' '.join(parts[7:])  # Skip the '~' separator
        
        # Get frequency from band (approximate center frequency)
        frequency_map = {
            '80M': 3573000,
            '40M': 7074000, 
            '20M': 14074000,
            '17M': 18100000,
            '15M': 21074000,
            '12M': 24915000,
            '10M': 28074000,
            '6M': 50313000
        }
        frequency = frequency_map.get(band, 0)
        
        decode_data = {
            'sdr_number': f"sdr[{sdr_id}]",
            'frequency': frequency,
            'message': message_part
        }
        return decode_data
    except (ValueError, IndexError) as e:
        return {
            'sdr_number': f"sdr[{sdr_id}]",
            'frequency': 0,
            'message': line,
            'valid': False,
            'error': str(e)
        }

@dataclass
class LocalSDR:
    """Represents a local SDR device"""
    serial_number: str
    device_type: str
    device_index: int
    vendor_id: str = ""
    product_id: str = ""
    manufacturer: str = ""
    product: str = ""
    # Configuration fields loaded from .config file
    sdr_name: Optional[str] = None
    sdr_frequency: Optional[float] = None
    sdr_modulation: Optional[str] = None
    sdr_band: Optional[str] = None  # Amateur radio band (2M, 70CM, 40M, etc.)
    sdr_sample_rate: int = 24000
    sdr_bias_tee: int = 0  # 0=off, 1=on
    sdr_agc: str = "automatic"
    sdr_squelch: int = 0
    sdr_ppm: int = 0
    sdr_dc_correction: bool = False
    sdr_edge_correction: bool = False
    sdr_deemphasis: bool = False
    sdr_direct_i: bool = False
    sdr_direct_q: bool = False
    sdr_offset_tuning: bool = False
    # Modem fields - only populated if using modem modulation
    modem_decoder: Optional[str] = None
    modem_lead_in: Optional[int] = None
    modem_sample_rate: Optional[int] = None
    modem_trailing: Optional[int] = None
    modem_audio_device: Optional[str] = None
    modem_debug: Optional[bool] = None

class SDRConfigManager:
    """Manages SDR configuration in a single unified JSON file"""
    
    def __init__(self):
        """Initialize the configuration manager"""
        # Always use /root/.config for persistent configuration
        self.home_dir = os.path.expanduser("~")
        self.config_dir = os.path.join(self.home_dir, ".config", "sdr-hosts")
        self.config_file = os.path.join(self.config_dir, "sdr-host.json")
        
        # Create config directory if it doesn't exist
        os.makedirs(self.config_dir, exist_ok=True)
        logger.info(f"SDR config directory: {self.config_dir}")
        
        # Initialize config file if it doesn't exist
        if not os.path.exists(self.config_file):
            self._create_initial_config_file()
    
    def _create_initial_config_file(self):
        """Create initial unified config file"""
        initial_config = {
            "version": "1.0",
            "created": time.strftime("%Y-%m-%d %H:%M:%S"),
            "sdr_devices": {}
        }
        
        try:
            with open(self.config_file, 'w') as f:
                json.dump(initial_config, f, indent=2)
            logger.info(f"Created initial config file: {self.config_file}")
        except IOError as e:
            logger.error(f"Error creating config file: {e}")
    
    def _load_config(self) -> Dict:
        """Load the complete configuration file"""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error loading config file: {e}")
            # Return default structure
            return {
                "version": "1.0",
                "created": time.strftime("%Y-%m-%d %H:%M:%S"),
                "sdr_devices": {}
            }
    
    def _save_config(self, config: Dict) -> bool:
        """Save the complete configuration file"""
        try:
            config["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=2)
            return True
        except IOError as e:
            logger.error(f"Error saving config file: {e}")
            return False
    
    def get_sdr_config(self, serial_number: str) -> Dict:
        """Get configuration for an SDR device"""
        config = self._load_config()
        
        if serial_number in config.get("sdr_devices", {}):
            logger.debug(f"Loaded config for {serial_number}")
            device_config = config["sdr_devices"][serial_number]
            
            # Convert boolean bias_tee values to integer for compatibility
            if "sdr_bias_tee" in device_config:
                bias_value = device_config["sdr_bias_tee"]
                if isinstance(bias_value, bool):
                    device_config["sdr_bias_tee"] = 1 if bias_value else 0
                    logger.info(f"Converted boolean bias_tee {bias_value} to integer {device_config['sdr_bias_tee']} for {serial_number}")
                    # Save the corrected config back
                    self.save_sdr_config(serial_number, device_config)
            
            return device_config
        
        # Return default configuration if device not found
        logger.debug(f"Using default config for {serial_number}")
        return self._get_default_config()
    
    def save_sdr_config(self, serial_number: str, device_config: Dict) -> bool:
        """Save configuration for an SDR device (filtered to remove noise)"""
        config = self._load_config()
        
        if "sdr_devices" not in config:
            config["sdr_devices"] = {}
        
        # Save device config directly without filtering
        # Save device config directly without filtering
        config["sdr_devices"][serial_number] = device_config
        
        if self._save_config(config):
            logger.info(f"Saved configuration for SDR {serial_number}")
            return True
        else:
            logger.error(f"Failed to save config for {serial_number}")
            return False
    
    def get_all_sdr_configs(self) -> Dict[str, Dict]:
        """Get all SDR device configurations"""
        config = self._load_config()
        return config.get("sdr_devices", {})
    
    def get_default_sdr_values(self) -> Dict:
        """Get default SDR configuration values"""
        return {
            "sdr_name": None,
            "sdr_frequency": None,
            "sdr_modulation": None,
            "sdr_band": None,
            "sdr_sample_rate": 24000,
            "sdr_bias_tee": 0,
            "sdr_agc": "automatic",
            "sdr_squelch": 0,
            "sdr_ppm": 0,
            "sdr_dc_correction": False,
            "sdr_edge_correction": False,
            "sdr_deemphasis": False,
            "sdr_direct_i": False,
            "sdr_direct_q": False,
            "sdr_offset_tuning": False,
            "sdr_converter": 0,
            "sdr_converter_type": "none",
            "sdr_converter_offset": 0,
            "sdr_converter_description": "No converter - direct connection"
        }
    
    def get_default_modem_values(self) -> Dict:
        """Get default modem configuration values"""
        return {
            "modem_software": None,
            "modem_bandwidth": None,
            "modem_audio_sample_rate": None,
            "modem_audio_device": None,
            "modem_mode": None,
            "modem_tx_interval": None,
            "modem_sync_time": None,
            "modem_decode_depth": None,
            "modem_band": None,
            "modem_power": None,
            "modem_agc_speed": None,
            "modem_baud_rate": None,
            "modem_varicode": None
        }
    
    def filter_config_for_storage(self, config: Dict) -> Dict:
        """Filter configuration for clean storage in config file"""
        filtered_config = {}
        
        for key, value in config.items():
            # Always keep metadata fields
            if key.startswith('_'):
                filtered_config[key] = value
                continue
            
            # Filter out noise values but keep meaningful data
            should_exclude = (
                value is None or                    # null values
                (value is False and key not in [   # false values except meaningful ones
                    'sdr_dc_correction', 'sdr_deemphasis'  # these can be meaningfully false
                ]) or
                (value == "automatic" and key == "sdr_agc") or  # automatic AGC
                (value == 0 and key in ['sdr_squelch', 'sdr_ppm'])  # zero defaults
            )
            
            if not should_exclude:
                filtered_config[key] = value
        
        return filtered_config
    
    def filter_non_default_values(self, config: Dict) -> Dict:
        """Filter out false/null values and default strings from configuration"""
        filtered_config = {}
        
        for key, value in config.items():
            # Skip metadata fields
            if key.startswith('_'):
                continue
            
            # Convert false to null for boolean fields
            if value is False:
                value = None
            
            # Filter out default string values that add noise
            if value == "automatic":
                value = None
            
            # Filter out zero values that are typically defaults (except for meaningful zeros)
            if value == 0 and key in ['sdr_squelch', 'sdr_ppm']:
                value = None
            
            # Only include non-None values (excludes null/false/automatic/zero values)
            if value is not None:
                filtered_config[key] = value
        
        return filtered_config
    
    def get_sdr_config_for_server(self, serial_number: str) -> Dict:
        """Get SDR configuration filtered for server transmission"""
        full_config = self.get_sdr_config(serial_number)
        logger.info(f"🚀 FILTERED VERSION 2.1.0 - Sending config for {serial_number}")
        
        # Convert display values to uppercase for presentation
        display_config = {}
        for key, value in full_config.items():
            if key in ["sdr_modulation", "sdr_modem_decoder", "sdr_band", "sdr_modem_audio_device"] and value:
                # Display modulation, decoder, band, and audio device in uppercase for tables/UI
                display_config[key] = str(value).upper()
            else:
                display_config[key] = value
                
        return display_config
    
    def add_modem_settings_to_sdr(self, serial_number: str, modulation: str) -> Dict:
        """Add appropriate modem settings to an SDR configuration based on modulation"""
        modem_settings = {}
        
        if modulation.upper() == "FT4":
            modem_settings = {
                "modem_software": "WSJT-X",
                "modem_bandwidth": 2500,
                "modem_audio_sample_rate": 48000,
                "modem_audio_device": "virtual_cable",
                "modem_mode": "usb",
                "modem_tx_interval": 7.5,  # FT4 uses 7.5 second intervals
                "modem_sync_time": True,
                "modem_decode_depth": "Normal"
            }
        elif modulation.upper() == "FT8":
            modem_settings = {
                "modem_software": "WSJT-X", 
                "modem_bandwidth": 3000,
                "modem_audio_sample_rate": 48000,
                "modem_audio_device": "virtual_cable",
                "modem_mode": "usb",
                "modem_tx_interval": 15,  # FT8 uses 15 second intervals
                "modem_sync_time": True,
                "modem_decode_depth": "Normal"
            }
        elif modulation.upper() == "JS8":
            modem_settings = {
                "modem_software": "JS8Call",
                "modem_bandwidth": 2500,
                "modem_audio_sample_rate": 48000,
                "modem_audio_device": "virtual_cable", 
                "modem_mode": "usb",
                "modem_tx_interval": 15,
                "modem_sync_time": False,  # JS8 doesn't require time sync
                "modem_decode_depth": "Normal"
            }
        elif modulation.upper() == "PSK31":
            modem_settings = {
                "modem_software": "fldigi",
                "modem_bandwidth": 62.5,
                "modem_audio_sample_rate": 48000,
                "modem_audio_device": "soundcard",
                "modem_mode": "usb",
                "modem_baud_rate": 31.25,
                "modem_varicode": True
            }
        # Add more digital modes as needed
        
        return modem_settings
    
    def detect_converter_type(self, serial_number: str) -> int:
        """Detect converter type from serial number first digit"""
        if not serial_number or len(serial_number) != 8:
            return 0  # Default to no converter if not exactly 8 digits
        
        try:
            first_digit = int(serial_number[0])
            if first_digit == 1:
                return 1  # UpConverter
            elif first_digit == 2:
                return 2  # DnConverter
            else:
                return 0  # Default to no converter for other digits
        except (ValueError, IndexError):
            return 0  # Default to no converter on error
    
    def get_converter_info(self, converter_type: int) -> Dict:
        """Get converter configuration based on type"""
        converter_configs = {
            0: {
                "sdr_converter": 0,
                "sdr_converter_type": "none",
                "sdr_converter_offset": 0,
                "sdr_converter_description": "No converter - direct connection"
            },
            1: {
                "sdr_converter": 1,
                "sdr_converter_type": "upconverter",  # Store in lowercase for consistency
                "sdr_converter_offset": 125000000,  # 125 MHz upconverter (common for HF)
                "sdr_converter_description": "UpConverter - HF to VHF translation"
            },
            2: {
                "sdr_converter": 2,
                "sdr_converter_type": "dnconverter",  # Store in lowercase for consistency
                "sdr_converter_offset": -10700000000,  # 10.7 GHz downconverter (common for microwave)
                "sdr_converter_description": "DnConverter - microwave to VHF translation"
            }
        }
        
        return converter_configs.get(converter_type, converter_configs[0])
    
    def apply_converter_frequency_correction(self, frequency: int, converter_type: int) -> int:
        """Apply frequency correction based on converter type"""
        converter_info = self.get_converter_info(converter_type)
        offset = converter_info["sdr_converter_offset"]
        
        # Simply add the offset (positive for upconverter, negative for downconverter, zero for none)
        return frequency + offset
    
    def fix_converter_settings_for_existing_sdrs(self):
        """Fix converter settings for all existing SDRs based on serial number rules"""
        try:
            configs = self.get_all_sdr_configs()
            updated_count = 0
            
            for serial_number, config in configs.items():
                # Detect correct converter type
                correct_converter_type = self.detect_converter_type(serial_number)
                correct_converter_info = self.get_converter_info(correct_converter_type)
                
                # Check if current settings are wrong
                current_converter_type = config.get("sdr_converter_type", "none")
                expected_converter_type = correct_converter_info["sdr_converter_type"]
                
                if current_converter_type != expected_converter_type:
                    logger.info(f"🔧 Fixing converter for {serial_number}: {current_converter_type} → {expected_converter_type}")
                    
                    # Update converter settings
                    config.update(correct_converter_info)
                    
                    # Save updated config
                    if self.save_sdr_config(serial_number, config):
                        updated_count += 1
                        logger.info(f"✅ Updated converter settings for {serial_number}")
                    else:
                        logger.error(f"❌ Failed to save converter settings for {serial_number}")
            
            logger.info(f"🎯 Converter fix complete: {updated_count} SDRs updated")
            return updated_count
            
        except Exception as e:
            logger.error(f"❌ Error fixing converter settings: {e}")
            return 0
    
    def change_sdr_serial_number(self, current_serial: str, new_first_digit: str) -> "tuple[bool, str]":
        """
        Change the first digit of an SDR's serial number using rtl_eeprom
        Returns (success, new_serial_number)
        Note: After successful serial change, operator must unplug/replug SDR or reboot system
        """
        import subprocess
        
        # Validate input
        if len(new_first_digit) != 1 or not new_first_digit.isdigit():
            logger.error(f"Invalid first digit: {new_first_digit}. Must be a single digit.")
            return False, current_serial
        
        if len(current_serial) != 8:
            logger.error(f"Invalid serial number length: {current_serial}. Must be 8 digits.")
            return False, current_serial
        
        # Calculate new serial number
        new_serial = new_first_digit + current_serial[1:]
        
        logger.info(f"🔧 Changing SDR serial number: {current_serial} → {new_serial}")
        
        try:
            # Find the RTL-SDR device index for this serial number
            device_index = self._find_rtl_sdr_device_index(current_serial)
            if device_index is None:
                logger.error(f"❌ Could not find RTL-SDR device with serial {current_serial}")
                return False, current_serial
            
            # Use rtl_eeprom to change the serial number - try different paths
            rtl_eeprom_paths = ["rtl_eeprom", "/usr/local/bin/rtl_eeprom", "/usr/bin/rtl_eeprom"]
            
            result = None
            for rtl_path in rtl_eeprom_paths:
                try:
                    # Use shell command with yes to automatically answer prompts
                    cmd_str = f"yes | {rtl_path} -d {device_index} -s {new_serial}"
                    logger.info(f"📡 Running: {cmd_str}")
                    
                    result = subprocess.run(cmd_str, shell=True, capture_output=True, text=True, timeout=30)
                    break  # Command found and executed
                except FileNotFoundError:
                    continue
                except subprocess.TimeoutExpired:
                    logger.error("❌ rtl_eeprom command timed out")
                    return False, current_serial
                except Exception:
                    continue
            
            if not result:
                logger.error("❌ rtl_eeprom not found in any expected location")
                return False, current_serial
            
            if result.returncode == 0:
                logger.info(f"✅ Successfully changed serial number to {new_serial}")
                logger.warning("⚠️  IMPORTANT: Please unplug and replug the SDR device or reboot the system for changes to take effect")
                return True, new_serial
            else:
                logger.error(f"❌ rtl_eeprom failed: {result.stderr}")
                return False, current_serial
                
        except subprocess.TimeoutExpired:
            logger.error("❌ rtl_eeprom command timed out")
            return False, current_serial
        except Exception as e:
            logger.error(f"❌ Error changing serial number: {e}")
            return False, current_serial
    
    def _find_rtl_sdr_device_index(self, serial_number: str) -> Optional[int]:
        """Find the device index for an RTL-SDR with the given serial number"""
        import subprocess
        
        try:
            # Try different paths for rtl_test (Linux vs macOS)
            rtl_test_paths = ["rtl_test", "/usr/local/bin/rtl_test", "/usr/bin/rtl_test"]
            
            result = None
            for rtl_path in rtl_test_paths:
                try:
                    result = subprocess.run([rtl_path], capture_output=True, text=True, timeout=10)
                    if result.returncode is not None:  # Command found
                        break
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
            
            if not result:
                logger.error("❌ rtl_test not found in any expected location")
                return None
            
            # Combine stdout and stderr to get all output
            output = (result.stdout + "\n" + result.stderr).strip()
            lines = output.split('\n')
            device_index = None
            
            for line in lines:
                if serial_number in line and ":" in line and "Realtek" in line:
                    # Parse line like: "  3:  Realtek, RTL2838UHIDIR, SN: 00000001"
                    parts = line.split(':')
                    if len(parts) >= 2:
                        try:
                            device_index = int(parts[0].strip())
                            break
                        except ValueError:
                            continue
            
            return device_index
            
        except Exception as e:
            logger.error(f"❌ Error finding RTL-SDR device index: {e}")
            return None
    
    def update_sdr_config_after_serial_change(self, old_serial: str, new_serial: str) -> bool:
        """
        Update SDR configuration after serial number change
        Moves config from old serial to new serial
        """
        try:
            # Get the old configuration
            old_config = self.get_sdr_config(old_serial)
            
            # Update the serial number in the config
            old_config["sdr_serial_number"] = new_serial
            
            # Save config with new serial number
            if self.save_sdr_config(new_serial, old_config):
                logger.info(f"✅ Moved configuration from {old_serial} to {new_serial}")
                
                # Remove old configuration file
                old_config_path = os.path.join(self.config_dir, f"{old_serial}.json")
                try:
                    if os.path.exists(old_config_path):
                        os.remove(old_config_path)
                        logger.info(f"✅ Removed old config file for {old_serial}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not remove old config file: {e}")
                
                return True
            else:
                logger.error(f"❌ Failed to save new configuration for {new_serial}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error updating config after serial change: {e}")
            return False
    
    def create_initial_config(self, serial_number: str, device_type: str, 
                            manufacturer: str = "", product: str = "") -> Dict:
        """Create initial configuration for a newly detected SDR device"""
        
        # Generate a meaningful default name
        if manufacturer and product:
            default_name = f"{manufacturer} {product}"
        else:
            default_name = f"{device_type} Device"
        
        # Set reasonable defaults based on device type
        if device_type.upper() == "RTL-SDR":
            default_config = {
                "sdr_name": default_name,
                "sdr_frequency": None,
                "sdr_modulation": None,
                "sdr_band": None,
                "sdr_gain": None,
                "sdr_agc": "automatic",
                "sdr_ppm": 0,
                "sdr_bias_tee": 0,
                "sdr_sample_rate": 24000,
                "sdr_squelch": 0,
                "sdr_status": "inactive",
                "sdr_device_type": device_type,
                "sdr_serial_number": serial_number,
                "sdr_dc_correction": None,
                "sdr_edge_correction": False,
                "sdr_deemphasis": None,
                "sdr_direct_i": False,
                "sdr_direct_q": False,
                "sdr_offset_tuning": False
            }
        else:
            # Generic SDR defaults
            default_config = {
                "sdr_name": default_name,
                "sdr_frequency": 100000000,  # 100 MHz
                "sdr_modulation": "fm",
                "sdr_band": None,
                "sdr_gain": None,
                "sdr_agc": "automatic",
                "sdr_ppm": 0,
                "sdr_bias_tee": 0,
                "sdr_sample_rate": 24000,
                "sdr_squelch": 0,
                "sdr_status": "inactive",
                "sdr_device_type": device_type,
                "sdr_serial_number": serial_number,
                "sdr_dc_correction": False,
                "sdr_edge_correction": False,
                "sdr_deemphasis": False,
                "sdr_direct_i": False,
                "sdr_direct_q": False,
                "sdr_offset_tuning": False
            }
        
        # Detect converter type from serial number
        converter_type = self.detect_converter_type(serial_number)
        converter_info = self.get_converter_info(converter_type)
        
        # Add converter configuration
        default_config.update(converter_info)
        
        # Adjust default frequency based on converter type
        if converter_type != 0:  # If using a converter
            # Log the converter detection
            logger.info(f"Detected {converter_info['sdr_converter_description']} for {serial_number}")
            
            # For upconverter, default to HF frequency (20m) if no frequency set
            if converter_type == 1:
                if default_config.get("sdr_frequency") is None:
                    default_config["sdr_frequency"] = 14200000  # 20m band
                default_config["sdr_name"] = f"{default_name} (Upconverter)"
            # For downconverter, default to microwave frequency (10 GHz) if no frequency set  
            elif converter_type == 2:
                if default_config.get("sdr_frequency") is None:
                    default_config["sdr_frequency"] = 10368000000  # 10 GHz amateur band
                default_config["sdr_name"] = f"{default_name} (Downconverter)"
        
        # Add modem settings if this is a digital mode
        modulation = default_config.get("sdr_modulation")
        if modulation and modulation.lower() in ["ft4", "ft8", "js8", "psk31"]:
            modem_settings = self.add_modem_settings_to_sdr(serial_number, modulation)
            default_config.update(modem_settings)
        
        # Add metadata
        default_config["_device_type"] = device_type
        default_config["_manufacturer"] = manufacturer
        default_config["_product"] = product
        default_config["_created"] = datetime.now().isoformat()
        
        # Save the initial configuration
        self.save_sdr_config(serial_number, default_config)
        logger.info(f"Created initial configuration for {serial_number} ({device_type})")
        
        return default_config
    
    def _get_default_config(self) -> Dict:
        """Get minimal default configuration for unknown devices"""
        return {
            "sdr_name": None,
            "sdr_frequency": None,
            "sdr_modulation": None,
            "sdr_sample_rate": 24000,
            "sdr_biast": 0,
            "sdr_agc": "automatic",
            "sdr_squelch": 0,
            "sdr_ppm": 0,
            "sdr_dc_correction": False,
            "sdr_edge_correction": False,
            "sdr_deemphasis": False,
            "sdr_direct_i": False,
            "sdr_direct_q": False,
            "sdr_offset_tuning": False,
            "modem_decoder": None,
            "modem_lead_in": None,
            "modem_sample_rate": None,
            "modem_trailing": None,
            "modem_audio_device": None,
            "modem_debug": None
        }

class SDRDiscovery:
    """Handles discovery of local SDR devices"""
    
    @staticmethod
    def discover_rtl_sdr_devices() -> List[LocalSDR]:
        """Discover RTL-SDR devices using rtl_test"""
        devices = []
        try:
            # Use rtl_test to just get device enumeration without running the test
            proc = subprocess.Popen(['rtl_test'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Read initial output lines until we get the device enumeration
            output_lines = []
            try:
                # Read with a timeout to get just the device enumeration
                stdout_data, stderr_data = proc.communicate(timeout=3)
                output_lines = (stdout_data + stderr_data).split('\n')
            except subprocess.TimeoutExpired:
                # Kill the process if it hangs
                proc.kill()
                stdout_data, stderr_data = proc.communicate()
                output_lines = (stdout_data + stderr_data).split('\n')
            
            logger.debug(f"rtl_test output: {output_lines}")
            
            # Parse the output to extract device information
            # Only look for enumeration lines (format: "0:  Realtek, RTL2838UHIDIR, SN: 00000001")
            # Ignore lines that start with "Using device" as they refer to the same devices
            device_index = 0
            
            for line in output_lines:
                line = line.strip()
                # Only process device enumeration lines (number: manufacturer, product, SN)
                # Skip "Using device" lines and lines without proper enumeration format
                if (re.match(r'^\d+:\s+', line) and 
                    ('Realtek' in line or 'RTL' in line or 'Generic RTL' in line) and
                    not line.startswith('Using device')):
                    
                    # Parse device line like "0:  Realtek, RTL2838UHIDIR, SN: 00000001"
                    parts = line.split(':', 1)
                    if len(parts) >= 2:
                        device_info = parts[1].strip()
                        info_parts = device_info.split(',')
                        
                        manufacturer = info_parts[0].strip() if len(info_parts) > 0 else "Realtek"
                        product = info_parts[1].strip() if len(info_parts) > 1 else "RTL-SDR"
                        serial = "unknown"
                        
                        # Extract serial number if present
                        if len(info_parts) > 2:
                            sn_part = info_parts[2].strip()
                            if sn_part.startswith('SN:'):
                                serial = sn_part[3:].strip()
                        
                        device = LocalSDR(
                            serial_number=serial,
                            device_type="RTL-SDR",
                            device_index=device_index,
                            manufacturer=manufacturer,
                            product=product
                        )
                        devices.append(device)
                        logger.info(f"Found RTL-SDR device: {manufacturer} {product}, SN: {serial}")
                        device_index += 1
            
            # Fallback: if no devices found but rtl_test exists, check with rtl_eeprom
            if not devices:
                try:
                    result = subprocess.run(['rtl_eeprom'], capture_output=True, text=True, timeout=5)
                    if 'Found device' in result.stdout or result.returncode == 0:
                        # Create a basic RTL-SDR entry
                        device = LocalSDR(
                            serial_number="00000001",
                            device_type="RTL-SDR",
                            device_index=0,
                            manufacturer="Realtek",
                            product="RTL-SDR Device"
                        )
                        devices.append(device)
                        logger.info("Found RTL-SDR device via rtl_eeprom fallback")
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    pass
            
        except FileNotFoundError:
            logger.debug("rtl_test not found - trying USB device detection...")
            # Fallback: try to detect RTL-SDR devices via USB enumeration
            devices.extend(SDRDiscovery._detect_rtl_sdr_via_usb())
        except Exception as e:
            logger.error(f"Error discovering RTL-SDR devices: {e}")
        
        return devices
    
    @staticmethod
    def _detect_rtl_sdr_via_usb() -> List[LocalSDR]:
        """Detect RTL-SDR devices via USB enumeration when rtl_test is not available"""
        devices = []
        
        try:
            import platform
            system = platform.system()
            
            if system == "Darwin":  # macOS
                # Use system_profiler to detect RTL-SDR devices
                result = subprocess.run(
                    ["system_profiler", "SPUSBDataType"], 
                    capture_output=True, text=True, timeout=10
                )
                
                lines = result.stdout.split('\n')
                current_device = None
                serial_number = None
                device_index = 0
                
                for line in lines:
                    line = line.strip()
                    
                    # Look for RTL2838UHIDIR devices
                    if "RTL2838UHIDIR:" in line:
                        current_device = "RTL2838UHIDIR"
                        serial_number = None
                    elif current_device and "Serial Number:" in line:
                        serial_number = line.split("Serial Number:", 1)[1].strip()
                        
                        # Create device entry
                        device = LocalSDR(
                            serial_number=serial_number,
                            device_type="RTL-SDR",
                            device_index=device_index,
                            vendor_id="0x0bda",
                            product_id="0x2838",
                            manufacturer="Realtek",
                            product="RTL2838UHIDIR"
                        )
                        devices.append(device)
                        logger.info(f"Found RTL-SDR device via USB: Realtek RTL2838UHIDIR, SN: {serial_number}")
                        device_index += 1
                        current_device = None
                        
            elif system == "Linux":
                # Use lsusb to detect RTL-SDR devices
                try:
                    result = subprocess.run(["lsusb"], capture_output=True, text=True, timeout=5)
                    lines = result.stdout.split('\n')
                    device_index = 0
                    
                    for line in lines:
                        # Look for Realtek RTL2838 devices
                        if "0bda:2838" in line or "Realtek" in line and "RTL" in line:
                            # Extract bus and device info for serial number
                            parts = line.split()
                            if len(parts) >= 6:
                                bus = parts[1]
                                dev = parts[3].rstrip(':')
                                serial = f"{bus}_{dev}"
                                
                                device = LocalSDR(
                                    serial_number=serial,
                                    device_type="RTL-SDR",
                                    device_index=device_index,
                                    vendor_id="0x0bda",
                                    product_id="0x2838",
                                    manufacturer="Realtek",
                                    product="RTL2838UHIDIR"
                                )
                                devices.append(device)
                                logger.info(f"Found RTL-SDR device via USB: Realtek RTL2838UHIDIR, Bus: {bus}")
                                device_index += 1
                                
                except FileNotFoundError:
                    logger.debug("lsusb not available on this Linux system")
                    
            elif system == "Windows":
                # Windows USB detection would require additional libraries
                logger.debug("Windows USB detection not implemented - install RTL-SDR tools")
                
        except Exception as e:
            logger.debug(f"USB detection failed: {e}")
        
        if devices:
            logger.info(f"Detected {len(devices)} RTL-SDR devices via USB enumeration")
            logger.warning("⚠️  RTL-SDR tools not installed - device functionality will be limited")
            logger.warning("📦 Install RTL-SDR tools for full functionality:")
            
            import platform
            system = platform.system()
            if system == "Darwin":
                logger.warning("   macOS: brew install rtl-sdr")
            elif system == "Linux":
                logger.warning("   Linux: sudo apt install rtl-sdr  (or yum/dnf)")
            elif system == "Windows":
                logger.warning("   Windows: Download from https://www.rtl-sdr.com/")
        
        return devices
    
    @staticmethod
    def discover_hackrf_devices() -> List[LocalSDR]:
        """Discover HackRF devices using hackrf_info"""
        devices = []
        try:
            result = subprocess.run(['hackrf_info'], capture_output=True, text=True, timeout=10)
            output = result.stdout
            
            device_index = 0
            serial_number = "unknown"
            
            for line in output.split('\n'):
                if 'Serial number:' in line:
                    match = re.search(r'Serial number:\s*0x([0-9a-fA-F]+)', line)
                    if match:
                        serial_number = match.group(1)
                        
                        device = LocalSDR(
                            serial_number=serial_number,
                            device_type="HackRF",
                            device_index=device_index,
                            manufacturer="Great Scott Gadgets",
                            product="HackRF One"
                        )
                        devices.append(device)
                        device_index += 1
                        
        except subprocess.TimeoutExpired:
            logger.warning("hackrf_info timed out")
        except FileNotFoundError:
            logger.debug("hackrf_info not found - HackRF tools may not be installed")
        except Exception as e:
            logger.error(f"Error discovering HackRF devices: {e}")
        
        return devices
    
    @staticmethod
    def discover_sdrplay_devices() -> List[LocalSDR]:
        """Discover SDRplay devices using SoapySDRUtil"""
        devices = []
        try:
            result = subprocess.run(['SoapySDRUtil', '--find="driver=sdrplay"'], 
                                  capture_output=True, text=True, timeout=10)
            output = result.stdout
            
            # Parse SoapySDR output
            device_blocks = output.split('------')
            device_index = 0
            
            for block in device_blocks:
                if 'driver=sdrplay' in block:
                    serial_match = re.search(r'serial=([^\s,]+)', block)
                    serial_number = serial_match.group(1) if serial_match else f"sdrplay_{device_index}"
                    
                    device = LocalSDR(
                        serial_number=serial_number,
                        device_type="SDRplay",
                        device_index=device_index,
                        manufacturer="SDRplay",
                        product="SDRplay Device"
                    )
                    devices.append(device)
                    device_index += 1
                    
        except subprocess.TimeoutExpired:
            logger.warning("SoapySDRUtil timed out")
        except FileNotFoundError:
            logger.debug("SoapySDRUtil not found - SoapySDR may not be installed")
        except Exception as e:
            logger.error(f"Error discovering SDRplay devices: {e}")
        
        return devices
    
    @staticmethod
    def _create_sdr_from_dict(device_dict: dict, device_index: int) -> LocalSDR:
        """Create LocalSDR from dictionary"""
        return LocalSDR(
            serial_number=device_dict.get('serial_number', f"unknown_{device_index}"),
            device_type=device_dict.get('device_type', 'Unknown'),
            device_index=device_index,
            vendor_id=device_dict.get('vendor_id', ''),
            product_id=device_dict.get('product_id', ''),
            manufacturer=device_dict.get('manufacturer', ''),
            product=device_dict.get('product', '')
        )
    
    @staticmethod
    def discover_all_sdr_devices() -> List[LocalSDR]:
        """Discover all supported SDR devices and load/create their configurations"""
        all_devices = []
        
        logger.info("Discovering SDR devices...")
        
        # Initialize configuration manager
        config_manager = SDRConfigManager()
        
        # Discover different types of SDR devices
        rtl_devices = SDRDiscovery.discover_rtl_sdr_devices()
        hackrf_devices = SDRDiscovery.discover_hackrf_devices()
        sdrplay_devices = SDRDiscovery.discover_sdrplay_devices()
        
        all_devices.extend(rtl_devices)
        all_devices.extend(hackrf_devices)
        all_devices.extend(sdrplay_devices)
        
        # Load configuration for each discovered device
        for device in all_devices:
            # Check if configuration exists in the unified config file
            config = config_manager.get_sdr_config(device.serial_number)
            
            # If no configuration found, create initial configuration for new device
            if not config or not config.get('sdr_name'):
                logger.info(f"Creating initial configuration for new device: {device.serial_number}")
                config = config_manager.create_initial_config(
                    device.serial_number, 
                    device.device_type,
                    device.manufacturer,
                    device.product
                )
            else:
                logger.info(f"Using existing configuration for device: {device.serial_number}")
            
            # Apply configuration to device using sdr_ prefixed field names
            device.sdr_name = config.get('sdr_name')
            device.sdr_frequency = config.get('sdr_frequency')
            device.sdr_modulation = config.get('sdr_modulation')
            device.sdr_sample_rate = config.get('sdr_sample_rate', 24000)
            device.sdr_bias_tee = config.get('sdr_bias_tee', 0)
            device.sdr_agc = config.get('sdr_agc', 'automatic')
            device.sdr_squelch = config.get('sdr_squelch', 0)
            device.sdr_ppm = config.get('sdr_ppm', 0)
            device.sdr_dc_correction = config.get('sdr_dc_correction', False)
            device.sdr_edge_correction = config.get('sdr_edge_correction', False)
            device.sdr_deemphasis = config.get('sdr_deemphasis', False)
            device.sdr_direct_i = config.get('sdr_direct_i', False)
            device.sdr_direct_q = config.get('sdr_direct_q', False)
            device.sdr_offset_tuning = config.get('sdr_offset_tuning', False)
            
            # Apply converter configuration
            device.sdr_converter = config.get('sdr_converter', 0)
            device.sdr_converter_type = config.get('sdr_converter_type', 'none')
            device.sdr_converter_offset = config.get('sdr_converter_offset', 0)
            device.sdr_converter_description = config.get('sdr_converter_description', 'No converter - direct connection')
            
            # Apply modem configuration if present
            device.modem_decoder = config.get('sdr_modem_decoder')
            device.modem_lead_in = config.get('sdr_modem_lead_in')
            device.modem_sample_rate = config.get('sdr_modem_sample_rate')
            device.modem_trailing = config.get('sdr_modem_trailing')
            device.modem_audio_device = config.get('sdr_modem_audio_device')
            device.modem_debug = config.get('sdr_modem_debug', False)
            
            # Auto-update modem_decoder from modulation if modulation exists
            if device.sdr_modulation and device.sdr_modulation.strip():
                auto_decoder = device.sdr_modulation.upper()
                if device.modem_decoder != auto_decoder:
                    logger.info(f"🔄 Auto-updating modem_decoder from '{device.modem_decoder}' to '{auto_decoder}' based on modulation")
                    device.modem_decoder = auto_decoder
                    # Save the updated config
                    config['sdr_modem_decoder'] = auto_decoder
                    config_mgr = SDRConfigManager()
                    config_mgr.save_sdr_config(device.serial_number, config)
            
            logger.info(f"Loaded config for {device.device_type} (SN: {device.serial_number})")
            if device.sdr_name:
                logger.info(f"  Name: {device.sdr_name}")
            if device.sdr_frequency and device.sdr_modulation:
                logger.info(f"  Default settings: {device.sdr_frequency} Hz, {device.sdr_modulation}")
            if device.sdr_converter != 0:
                logger.info(f"  Converter: {device.sdr_converter_description}")
                logger.info(f"  Frequency offset: {device.sdr_converter_offset:+,} Hz")
        
        logger.info(f"Found {len(all_devices)} SDR devices:")
        for i, device in enumerate(all_devices):
            name_info = f" ({device.sdr_name})" if device.sdr_name else ""
            logger.info(f"  {i+1}. {device.device_type}{name_info} - Serial: {device.serial_number}")
            
            # Show converter information if present
            if hasattr(device, 'sdr_converter') and device.sdr_converter != 0:
                converter_symbol = "🔧" if device.sdr_converter == 1 else "📡" if device.sdr_converter == 2 else ""
                logger.info(f"      {converter_symbol} {device.sdr_converter_description}")
            
            logger.info(f"      Config: sdr-host.json")
        
        return all_devices

class SDRHost:
    """Main SDR Host class that manages local SDRs and server connection"""
    
    def __init__(self, server_discovery_port=4210):
        self.server_discovery_port = server_discovery_port
        self.server_ip = None
        self.server_port = None
        self.websocket = None
        self.local_sdrs: List[LocalSDR] = []
        self.host_id = None
        self.running = False
        self.config_manager = SDRConfigManager()
        
        # Modem decoder management (based on sdrctl.py implementation)
        self.active_decoders: Dict[str, Dict] = {}
        self.UDP_AUDIO_BASE_PORT = 3100      # AF2UDP audio streaming ports
        self.FT8_WEBSOCKET_BASE_PORT = 4200  # FT8 WebSocketD ports
        
        # Binary paths for modem decoding (iMac ShackMate installation)
        self.af2udp_path = "/opt/ShackMate/ft8modem/af2udp"
        self.websocketd_path = "/usr/local/bin/websocketd"  # Standard Homebrew location
        self.ft8modem_path = "/opt/ShackMate/ft8modem/ft8modem"
        self.jt9_path = "/opt/ShackMate/ft8modem/jt9"
        
    async def discover_server(self, timeout=10):
        """Discover SDR server via UDP broadcast with fallback to direct connection"""
        logger.info(f"Listening for SDR server broadcasts on port {self.server_discovery_port}...")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Allow multiple processes to bind to the same broadcast port
        if hasattr(socket, 'SO_REUSEPORT'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.settimeout(1.0)
        
        try:
            # Listen on port 4210 for server broadcasts
            sock.bind(('', self.server_discovery_port))
            
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    data, addr = sock.recvfrom(1024)
                    message = data.decode().strip()
                    
                    # Parse message: "ShackMate SDR-Server, IP Address, Port"
                    if message.startswith("ShackMate SDR-Server"):
                        parts = message.split(", ")
                        if len(parts) >= 3:
                            self.server_ip = parts[1].strip()
                            self.server_port = int(parts[2].strip())
                            logger.info(f"Found SDR server at {self.server_ip}:{self.server_port}")
                            return True
                            
                except socket.timeout:
                    continue
                except Exception as e:
                    logger.error(f"Error receiving broadcast: {e}")
                    
                # Allow other async tasks to run
                await asyncio.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Error setting up UDP listener: {e}")
        finally:
            sock.close()
        
        logger.warning("No SDR server found within timeout period")
        
        # Fallback: Try direct connections to known server IPs
        logger.info("🔄 Attempting direct connection to known server addresses...")
        known_servers = [
            ("192.168.1.1", 4010),   # Router with port forwarding to iMac
            ("10.146.1.241", 4010),  # iMac server (direct - previous)
            ("10.146.1.118", 4010),  # iMac server (direct - if IP changed)
            ("192.168.0.2", 4010),   # iMac via 192.168.0.x network
            ("127.0.0.1", 4010),     # Local server
        ]
        
        for server_ip, server_port in known_servers:
            logger.info(f"🔍 Trying direct connection to {server_ip}:{server_port}...")
            if await self._test_server_connection(server_ip, server_port):
                self.server_ip = server_ip
                self.server_port = server_port
                logger.info(f"✅ Direct connection successful to {server_ip}:{server_port}")
                return True
        
        logger.error("❌ No SDR server found via broadcast or direct connection")
        return False
    
    async def _test_server_connection(self, ip, port, timeout=3):
        """Test if a server is reachable on the given IP and port"""
        try:
            import asyncio
            import socket
            
            # Create a TCP connection test
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            
            try:
                # Test if port is open
                result = sock.connect_ex((ip, port))
                if result == 0:
                    return True
            except Exception:
                pass
            finally:
                sock.close()
                
            # Alternative: Try a simple WebSocket connection test
            if WEBSOCKETS_AVAILABLE:
                try:
                    uri = f"ws://{ip}:{port}"
                    websocket_conn = await asyncio.wait_for(
                        websockets.connect(uri), 
                        timeout=timeout
                    )
                    await websocket_conn.close()
                    return True
                except Exception:
                    pass
                    
        except Exception as e:
            logger.debug(f"Connection test failed for {ip}:{port}: {e}")
            
        return False
        return False

    async def connect_to_server(self, max_retries: int = 5, retry_delay: int = 5) -> bool:
        """Connect to the SDR server via WebSocket with retry logic"""
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSocket functionality not available - websockets module not installed")
            return False
            
        if not self.server_ip or not self.server_port:
            logger.error("Server IP/port not set - run discover_server first")
            return False
        
        for attempt in range(1, max_retries + 1):
            try:
                uri = f"ws://{self.server_ip}:{self.server_port}"
                logger.info(f"🔗 Connecting to SDR server at {uri} (attempt {attempt}/{max_retries})")
                
                self.websocket = await websockets.connect(uri)
                logger.info("✅ Connected to SDR server")
                return True
                
            except Exception as e:
                logger.warning(f"❌ Connection attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    logger.info(f"⏳ Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"💥 Failed to connect after {max_retries} attempts")
                    return False
        
        return False
    
    async def send_sdr_discovery(self):
        """Send discovered SDR information to the server"""
        if not self.websocket:
            logger.error("Not connected to server")
            return
        
        # Convert LocalSDR objects to dictionaries
        sdr_data = []
        for sdr in self.local_sdrs:
            sdr_dict = {
                "serial_number": sdr.serial_number,
                "device_type": sdr.device_type,
                "device_index": sdr.device_index,
                "vendor_id": sdr.vendor_id,
                "product_id": sdr.product_id,
                "manufacturer": sdr.manufacturer,
                "product": sdr.product,
                "status": "available"
            }
            
            # Add filtered configuration (only non-default values) and flatten structure
            config_manager = SDRConfigManager()
            filtered_config = config_manager.get_sdr_config_for_server(sdr.serial_number)
            if filtered_config:
                # Process configuration fields for server compatibility
                for key, value in filtered_config.items():
                    if key.startswith('sdr_'):
                        # Special case: keep sdr_name as sdr_name (don't remove prefix)
                        if key == 'sdr_name':
                            sdr_dict['sdr_name'] = value
                        else:
                            # Remove sdr_ prefix for other fields for server compatibility
                            server_key = key[4:]  # Remove 'sdr_' prefix
                            sdr_dict[server_key] = value
                    else:
                        # Keep non-sdr fields as is (like modem fields, converter fields)
                        sdr_dict[key] = value
            
            sdr_data.append(sdr_dict)
        
        message = {
            "type": "sdr_discovery",
            "sdrs": sdr_data,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            message_json = json.dumps(message)
            logger.info(f"🚀 Transmitting discovery data for {len(sdr_data)} SDR devices")
            await self.websocket.send(message_json)
            logger.info(f"✅ Discovery data sent successfully")
        except Exception as e:
            logger.error(f"Failed to send SDR discovery: {e}")
    
    async def handle_server_messages(self):
        """Handle incoming messages from the server with automatic reconnection"""
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSocket not available")
            return
            
        while self.running:
            try:
                if not self.websocket:
                    logger.warning("⚠️  WebSocket connection lost, attempting to reconnect...")
                    # Keep trying indefinitely every 30 seconds until reconnected
                    while self.running and not await self.reconnect_to_server():
                        logger.error("❌ Failed to reconnect, retrying in 30 seconds...")
                        await asyncio.sleep(30)
                    
                    if not self.running:
                        break
                    
                    logger.info("✅ Reconnection successful, resuming normal operation...")
                
                # Only process messages if we have a valid websocket connection
                if self.websocket:
                    # Process messages from server
                    async for message in self.websocket:
                        await self._process_server_message(message)
                    
            except websockets.exceptions.ConnectionClosed:
                logger.warning("🔌 Server connection closed, will attempt to reconnect...")
                self.websocket = None
                await asyncio.sleep(5)  # Brief pause before reconnection attempt
                
            except websockets.exceptions.ConnectionClosedError:
                logger.warning("🔌 Server connection closed with error, will attempt to reconnect...")
                self.websocket = None
                await asyncio.sleep(5)
                
            except Exception as e:
                # Handle other websocket exceptions gracefully
                error_name = type(e).__name__
                logger.error(f"❌ WebSocket error ({error_name}): {e}")
                self.websocket = None
                await asyncio.sleep(10)  # Longer pause for unexpected errors
    
    async def reconnect_to_server(self) -> bool:
        """Attempt to reconnect to the server and resend discovery data"""
        logger.info("🔄 Attempting to reconnect to SDR server...")
        
        # First try to rediscover the server (in case IP changed)
        if not await self.discover_server():
            logger.warning("⚠️  Server discovery failed, using previous server info")
        
        # Attempt to reconnect
        if await self.connect_to_server(max_retries=3, retry_delay=3):
            logger.info("✅ Reconnected successfully, resending discovery data...")
            # Resend SDR discovery information after reconnection
            await self.send_sdr_discovery()
            return True
        else:
            logger.error("❌ Reconnection failed")
            return False
    
    async def _process_server_message(self, message: str):
        """Process a message from the server"""
        try:
            # Try to parse as JSON first
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "welcome":
                self.host_id = data.get("host_id")
                logger.info(f"Received welcome from server, host ID: {self.host_id}")
                
            elif msg_type == "sdr_discovery_ack":
                global_indices = data.get("global_indices", [])
                logger.info(f"Server assigned global indices: {global_indices}")
                
            elif msg_type == "sdr_command":
                await self._handle_sdr_command(data)
                
            elif msg_type == "json_api_command":
                await self._handle_json_api_command(data)
                
            elif msg_type == "heartbeat_ack":
                logger.debug("Received heartbeat acknowledgment")
                
            elif msg_type == "host_inventory_response":
                await self._handle_host_inventory_response(data)
                
            elif msg_type == "host_inventory":
                await self._handle_host_inventory(data)
                
            else:
                logger.warning(f"Unknown message type: {msg_type}")
                
        except json.JSONDecodeError:
            # Not JSON, try as text command
            await self._handle_text_command(message.strip())
        except Exception as e:
            logger.error(f"Error processing server message: {e}")
    
    async def _handle_text_command(self, message: str):
        """Handle simple text commands from server like 'set 1 sdr_frequency 14230000'"""
        try:
            parts = message.split()
            if len(parts) < 4:
                logger.warning(f"Invalid text command format: {message}")
                return
            
            command = parts[0]  # "set"
            local_index = int(parts[1])  # "1"
            variable = parts[2]  # "sdr_frequency" or "sdr_modulation"
            value = " ".join(parts[3:])  # rest as value
            
            logger.info(f"📡 Received text command: {command} {local_index} {variable} = {value}")
            
            if command == "set":
                # Find the target SDR
                target_sdr = None
                for sdr in self.local_sdrs:
                    if sdr.device_index + 1 == local_index:  # device_index is 0-based, local_index is 1-based
                        target_sdr = sdr
                        break
                
                if not target_sdr:
                    logger.error(f"SDR with local index {local_index} not found")
                    return
                
                # Handle the set command using existing logic but with proper variable names
                config_manager = SDRConfigManager()
                current_config = config_manager.get_sdr_config(target_sdr.serial_number)
                
                if variable == "sdr_frequency":
                    try:
                        freq_value = float(value)
                        if freq_value <= 0:
                            logger.error("Frequency must be positive")
                            return
                        current_config["sdr_frequency"] = int(freq_value)  # Store as integer
                        if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                            logger.info(f"✅ Set sdr_frequency to {int(freq_value)} Hz for {target_sdr.serial_number}")
                            await self.send_sdr_discovery()  # Update server
                        else:
                            logger.error("Failed to save sdr_frequency configuration")
                    except (ValueError, TypeError):
                        logger.error(f"Invalid frequency value: {value}")
                
                elif variable == "sdr_modulation":
                    valid_modes = ["am", "fm", "usb", "lsb", "cw", "wfm", "nfm", "ft4", "ft8", "aprs", "ads-b", "adsb"]
                    if value.lower() not in valid_modes:
                        logger.error(f"Invalid modulation: {value}. Valid: {valid_modes}")
                        return
                    current_config["sdr_modulation"] = value.lower()
                    if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                        logger.info(f"✅ Set sdr_modulation to {value.lower()} for {target_sdr.serial_number}")
                        await self.send_sdr_discovery()  # Update server
                    else:
                        logger.error("Failed to save sdr_modulation configuration")
                
                elif variable == "sdr_band":
                    # Store band in lowercase format for consistency
                    current_config["sdr_band"] = value.lower()
                    if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                        logger.info(f"✅ Set sdr_band to {value.lower()} for {target_sdr.serial_number}")
                        await self.send_sdr_discovery()  # Update server
                    else:
                        logger.error("Failed to save sdr_band configuration")
                
                elif variable == "sdr_bias_tee":
                    try:
                        # Convert value to integer (0 or 1)
                        biast_value = int(value) if str(value).isdigit() else 0
                        if biast_value not in [0, 1]:
                            logger.error(f"Invalid bias tee value: {value}. Must be 0 or 1")
                            return
                        current_config["sdr_bias_tee"] = biast_value
                        if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                            logger.info(f"✅ Set sdr_bias_tee to {biast_value} for {target_sdr.serial_number}")
                            await self.send_sdr_discovery()  # Update server
                        else:
                            logger.error("Failed to save sdr_bias_tee configuration")
                    except (ValueError, TypeError):
                        logger.error(f"Invalid bias tee value: {value}")
                
                elif variable == "sdr_gain":
                    try:
                        gain_value = float(value)
                        current_config["sdr_gain"] = gain_value
                        if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                            logger.info(f"✅ Set sdr_gain to {gain_value} for {target_sdr.serial_number}")
                            await self.send_sdr_discovery()  # Update server
                        else:
                            logger.error("Failed to save sdr_gain configuration")
                    except (ValueError, TypeError):
                        logger.error(f"Invalid gain value: {value}")
                
                elif variable == "sdr_agc":
                    current_config["sdr_agc"] = value.lower()
                    if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                        logger.info(f"✅ Set sdr_agc to {value.lower()} for {target_sdr.serial_number}")
                        await self.send_sdr_discovery()  # Update server
                    else:
                        logger.error("Failed to save sdr_agc configuration")
                
                elif variable == "sdr_ppm":
                    try:
                        ppm_value = int(float(value))
                        current_config["sdr_ppm"] = ppm_value
                        if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                            logger.info(f"✅ Set sdr_ppm to {ppm_value} for {target_sdr.serial_number}")
                            await self.send_sdr_discovery()  # Update server
                        else:
                            logger.error("Failed to save sdr_ppm configuration")
                    except (ValueError, TypeError):
                        logger.error(f"Invalid PPM value: {value}")
                
                elif variable == "sdr_sample_rate":
                    try:
                        sample_rate_value = int(value)
                        current_config["sdr_sample_rate"] = sample_rate_value
                        if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                            logger.info(f"✅ Set sdr_sample_rate to {sample_rate_value} for {target_sdr.serial_number}")
                            await self.send_sdr_discovery()  # Update server
                        else:
                            logger.error("Failed to save sdr_sample_rate configuration")
                    except (ValueError, TypeError):
                        logger.error(f"Invalid sample rate value: {value}")
                
                elif variable == "sdr_squelch":
                    try:
                        squelch_value = float(value)
                        current_config["sdr_squelch"] = squelch_value
                        if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                            logger.info(f"✅ Set sdr_squelch to {squelch_value} for {target_sdr.serial_number}")
                            await self.send_sdr_discovery()  # Update server
                        else:
                            logger.error("Failed to save sdr_squelch configuration")
                    except (ValueError, TypeError):
                        logger.error(f"Invalid squelch value: {value}")
                
                elif variable == "sdr_modem_decoder":
                    # Handle modem decoder updates from server
                    valid_decoders = ["ft4", "ft8", "jt65", "msk", "q65", "wspr", "js8", "ads-b", "aprs", "none"]
                    if value and str(value).lower() not in valid_decoders:
                        logger.error(f"Invalid modem decoder: {value}. Valid: {valid_decoders}")
                        return
                    # Store in lowercase format, handle "none" special case
                    decoder_value = str(value).lower() if value and value.lower() != "none" else None
                    current_config["sdr_modem_decoder"] = decoder_value
                    if config_manager.save_sdr_config(target_sdr.serial_number, current_config):
                        logger.info(f"✅ Set sdr_modem_decoder to {decoder_value} for {target_sdr.serial_number}")
                        await self.send_sdr_discovery()  # Update server
                    else:
                        logger.error("Failed to save sdr_modem_decoder configuration")
                
                else:
                    logger.warning(f"Unknown variable for text command: {variable}")
            
            elif command == "converter":
                # Handle converter command: converter 1 {0-2}
                # Format: converter <local_index> <converter_value>
                if len(parts) != 3:
                    logger.error("Converter command requires format: converter <sdr_index> <converter_value>")
                    return
                
                try:
                    converter_value = int(parts[2])
                    if converter_value not in [0, 1, 2]:
                        logger.error("Converter value must be 0 (none), 1 (upconverter), or 2 (dnconverter)")
                        return
                    
                    # Find the target SDR
                    target_sdr = None
                    for sdr in self.local_sdrs:
                        if sdr.device_index + 1 == local_index:  # device_index is 0-based, local_index is 1-based
                            target_sdr = sdr
                            break
                    
                    if not target_sdr:
                        logger.error(f"SDR with local index {local_index} not found")
                        return
                    
                    # Check if serial number needs to be changed
                    current_serial = target_sdr.serial_number
                    current_first_digit = current_serial[0] if len(current_serial) >= 1 else "0"
                    required_first_digit = str(converter_value)
                    
                    final_serial = current_serial
                    
                    # If the first digit doesn't match the converter value, change the serial number
                    if current_first_digit != required_first_digit:
                        logger.info(f"🔧 Serial number first digit mismatch: {current_first_digit} != {required_first_digit}")
                        logger.info(f"🔧 Changing serial number for converter compatibility...")
                        
                        config_manager = SDRConfigManager()
                        success, new_serial = config_manager.change_sdr_serial_number(current_serial, required_first_digit)
                        
                        if success:
                            logger.info(f"✅ Serial number changed: {current_serial} → {new_serial}")
                            
                            # Update config with new serial number
                            if config_manager.update_sdr_config_after_serial_change(current_serial, new_serial):
                                logger.info(f"✅ Configuration updated for new serial {new_serial}")
                                final_serial = new_serial
                                
                                # Update the target_sdr object with new serial
                                target_sdr.serial_number = new_serial
                                
                                # Trigger SDR re-discovery to pick up the new serial
                                logger.info("🔍 Triggering SDR re-discovery...")
                                await self.send_sdr_discovery()
                            else:
                                logger.error(f"❌ Failed to update configuration for new serial {new_serial}")
                                return
                        else:
                            logger.error(f"❌ Failed to change serial number from {current_serial}")
                            return
                    
                    # Set converter using existing logic
                    config_manager = SDRConfigManager()
                    current_config = config_manager.get_sdr_config(final_serial)
                    
                    # Update converter settings using complete converter info
                    converter_info = config_manager.get_converter_info(converter_value)
                    current_config["sdr_converter"] = converter_info["sdr_converter"]
                    current_config["sdr_converter_type"] = converter_info["sdr_converter_type"]
                    current_config["sdr_converter_offset"] = converter_info["sdr_converter_offset"]
                    
                    if config_manager.save_sdr_config(final_serial, current_config):
                        logger.info(f"✅ Set converter to {converter_value} ({converter_info['sdr_converter_type']}) for {final_serial}")
                        await self.send_sdr_discovery()  # Update server
                    else:
                        logger.error("Failed to save converter configuration")
                        
                except (ValueError, TypeError):
                    logger.error(f"Invalid converter value: {parts[2]}. Must be 0, 1, or 2")
            
            else:
                logger.warning(f"Unknown text command: {command}")
                
        except Exception as e:
            logger.error(f"Error handling text command '{message}': {e}")
    
    async def _handle_sdr_command(self, data: dict):
        """Handle a command for a local SDR"""
        local_index = data.get("local_index")
        command = data.get("command", {})
        
        logger.info(f"📡 Received command for local SDR {local_index}: {command.get('action', 'unknown')}")
        
        # Find the SDR
        target_sdr = None
        for sdr in self.local_sdrs:
            if sdr.device_index + 1 == local_index:  # device_index is 0-based, local_index is 1-based
                target_sdr = sdr
                break
        
        if not target_sdr:
            logger.error(f"SDR with local index {local_index} not found")
            return
        
        # Execute the command (this is a placeholder - implement actual SDR control)
        result = await self._execute_sdr_command(target_sdr, command)
        
        # Send response back to server
        response = {
            "type": "sdr_command_response",
            "local_index": local_index,
            "command": command,
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            if self.websocket and WEBSOCKETS_AVAILABLE:
                await self.websocket.send(json.dumps(response))
            else:
                logger.warning("WebSocket not available - cannot send command response")
        except Exception as e:
            logger.error(f"Failed to send command response: {e}")
    
    async def _execute_sdr_command(self, sdr: LocalSDR, command: dict) -> dict:
        """Execute a command on an SDR device (placeholder implementation)"""
        cmd_type = command.get("type")
        
        logger.info(f"Executing {cmd_type} command on {sdr.device_type} (Serial: {sdr.serial_number})")
        
        # Handle rename command
        if cmd_type == "sdr_rename":
            new_name = command.get("name")
            if not new_name or not isinstance(new_name, str):
                return {"status": "error", "message": "Name is required and must be a string"}
            
            old_name = sdr.sdr_name or f"SDR {sdr.serial_number}"
            sdr.sdr_name = new_name.strip()
            
            # Save the new name to configuration
            config_manager = SDRConfigManager()
            current_config = config_manager.get_sdr_config(sdr.serial_number)
            current_config["sdr_name"] = sdr.sdr_name
            
            if config_manager.save_sdr_config(sdr.serial_number, current_config):
                logger.info(f"📝 Renamed SDR {sdr.serial_number} from '{old_name}' to '{sdr.sdr_name}'")
                
                # Send updated discovery to server to reflect the name change
                await self.send_sdr_discovery()
                
                return {
                    "status": "success", 
                    "message": f"Renamed from '{old_name}' to '{sdr.sdr_name}'",
                    "old_name": old_name,
                    "new_name": sdr.sdr_name
                }
            else:
                return {"status": "error", "message": "Failed to save configuration"}
        
        # Handle sdr_name command (same as sdr_rename for compatibility)
        elif cmd_type == "sdr_name":
            new_name = command.get("name")
            if not new_name or not isinstance(new_name, str):
                return {"status": "error", "message": "Name is required and must be a string"}
            
            old_name = sdr.sdr_name or f"SDR {sdr.serial_number}"
            sdr.sdr_name = new_name.strip()
            
            # Save the new name to configuration
            config_manager = SDRConfigManager()
            current_config = config_manager.get_sdr_config(sdr.serial_number)
            current_config["sdr_name"] = sdr.sdr_name
            
            if config_manager.save_sdr_config(sdr.serial_number, current_config):
                logger.info(f"📝 Set SDR name for {sdr.serial_number} to '{sdr.sdr_name}' (was '{old_name}')")
                
                # Send updated discovery to server to reflect the name change
                await self.send_sdr_discovery()
                
                return {
                    "status": "success", 
                    "message": f"Set name to '{sdr.sdr_name}' (was '{old_name}')",
                    "old_name": old_name,
                    "new_name": sdr.sdr_name
                }
            else:
                return {"status": "error", "message": "Failed to save configuration"}
        
        # Handle other SDR parameter setting commands
        elif cmd_type == "sdr_frequency":
            frequency = command.get("frequency")
            if frequency is not None:
                try:
                    freq_value = float(frequency)
                    # Save frequency to configuration
                    config_manager = SDRConfigManager()
                    current_config = config_manager.get_sdr_config(sdr.serial_number)
                    current_config["sdr_frequency"] = freq_value
                    
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        logger.info(f"📻 Set frequency for {sdr.serial_number} to {freq_value} Hz")
                        # Update the SDR object
                        sdr.sdr_frequency = freq_value
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Frequency set to {freq_value} Hz"}
                    else:
                        return {"status": "error", "message": "Failed to save frequency configuration"}
                except ValueError:
                    return {"status": "error", "message": "Invalid frequency value"}
            return {"status": "error", "message": "Frequency value required"}
        
        elif cmd_type == "sdr_gain":
            gain = command.get("gain")
            if gain is not None:
                try:
                    gain_value = float(gain)
                    # Save gain to configuration
                    config_manager = SDRConfigManager()
                    current_config = config_manager.get_sdr_config(sdr.serial_number)
                    current_config["sdr_gain"] = gain_value  # Store as sdr_gain
                    
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        logger.info(f"📶 Set sdr_gain for {sdr.serial_number} to {gain_value} dB")
                        # Update the SDR object
                        sdr.gain = gain_value
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Gain set to {gain_value} dB"}
                    else:
                        return {"status": "error", "message": "Failed to save gain configuration"}
                except ValueError:
                    return {"status": "error", "message": "Invalid gain value"}
            return {"status": "error", "message": "Gain value required"}
        
        elif cmd_type == "sdr_biast":
            biast = command.get("biast") or command.get("value")
            if biast is not None:
                try:
                    # Handle on/off and 1/0 values
                    biast_value = 1 if str(biast).lower() in ["on", "1", "true", "yes"] else 0
                    # Save biast to configuration  
                    config_manager = SDRConfigManager()
                    current_config = config_manager.get_sdr_config(sdr.serial_number)
                    current_config["sdr_bias_tee"] = biast_value  # Store as sdr_bias_tee
                    
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        logger.info(f"⚡ Set sdr_bias_tee for {sdr.serial_number} to {biast_value}")
                        # Update the SDR object
                        sdr.sdr_bias_tee = biast_value
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Bias tee set to {biast_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save biast configuration"}
                except ValueError:
                    return {"status": "error", "message": "Invalid biast value"}
            return {"status": "error", "message": "Biast value required"}
        
        elif cmd_type == "sdr_ppm":
            ppm = command.get("ppm") or command.get("value")
            if ppm is not None:
                try:
                    ppm_value = int(float(ppm))
                    # Save ppm to configuration
                    config_manager = SDRConfigManager()
                    current_config = config_manager.get_sdr_config(sdr.serial_number)
                    current_config["sdr_ppm"] = ppm_value  # Store as sdr_ppm
                    
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        logger.info(f"📡 Set sdr_ppm for {sdr.serial_number} to {ppm_value}")
                        # Update the SDR object
                        sdr.sdr_ppm = ppm_value
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"PPM set to {ppm_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save ppm configuration"}
                except ValueError:
                    return {"status": "error", "message": "Invalid ppm value"}
            return {"status": "error", "message": "PPM value required"}
        
        elif cmd_type == "sdr_squelch":
            squelch = command.get("squelch") or command.get("value")
            if squelch is not None:
                try:
                    squelch_value = int(float(squelch))
                    # Save squelch to configuration
                    config_manager = SDRConfigManager()
                    current_config = config_manager.get_sdr_config(sdr.serial_number)
                    current_config["sdr_squelch"] = squelch_value  # Store as sdr_squelch
                    
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        logger.info(f"🔇 Set sdr_squelch for {sdr.serial_number} to {squelch_value}")
                        # Update the SDR object
                        sdr.sdr_squelch = squelch_value
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Squelch set to {squelch_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save squelch configuration"}
                except ValueError:
                    return {"status": "error", "message": "Invalid squelch value"}
            return {"status": "error", "message": "Squelch value required"}
        
        elif cmd_type == "sdr_bias_tee":
            bias_tee = command.get("bias_tee") or command.get("biast") or command.get("value")
            if bias_tee is not None:
                try:
                    # Handle on/off and 1/0 values
                    biast_value = 1 if str(bias_tee).lower() in ["on", "1", "true", "yes"] else 0
                    # Save bias_tee to configuration  
                    config_manager = SDRConfigManager()
                    current_config = config_manager.get_sdr_config(sdr.serial_number)
                    current_config["sdr_bias_tee"] = biast_value  # Store as sdr_bias_tee
                    
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        logger.info(f"⚡ Set sdr_bias_tee for {sdr.serial_number} to {biast_value}")
                        # Update the SDR object
                        sdr.sdr_bias_tee = biast_value
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Bias tee set to {biast_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save bias_tee configuration"}
                except ValueError:
                    return {"status": "error", "message": "Invalid bias_tee value"}
            return {"status": "error", "message": "Bias tee value required"}
        
        # Placeholder implementation - replace with actual SDR control logic
        elif cmd_type == "set_frequency":
            frequency = command.get("frequency")
            logger.info(f"Setting frequency to {frequency} Hz")
            return {"status": "success", "message": f"Frequency set to {frequency} Hz"}
            
        elif cmd_type == "set_gain":
            gain = command.get("gain")
            logger.info(f"Setting gain to {gain} dB")
            return {"status": "success", "message": f"Gain set to {gain} dB"}
            
        elif cmd_type == "start":
            logger.info("Starting SDR")
            return {"status": "success", "message": "SDR started"}
            
        elif cmd_type == "stop":
            logger.info("Stopping SDR")
            return {"status": "success", "message": "SDR stopped"}
            
        elif cmd_type == "get_status":
            return {
                "status": "success",
                "data": {
                    "serial_number": sdr.serial_number,
                    "device_type": sdr.device_type,
                    "status": "active",
                    "frequency": 100000000,  # 100 MHz placeholder
                    "sample_rate": 2048000,   # 2.048 MSps placeholder
                    "gain": 20.0              # 20 dB placeholder
                }
            }
            
        # Handle JSON API get_sdr commands
        elif cmd_type == "get_sdr":
            variable = command.get("variable")
            if not variable:
                return {"status": "error", "message": "Variable is required for get_sdr command"}
            
            # Create variable map for this SDR
            variable_map = {
                "sdr_name": sdr.sdr_name or "(not set)",
                "sdr_frequency": getattr(sdr, 'sdr_frequency', "(not set)"),
                "sdr_modulation": getattr(sdr, 'sdr_modulation', "(not set)"),
                "sdr_band": getattr(sdr, 'sdr_band', "(not set)"),
                "sdr_sample_rate": getattr(sdr, 'sdr_sample_rate', "(not set)"),
                "sdr_gain": getattr(sdr, 'sdr_gain', "(not set)"),
                "sdr_bias_tee": getattr(sdr, 'sdr_bias_tee', 0),
                "sdr_agc": getattr(sdr, 'sdr_agc', "(not set)"),
                "sdr_squelch": getattr(sdr, 'sdr_squelch', "(not set)"),
                "sdr_ppm": getattr(sdr, 'sdr_ppm', "(not set)"),
                
                # Advanced variables
                "sdr_dc_correction": getattr(sdr, 'sdr_dc_correction', False),
                "sdr_edge_correction": getattr(sdr, 'sdr_edge_correction', False),
                "sdr_deemphasis": getattr(sdr, 'sdr_deemphasis', False),
                "sdr_direct_i": getattr(sdr, 'sdr_direct_i', False),
                "sdr_direct_q": getattr(sdr, 'sdr_direct_q', False),
                "sdr_offset_tuning": getattr(sdr, 'sdr_offset_tuning', False),
                
                # Modem/Decoder variables
                "sdr_modem_decoder": getattr(sdr, 'sdr_modem_decoder', "(not set)"),
                "sdr_modem_lead_in": getattr(sdr, 'sdr_modem_lead_in', "(not set)"),
                "sdr_modem_sample_rate": getattr(sdr, 'sdr_modem_sample_rate', 48000),
                "sdr_modem_trailing": getattr(sdr, 'sdr_modem_trailing', "(not set)"),
                "sdr_modem_audio_device": getattr(sdr, 'sdr_modem_audio_device', "(not set)"),
                "sdr_modem_debug": getattr(sdr, 'sdr_modem_debug', False),
                
                # Read-only variables
                "sdr_status": getattr(sdr, 'status', "available"),
                "sdr_device_type": sdr.device_type,
                "sdr_serial_number": sdr.serial_number
            }
            
            if variable == "all":
                return {"status": "success", "variable": "all", "values": variable_map}
            elif variable in variable_map:
                value = variable_map[variable]
                return {"status": "success", "variable": variable, "value": value}
            else:
                return {"status": "error", "message": f"Unknown variable: {variable}. Available: {', '.join(sorted(variable_map.keys())) + ', all'}"}
        
        # Handle JSON API set_sdr commands  
        elif cmd_type == "set_sdr":
            variable = command.get("variable")
            value = command.get("value")
            
            if not variable:
                return {"status": "error", "message": "Variable is required for set_sdr command"}
            if value is None:
                return {"status": "error", "message": "Value is required for set_sdr command"}
            
            # Handle setting different variables
            config_manager = SDRConfigManager()
            current_config = config_manager.get_sdr_config(sdr.serial_number)
            
            if variable == "sdr_name":
                if not isinstance(value, str):
                    return {"status": "error", "message": "sdr_name must be a string"}
                old_name = sdr.sdr_name or "(not set)"
                sdr.sdr_name = value.strip()
                current_config["sdr_name"] = sdr.sdr_name
                
                if config_manager.save_sdr_config(sdr.serial_number, current_config):
                    logger.info(f"📝 Set SDR name for {sdr.serial_number} to '{sdr.sdr_name}' (was '{old_name}')")
                    await self.send_sdr_discovery()  # Update server with new name
                    return {"status": "success", "message": f"Set sdr_name to '{sdr.sdr_name}' (was '{old_name}')"}
                else:
                    return {"status": "error", "message": "Failed to save sdr_name configuration"}
                    
            elif variable == "sdr_frequency":
                try:
                    freq_value = float(value)
                    if freq_value <= 0:
                        return {"status": "error", "message": "Frequency must be positive"}
                    current_config["sdr_frequency"] = int(freq_value)  # Store as integer
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_frequency to {int(freq_value)} Hz"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_frequency configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": "Frequency must be a number"}
                    
            elif variable == "sdr_modulation":
                valid_modes = ["am", "fm", "usb", "lsb", "cw", "wfm", "nfm", "ft4", "ft8", "aprs", "ads-b", "adsb"]
                if value.lower() not in valid_modes:
                    return {"status": "error", "message": f"Modulation must be one of: {', '.join(valid_modes)}"}
                current_config["sdr_modulation"] = value.lower()  # Store as lowercase
                if config_manager.save_sdr_config(sdr.serial_number, current_config):
                    await self.send_sdr_discovery()
                    return {"status": "success", "message": f"Set sdr_modulation to {value.lower()}"}
                else:
                    return {"status": "error", "message": "Failed to save sdr_modulation configuration"}
                    
            elif variable == "sdr_band":
                # Store band in lowercase format for consistency
                current_config["sdr_band"] = str(value).lower()
                if config_manager.save_sdr_config(sdr.serial_number, current_config):
                    await self.send_sdr_discovery()
                    return {"status": "success", "message": f"Set sdr_band to {str(value).lower()}"}
                else:
                    return {"status": "error", "message": "Failed to save sdr_band configuration"}
                    
            elif variable == "sdr_converter_type":
                # Handle converter type - store in lowercase for consistency
                valid_types = ["none", "upconverter", "dnconverter"]
                value_lower = str(value).lower()
                
                if value_lower not in valid_types:
                    return {"status": "error", "message": f"Invalid converter type: {value}. Valid types: {valid_types}"}
                
                # Update converter type and related settings
                if value_lower == "none":
                    converter_info = {
                        "sdr_converter": 0,
                        "sdr_converter_type": "none", 
                        "sdr_converter_offset": 0,
                        "sdr_converter_description": "No converter - direct connection"
                    }
                elif value_lower == "upconverter":
                    converter_info = {
                        "sdr_converter": 1,
                        "sdr_converter_type": "upconverter",
                        "sdr_converter_offset": 125000000,
                        "sdr_converter_description": "UpConverter - HF to VHF translation"
                    }
                elif value_lower == "dnconverter":
                    converter_info = {
                        "sdr_converter": 2,
                        "sdr_converter_type": "dnconverter",
                        "sdr_converter_offset": -10700000000,
                        "sdr_converter_description": "DnConverter - microwave to VHF translation"
                    }
                
                # Update config with converter info
                current_config.update(converter_info)
                
                if config_manager.save_sdr_config(sdr.serial_number, current_config):
                    await self.send_sdr_discovery()
                    return {"status": "success", "message": f"Set sdr_converter_type to {value_lower}"}
                else:
                    return {"status": "error", "message": "Failed to save sdr_converter_type configuration"}
                    
            elif variable == "sdr_bias_tee":
                try:
                    # Convert value to integer (0 or 1)
                    biast_value = int(value) if str(value).isdigit() else 0
                    if biast_value not in [0, 1]:
                        return {"status": "error", "message": f"Invalid bias tee value: {value}. Must be 0 or 1"}
                    current_config["sdr_bias_tee"] = biast_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_bias_tee to {biast_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_bias_tee configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid bias tee value: {value}. Must be 0 or 1"}
                    
            elif variable == "sdr_gain":
                try:
                    gain_value = float(value)
                    current_config["sdr_gain"] = gain_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_gain to {gain_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_gain configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid gain value: {value}. Must be a number"}
                    
            elif variable == "sdr_agc":
                current_config["sdr_agc"] = str(value).lower()
                if config_manager.save_sdr_config(sdr.serial_number, current_config):
                    await self.send_sdr_discovery()
                    return {"status": "success", "message": f"Set sdr_agc to {str(value).lower()}"}
                else:
                    return {"status": "error", "message": "Failed to save sdr_agc configuration"}
                    
            elif variable == "sdr_ppm":
                try:
                    ppm_value = int(float(value))
                    current_config["sdr_ppm"] = ppm_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_ppm to {ppm_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_ppm configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid PPM value: {value}. Must be a number"}
                    
            elif variable == "sdr_sample_rate":
                try:
                    sample_rate_value = int(value)
                    current_config["sdr_sample_rate"] = sample_rate_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_sample_rate to {sample_rate_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_sample_rate configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid sample rate value: {value}. Must be a number"}
                    
            elif variable == "sdr_squelch":
                try:
                    squelch_value = float(value)
                    current_config["sdr_squelch"] = squelch_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_squelch to {squelch_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_squelch configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid squelch value: {value}. Must be a number"}
                    
            # Advanced Variables
            elif variable == "sdr_dc_correction":
                try:
                    dc_value = bool(str(value).lower() in ['true', '1', 'on', 'yes'])
                    current_config["sdr_dc_correction"] = dc_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_dc_correction to {dc_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_dc_correction configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid DC correction value: {value}. Must be true/false"}
                    
            elif variable == "sdr_edge_correction":
                try:
                    edge_value = bool(str(value).lower() in ['true', '1', 'on', 'yes'])
                    current_config["sdr_edge_correction"] = edge_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_edge_correction to {edge_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_edge_correction configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid edge correction value: {value}. Must be true/false"}
                    
            elif variable == "sdr_deemphasis":
                try:
                    deemph_value = bool(str(value).lower() in ['true', '1', 'on', 'yes'])
                    current_config["sdr_deemphasis"] = deemph_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_deemphasis to {deemph_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_deemphasis configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid deemphasis value: {value}. Must be true/false"}
                    
            elif variable == "sdr_direct_i":
                try:
                    direct_i_value = bool(str(value).lower() in ['true', '1', 'on', 'yes'])
                    current_config["sdr_direct_i"] = direct_i_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_direct_i to {direct_i_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_direct_i configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid direct I value: {value}. Must be true/false"}
                    
            elif variable == "sdr_direct_q":
                try:
                    direct_q_value = bool(str(value).lower() in ['true', '1', 'on', 'yes'])
                    current_config["sdr_direct_q"] = direct_q_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_direct_q to {direct_q_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_direct_q configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid direct Q value: {value}. Must be true/false"}
                    
            elif variable == "sdr_offset_tuning":
                try:
                    offset_value = bool(str(value).lower() in ['true', '1', 'on', 'yes'])
                    current_config["sdr_offset_tuning"] = offset_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_offset_tuning to {offset_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_offset_tuning configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid offset tuning value: {value}. Must be true/false"}
                    
            # Modem/Decoder Variables
            elif variable == "sdr_modem_decoder":
                valid_decoders = ["ft4", "ft8", "jt65", "msk", "q65", "wspr", "js8", "ads-b", "aprs", "none"]
                if str(value).lower() not in valid_decoders:
                    return {"status": "error", "message": f"Modem decoder must be one of: {', '.join(valid_decoders)}"}
                # Store in lowercase format, handle "none" special case
                decoder_value = str(value).lower() if value and str(value).lower() != "none" else None
                current_config["sdr_modem_decoder"] = decoder_value
                if config_manager.save_sdr_config(sdr.serial_number, current_config):
                    await self.send_sdr_discovery()
                    return {"status": "success", "message": f"Set sdr_modem_decoder to {decoder_value}"}
                else:
                    return {"status": "error", "message": "Failed to save sdr_modem_decoder configuration"}
                    
            elif variable == "sdr_modem_lead_in":
                try:
                    lead_in_value = int(value)
                    if lead_in_value < 0:
                        return {"status": "error", "message": "Lead-in time must be non-negative"}
                    current_config["sdr_modem_lead_in"] = lead_in_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_modem_lead_in to {lead_in_value} ms"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_modem_lead_in configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid lead-in value: {value}. Must be a number"}
                    
            elif variable == "sdr_modem_sample_rate":
                try:
                    modem_sr_value = int(value)
                    if modem_sr_value <= 0:
                        return {"status": "error", "message": "Modem sample rate must be positive"}
                    current_config["sdr_modem_sample_rate"] = modem_sr_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_modem_sample_rate to {modem_sr_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_modem_sample_rate configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid modem sample rate value: {value}. Must be a number"}
                    
            elif variable == "sdr_modem_trailing":
                try:
                    trailing_value = int(value)
                    if trailing_value < 0:
                        return {"status": "error", "message": "Trailing time must be non-negative"}
                    current_config["sdr_modem_trailing"] = trailing_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_modem_trailing to {trailing_value} ms"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_modem_trailing configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid trailing value: {value}. Must be a number"}
                    
            elif variable == "sdr_modem_audio_device":
                current_config["sdr_modem_audio_device"] = str(value).lower()  # Store in lowercase
                if config_manager.save_sdr_config(sdr.serial_number, current_config):
                    await self.send_sdr_discovery()
                    return {"status": "success", "message": f"Set sdr_modem_audio_device to {str(value).lower()}"}
                else:
                    return {"status": "error", "message": "Failed to save sdr_modem_audio_device configuration"}
                    
            elif variable == "sdr_modem_debug":
                try:
                    debug_value = bool(str(value).lower() in ['true', '1', 'on', 'yes'])
                    current_config["sdr_modem_debug"] = debug_value
                    if config_manager.save_sdr_config(sdr.serial_number, current_config):
                        await self.send_sdr_discovery()
                        return {"status": "success", "message": f"Set sdr_modem_debug to {debug_value}"}
                    else:
                        return {"status": "error", "message": "Failed to save sdr_modem_debug configuration"}
                except (ValueError, TypeError):
                    return {"status": "error", "message": f"Invalid modem debug value: {value}. Must be true/false"}
                    
            else:
                available_vars = [
                    # Basic variables
                    "sdr_name", "sdr_frequency", "sdr_modulation", "sdr_band", "sdr_bias_tee", 
                    "sdr_gain", "sdr_agc", "sdr_ppm", "sdr_sample_rate", "sdr_squelch",
                    # Advanced variables
                    "sdr_dc_correction", "sdr_edge_correction", "sdr_deemphasis", 
                    "sdr_direct_i", "sdr_direct_q", "sdr_offset_tuning",
                    # Converter variables
                    "sdr_converter", "sdr_converter_type", "sdr_converter_offset", "sdr_converter_description",
                    # Modem variables
                    "sdr_modem_decoder", "sdr_modem_lead_in", "sdr_modem_sample_rate", 
                    "sdr_modem_trailing", "sdr_modem_audio_device", "sdr_modem_debug"
                ]
                return {"status": "error", "message": f"Variable '{variable}' is not settable. Available: {', '.join(available_vars)}"}
        
        else:
            return {"status": "error", "message": f"Unknown command type: {cmd_type}"}
    
    async def _handle_json_api_command(self, data: dict):
        """Handle JSON API commands forwarded from the server"""
        command = data.get("command")
        local_index = data.get("local_index")
        original_data = data.get("data", {})
        response_to = data.get("response_to")
        
        logger.info(f"📡 Received JSON API command: {command} for local SDR {local_index}")
        
        # Find the SDR
        target_sdr = None
        for sdr in self.local_sdrs:
            if sdr.device_index + 1 == local_index:  # device_index is 0-based, local_index is 1-based
                target_sdr = sdr
                break
        
        if not target_sdr:
            error_response = {"error": f"SDR with local index {local_index} not found"}
            await self._send_json_api_response(response_to, error_response)
            return
        
        # Execute the JSON API command using existing _execute_sdr_command logic
        if command == "get_sdr":
            variable = original_data.get("variable")
            if not variable:
                error_response = {"error": "Variable is required for get command"}
                await self._send_json_api_response(response_to, error_response)
                return
            
            # Create command for existing handler (note: uses "type" not "command")
            get_command = {"type": "get_sdr", "variable": variable}
            result = await self._execute_sdr_command(target_sdr, get_command)
            await self._send_json_api_response(response_to, result)
            
        elif command == "set_sdr":
            variable = original_data.get("variable")
            value = original_data.get("value")
            
            if not variable:
                error_response = {"error": "Variable is required for set command"}
                await self._send_json_api_response(response_to, error_response)
                return
            if value is None:
                error_response = {"error": "Value is required for set command"}
                await self._send_json_api_response(response_to, error_response)
                return
            
            # Create command for existing handler (note: uses "type" not "command")
            set_command = {"type": "set_sdr", "variable": variable, "value": value}
            result = await self._execute_sdr_command(target_sdr, set_command)
            await self._send_json_api_response(response_to, result)
            
        elif command == "set_converter":
            converter_value = original_data.get("value")
            
            if converter_value is None:
                error_response = {"error": "Value is required for set_converter command"}
                await self._send_json_api_response(response_to, error_response)
                return
            
            try:
                converter_int = int(converter_value)
                if converter_int not in [0, 1, 2]:
                    error_response = {"error": "Converter value must be 0 (none), 1 (upconverter), or 2 (dnconverter)"}
                    await self._send_json_api_response(response_to, error_response)
                    return
                
                # Check if serial number needs to be changed
                current_serial = target_sdr.serial_number
                current_first_digit = current_serial[0] if len(current_serial) >= 1 else "0"
                required_first_digit = str(converter_int)
                
                final_serial = current_serial
                
                # If the first digit doesn't match the converter value, change the serial number
                if current_first_digit != required_first_digit:
                    logger.info(f"🔧 Serial number first digit mismatch: {current_first_digit} != {required_first_digit}")
                    logger.info(f"🔧 Changing serial number for converter compatibility...")
                    
                    config_manager = SDRConfigManager()
                    success, new_serial = config_manager.change_sdr_serial_number(current_serial, required_first_digit)
                    
                    if success:
                        logger.info(f"✅ Serial number changed: {current_serial} → {new_serial}")
                        
                        # Update config with new serial number
                        if config_manager.update_sdr_config_after_serial_change(current_serial, new_serial):
                            logger.info(f"✅ Configuration updated for new serial {new_serial}")
                            final_serial = new_serial
                            
                            # Update the target_sdr object with new serial
                            target_sdr.serial_number = new_serial
                            
                            # Trigger SDR re-discovery to pick up the new serial
                            logger.info("🔍 Triggering SDR re-discovery...")
                            await self.send_sdr_discovery()
                        else:
                            error_response = {"error": f"Failed to update configuration for new serial {new_serial}"}
                            await self._send_json_api_response(response_to, error_response)
                            return
                    else:
                        error_response = {"error": f"Failed to change serial number from {current_serial}"}
                        await self._send_json_api_response(response_to, error_response)
                        return
                
                # Update converter configuration using complete converter info
                config_manager = SDRConfigManager()
                current_config = config_manager.get_sdr_config(final_serial)
                
                converter_info = config_manager.get_converter_info(converter_int)
                current_config["sdr_converter"] = converter_info["sdr_converter"]
                current_config["sdr_converter_type"] = converter_info["sdr_converter_type"]
                current_config["sdr_converter_offset"] = converter_info["sdr_converter_offset"]
                
                if config_manager.save_sdr_config(final_serial, current_config):
                    success_response = {
                        "success": True,
                        "message": f"Converter set to {converter_int} ({converter_info['sdr_converter_type']})",
                        "sdr_converter": converter_info["sdr_converter"],
                        "sdr_converter_type": converter_info["sdr_converter_type"],
                        "sdr_converter_offset": converter_info["sdr_converter_offset"],
                        "serial_number": final_serial
                    }
                    await self._send_json_api_response(response_to, success_response)
                    await self.send_sdr_discovery()  # Update server
                else:
                    error_response = {"error": "Failed to save converter configuration"}
                    await self._send_json_api_response(response_to, error_response)
                    
            except (ValueError, TypeError):
                error_response = {"error": f"Invalid converter value: {converter_value}. Must be 0, 1, or 2"}
                await self._send_json_api_response(response_to, error_response)
        
        elif command == "get_converter":
            # Get current converter configuration
            config_manager = SDRConfigManager()
            current_config = config_manager.get_sdr_config(target_sdr.serial_number)
            
            converter_value = current_config.get("sdr_converter", 0)
            converter_type = current_config.get("sdr_converter_type", "none")
            converter_offset = current_config.get("sdr_converter_offset", 0)
            
            converter_response = {
                "success": True,
                "sdr_converter": converter_value,
                "sdr_converter_type": converter_type,
                "sdr_converter_offset": converter_offset,
                "serial_number": target_sdr.serial_number
            }
            await self._send_json_api_response(response_to, converter_response)
            
        else:
            error_response = {"error": f"Unknown JSON API command: {command}"}
            await self._send_json_api_response(response_to, error_response)
    
    async def _send_json_api_response(self, response_to: str, response: dict):
        """Send JSON API response back to the server for forwarding to the original client"""
        if not self.websocket:
            logger.error("Cannot send JSON API response - not connected to server")
            return
        
        response_message = {
            "type": "json_api_response",
            "response_to": response_to,
            "response": response
        }
        
        await self.websocket.send(json.dumps(response_message))
        logger.info(f"📤 Sent JSON API response to server for forwarding")
    
    async def send_heartbeat(self):
        """Send periodic heartbeat to server with connection monitoring"""
        while self.running:
            try:
                await asyncio.sleep(30)  # Send heartbeat every 30 seconds
                
                if not self.websocket:
                    logger.debug("💓 Skipping heartbeat - not connected to server")
                    continue
                
                # Check if connection is still alive before sending heartbeat
                if getattr(self.websocket, 'closed', False):
                    logger.warning("💓 Connection closed, marking for reconnection")
                    self.websocket = None
                    continue
                
                heartbeat = {
                    "type": "heartbeat",
                    "timestamp": datetime.now().isoformat(),
                    "sdr_count": len(self.local_sdrs)
                }
                
                await self.websocket.send(json.dumps(heartbeat))
                logger.debug("💓 Sent heartbeat to server")
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                # Handle any heartbeat errors by marking connection as lost
                logger.warning(f"💓 Heartbeat failed: {e}")
                self.websocket = None
    
    async def _handle_host_inventory_response(self, data: dict):
        """Handle host inventory response from server"""
        hosts = data.get("hosts", [])
        logger.info(f"Received host inventory: {len(hosts)} hosts with SDRs")
        
        for host in hosts:
            host_id = host.get("host_id")
            ip = host.get("ip")
            sdr_count = len(host.get("sdr_devices", []))
            logger.info(f"  Host {host_id} ({ip}): {sdr_count} SDR devices")
            
            for sdr in host.get("sdr_devices", []):
                global_idx = sdr.get("global_index")
                device_type = sdr.get("device_type")
                serial = sdr.get("serial_number")
                status = sdr.get("status")
                logger.info(f"    SDR[{global_idx}]: {device_type} (SN: {serial}) - {status}")
                
                # Log current configuration if available
                config = sdr.get("configuration", {})
                if config.get("frequency"):
                    freq = config.get("frequency")
                    mod = config.get("modulation", "Unknown")
                    logger.info(f"      Current: {freq} Hz, {mod}")
    
    async def _handle_host_inventory(self, data: dict):
        """Handle host inventory message from server"""
        hosts = data.get("hosts", [])
        logger.info(f"Received host inventory: {len(hosts)} hosts with SDRs")
        
        for host in hosts:
            host_id = host.get("host_id")
            ip = host.get("ip_address")
            sdr_count = host.get("sdr_count", 0)
            logger.info(f"  Host {host_id} ({ip}): {sdr_count} SDR devices")
            
            for sdr in host.get("sdrs", []):
                global_idx = sdr.get("global_index")
                device_type = sdr.get("device_type")
                serial = sdr.get("serial_number")
                logger.info(f"    SDR[{global_idx}]: {device_type} (SN: {serial})")
    
    # ========== MODEM DECODER METHODS ==========
    
    def check_modem_dependencies(self) -> Dict[str, bool]:
        """Check if modem decoding dependencies are available"""
        dependencies = {
            'af2udp': os.path.exists(self.af2udp_path),
            'websocketd': os.path.exists(self.websocketd_path),
            'ft8modem': os.path.exists(self.ft8modem_path),
            'jt9': os.path.exists(self.jt9_path)
        }
        return dependencies
    
    def get_device_ports(self, device_index: int, serial_number: Optional[str] = None) -> Dict[str, int]:
        """Calculate UDP and WebSocket ports for a device"""
        if serial_number:
            # Use last 3 digits of serial number for port calculation
            # e.g., serial 10000003 -> port 3103
            try:
                last_digits = int(serial_number[-3:])
                udp_port = 3100 + last_digits
            except (ValueError, IndexError):
                # Fall back to device index if serial parsing fails
                udp_port = self.UDP_AUDIO_BASE_PORT + device_index
        else:
            udp_port = self.UDP_AUDIO_BASE_PORT + device_index
            
        return {
            'udp_port': udp_port,
            'websocket_port': self.FT8_WEBSOCKET_BASE_PORT + device_index
        }
    
    def should_start_decoder(self, sdr: LocalSDR) -> bool:
        """Determine if a decoder should be started for this SDR"""
        # Check if SDR has modem decoder configuration
        if hasattr(sdr, 'modem_decoder') and sdr.modem_decoder:
            mode = sdr.modem_decoder.upper()
            if mode in ['FT4', 'FT8', 'JS8', 'PSK31']:
                return True
        
        # Check if modulation is a digital mode
        if hasattr(sdr, 'modulation') and sdr.modulation:
            mode = sdr.modulation.upper()
            if mode in ['FT4', 'FT8', 'JS8', 'PSK31']:
                return True
        
        return False
    
    def start_modem_decoder(self, sdr: LocalSDR, device_index: int = 0) -> Dict[str, Any]:
        """Start modem decoder for an SDR device based on sdrctl.py implementation"""
        serial_number = sdr.serial_number
        try:
            
            # Check dependencies
            deps = self.check_modem_dependencies()
            missing_deps = [name for name, available in deps.items() if not available]
            
            if missing_deps:
                logger.warning(f"Missing modem dependencies for {serial_number}: {', '.join(missing_deps)}")
                return {
                    "success": False,
                    "error": f"Missing dependencies: {', '.join(missing_deps)}",
                    "dependencies": deps
                }
            
            # Get modem configuration
            mode = getattr(sdr, 'modem_decoder', None) or getattr(sdr, 'sdr_modulation', '')
            if not mode:
                return {"success": False, "error": "No modem mode configured"}
            
            mode = mode.upper()
            if mode not in ['FT4', 'FT8', 'JS8', 'PSK31']:
                return {"success": False, "error": f"Unsupported modem mode: {mode}"}
            
            frequency = getattr(sdr, 'sdr_frequency', 0)
            sample_rate = getattr(sdr, 'modem_sample_rate', 48000)
            
            # Calculate ports using serial number logic
            ports = self.get_device_ports(device_index, serial_number)
            
            # Stop any existing decoder for this device
            self.stop_modem_decoder(serial_number)
            
            logger.info(f"🎯 Starting {mode} decoder for {serial_number} on {frequency} Hz")
            
            # Start decoder pipeline based on sdrctl.py implementation
            processes = []
            
            # Calculate the upconverter frequency if needed
            # Load converter info from config and set default values
            upconverter_freq = frequency
            ppm_correction = "-6"       # Default PPM
            gain_value = "49.6"         # Default gain  
            sample_rate = 48000         # Default to 48kHz for FT8
            
            config = self.config_manager._load_config()
            if serial_number in config.get("sdr_devices", {}):
                device_config = config["sdr_devices"][serial_number]
                converter_type = device_config.get("sdr_converter_type", "none")
                if converter_type == "upconverter":
                    offset = device_config.get("sdr_converter_offset", 125000000)  # Default 125MHz
                    upconverter_freq = frequency + offset
                    logger.info(f"Using upconverter: {frequency} Hz -> {upconverter_freq} Hz")
                
                # Get stored PPM correction (override default)
                ppm_correction = str(device_config.get("sdr_ppm_correction", ppm_correction))
                
                # Get stored gain value (override default)
                gain_value = str(device_config.get("sdr_gain", gain_value))
                
                # Get stored sample rate (override default)
                stored_sample_rate = device_config.get("sdr_sample_rate")
                if stored_sample_rate:
                    sample_rate = int(stored_sample_rate)
            
            # Start RTL-FM -> af2udp pipeline in background
            rtl_af_cmd = (
                f"/usr/local/bin/rtl_fm -d {serial_number} -f {upconverter_freq} -M usb "
                f"-s {sample_rate} -p {ppm_correction} -g {gain_value} -E dc | "
                f"{self.af2udp_path} {ports['udp_port']}"
            )
            
            # Start ft8modem separately
            ft8_cmd = [
                self.ft8modem_path,
                "-r", str(sample_rate),
                "-j", self.jt9_path,
                mode,
                f"udp:{ports['udp_port']}"
            ]
            
            logger.info(f"Starting RTL-FM -> af2udp: {rtl_af_cmd}")
            logger.info(f"Starting ft8modem: {' '.join(ft8_cmd)}")
            
            # Start RTL-FM -> af2udp pipeline
            rtl_af_process = subprocess.Popen(
                rtl_af_cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            
            # Start ft8modem
            ft8_process = subprocess.Popen(
                ft8_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,  # Line buffered
                preexec_fn=os.setsid
            )
            
            processes.append(('rtl_af_pipeline', rtl_af_process))
            processes.append(('ft8modem', ft8_process))
            
            # Start monitoring thread for FT8 decode output
            band = get_band_from_frequency(frequency)
            sdr_id = serial_number[-3:]  # Last 3 digits for display
            
            monitor_thread = threading.Thread(
                target=self._monitor_ft8_output,
                args=(ft8_process, serial_number, sdr_id, band),
                daemon=True
            )
            monitor_thread.start()
            
            # Give processes time to start
            time.sleep(0.5)
            
            # Check if all processes started successfully
            failed_processes = []
            for name, process in processes:
                if process.poll() is not None:
                    failed_processes.append(name)
                    # Log any error output
                    try:
                        stderr_output = process.stderr.read().decode() if process.stderr else ""
                        if stderr_output:
                            logger.error(f"{name} error: {stderr_output}")
                    except:
                        pass
            
            if failed_processes:
                # Clean up failed processes
                for name, process in processes:
                    try:
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    except:
                        pass
                
                return {
                    "success": False,
                    "error": f"Failed to start processes: {', '.join(failed_processes)}"
                }
            
            # Store decoder information
            self.active_decoders[serial_number] = {
                "mode": mode,
                "frequency": frequency,
                "sample_rate": sample_rate,
                "udp_port": ports['udp_port'],
                "websocket_port": ports['websocket_port'],
                "processes": processes,
                "started_at": datetime.now(),
                "device_index": device_index
            }
            
            # Save PIDs for cleanup
            pid_files = {}
            for name, process in processes:
                pid_file = f"/tmp/{name}_{serial_number}.pid"
                with open(pid_file, 'w') as f:
                    f.write(str(process.pid))
                pid_files[name] = pid_file
            
            self.active_decoders[serial_number]["pid_files"] = pid_files
            
            logger.info(f"✅ Started {mode} decoder for {serial_number} - UDP:{ports['udp_port']} WS:{ports['websocket_port']}")
            
            return {
                "success": True,
                "mode": mode,
                "udp_port": ports['udp_port'],
                "websocket_port": ports['websocket_port'],
                "processes": len(processes),
                "message": f"{mode} decoder started for {serial_number}"
            }
            
        except Exception as e:
            logger.error(f"Failed to start modem decoder for {serial_number}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _monitor_ft8_output(self, process: subprocess.Popen, serial_number: str, sdr_id: str, band: str):
        """Monitor ft8modem output for decode lines and send to WebSocket"""
        logger.info(f"🎯 Starting FT8 decode monitoring for SDR[{sdr_id}]-{band}")
        
        try:
            while process.poll() is None:
                if process.stdout:
                    line = process.stdout.readline()
                    if line:
                        line = line.strip()
                        if line.startswith('D: FT8'):
                            # Parse the decode line and create JSON format
                            decode_data = parse_ft8_decode_line(line, sdr_id, band)
                            if decode_data:
                                # Send to WebSocket if connected
                                if hasattr(self, 'websocket') and self.websocket:
                                    asyncio.create_task(self._send_ft8_decode(decode_data))
                                
                                # Log the JSON decode data
                                logger.info(f"📡 FT8 Decode: {json.dumps(decode_data)}")
                            else:
                                # Log invalid decodes too
                                logger.warning(f"❌ Failed to parse decode: {line}")
                        elif line:
                            # Log other ft8modem output for debugging
                            logger.debug(f"FT8[{sdr_id}]: {line}")
        except Exception as e:
            logger.error(f"Error monitoring FT8 output for {serial_number}: {e}")
        finally:
            logger.info(f"🛑 FT8 decode monitoring ended for SDR[{sdr_id}]-{band}")
    
    async def _send_ft8_decode(self, decode_data: dict):
        """Send FT8 decode data to WebSocket"""
        try:
            if self.websocket:
                message = {
                    "type": "ft8_decode",
                    "data": decode_data,
                    "timestamp": time.time()
                }
                await self.websocket.send(json.dumps(message))
        except Exception as e:
            logger.error(f"Error sending FT8 decode to WebSocket: {e}")

    def stop_modem_decoder(self, serial_number: str) -> Dict[str, Any]:
        """Stop modem decoder for an SDR device"""
        try:
            if serial_number not in self.active_decoders:
                return {"success": True, "message": "No decoder was running"}
            
            decoder_info = self.active_decoders[serial_number]
            processes = decoder_info.get("processes", [])
            
            logger.info(f"🛑 Stopping modem decoder for {serial_number}")
            
            # Stop all processes
            stopped_processes = []
            for name, process in processes:
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    process.wait(timeout=5)
                    stopped_processes.append(name)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't respond to SIGTERM
                    try:
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                        stopped_processes.append(f"{name} (force killed)")
                    except:
                        pass
                except:
                    pass
            
            # Clean up PID files
            pid_files = decoder_info.get("pid_files", {})
            for name, pid_file in pid_files.items():
                try:
                    os.remove(pid_file)
                except:
                    pass
            
            # Remove from active decoders
            del self.active_decoders[serial_number]
            
            logger.info(f"✅ Stopped modem decoder for {serial_number}")
            
            return {
                "success": True,
                "stopped_processes": stopped_processes,
                "message": f"Decoder stopped for {serial_number}"
            }
            
        except Exception as e:
            logger.error(f"Failed to stop modem decoder for {serial_number}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_decoder_status(self, serial_number: str) -> Dict[str, Any]:
        """Get status of decoder for an SDR device"""
        if serial_number not in self.active_decoders:
            return {"status": "not_running"}
        
        decoder_info = self.active_decoders[serial_number]
        processes = decoder_info.get("processes", [])
        
        # Check if processes are still running
        running_processes = []
        failed_processes = []
        
        for name, process in processes:
            if process.poll() is None:
                running_processes.append(name)
            else:
                failed_processes.append(name)
        
        status = "running" if running_processes and not failed_processes else "failed"
        
        return {
            "status": status,
            "mode": decoder_info.get("mode"),
            "frequency": decoder_info.get("frequency"),
            "udp_port": decoder_info.get("udp_port"),
            "websocket_port": decoder_info.get("websocket_port"),
            "running_processes": running_processes,
            "failed_processes": failed_processes,
            "started_at": decoder_info.get("started_at").isoformat() if decoder_info.get("started_at") and hasattr(decoder_info.get("started_at"), 'isoformat') else None
        }
    
    def auto_start_decoders(self):
        """Automatically start decoders for SDRs that need them"""
        logger.info("🔍 Checking SDRs for automatic decoder startup...")
        
        device_index = 0
        started_count = 0
        
        for sdr in self.local_sdrs:
            if self.should_start_decoder(sdr):
                logger.info(f"📡 SDR {sdr.serial_number} needs decoder: {getattr(sdr, 'modem_decoder', getattr(sdr, 'modulation', 'unknown'))}")
                
                result = self.start_modem_decoder(sdr, device_index)
                if result.get("success"):
                    started_count += 1
                    device_index += 1
                else:
                    logger.error(f"Failed to start decoder for {sdr.serial_number}: {result.get('error')}")
        
        if started_count > 0:
            logger.info(f"✅ Started {started_count} modem decoder(s)")
        else:
            logger.info("ℹ️  No modem decoders needed")
    
    def stop_all_decoders(self):
        """Stop all active modem decoders"""
        if not self.active_decoders:
            return
        
        logger.info(f"🛑 Stopping {len(self.active_decoders)} active decoder(s)...")
        
        for serial_number in list(self.active_decoders.keys()):
            self.stop_modem_decoder(serial_number)
    
    # ========== END MODEM DECODER METHODS ==========

    async def run(self):
        """Main run loop for the SDR host with automatic reconnection"""
        self.running = True
        
        try:
            # Discover local SDR devices
            self.local_sdrs = SDRDiscovery.discover_all_sdr_devices()
            
            if not self.local_sdrs:
                logger.warning("No SDR devices found")
                return
            
            logger.info(f"Found {len(self.local_sdrs)} SDR devices:")
            for i, sdr in enumerate(self.local_sdrs, 1):
                logger.info(f"  {i}. {sdr.device_type} ({sdr.sdr_name}) - Serial: {sdr.serial_number}")
                if hasattr(sdr, 'sdr_converter_type') and sdr.sdr_converter_type != 'none':
                    logger.info(f"      🔧 {sdr.sdr_converter_description}")
                logger.info(f"      Config: sdr-host.json")
            
            # Auto-start modem decoders for configured SDRs
            self.auto_start_decoders()
            
            # Initial server discovery and connection
            logger.info("Listening for SDR server broadcasts on port 4210...")
            if not await self.discover_server():
                logger.error("Could not find SDR server initially")
                return
            
            if not await self.connect_to_server():
                logger.error("Could not connect to SDR server initially")
                return
            
            # Send initial SDR discovery information
            await self.send_sdr_discovery()
            
            # Start heartbeat task - it will handle its own connection monitoring
            heartbeat_task = asyncio.create_task(self.send_heartbeat())
            
            # Start message handling - it includes automatic reconnection logic
            logger.info("SDR Host running - waiting for commands...")
            message_task = asyncio.create_task(self.handle_server_messages())
            
            # Wait for both tasks (heartbeat and message handling)
            await asyncio.gather(heartbeat_task, message_task, return_exceptions=True)
            
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down SDR Host...")
        finally:
            self.running = False
            
            # Stop all modem decoders
            self.stop_all_decoders()
            
            if self.websocket and not getattr(self.websocket, 'closed', False):
                try:
                    await self.websocket.close()
                    logger.info("🔌 Closed connection to server")
                except Exception:
                    pass  # Connection might already be closed

# CLI Interface
async def main():
    host = SDRHost()
    await host.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSDR Host stopped.")
