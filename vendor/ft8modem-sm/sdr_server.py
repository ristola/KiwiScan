#!/usr/bin/env python3
"""
SDR Server - Central management server for distributed SDR hosts
Broadcasts availability via UDP and manages SDR hosts via WebSocket
"""

import sys
import subprocess
import platform
import os

def get_system_info():
    """Get system information for package installation"""
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
        'websockets': 'websockets',
        'psutil': 'psutil'
    }
    
    missing_packages = []
    
    # First pass: check what's missing
    for module_name, package_name in required_packages.items():
        try:
            __import__(module_name)
            print(f"✓ {module_name} is available")
        except ImportError:
            print(f"✗ {module_name} is missing")
            missing_packages.append((module_name, package_name))
    
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
            print("\n🔄 Then restart the script.")
            sys.exit(1)
        else:
            print("\n✅ All packages successfully installed!")
    else:
        print("✅ All required dependencies are available.")

def check_and_setup_service():
    """Check if sdr_server is set up as a systemd service, create if needed"""
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
        print("💡 To set up as a service, run: sudo python3 sdr_server.py")
        return
    
    service_name = "sdr-server"
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
                return
            else:
                print(f"🟡 Service '{service_name}' exists but is not running")
                print("🚀 Starting service...")
                subprocess.run(["systemctl", "start", service_name], check=True)
                print(f"✅ Service '{service_name}' started successfully")
                return
        
        # Service doesn't exist, create it
        print(f"🔧 Creating systemd service '{service_name}'...")
        
        service_content = f"""[Unit]
Description=SDR Server - Distributed Software Defined Radio Management
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
SyslogIdentifier=sdr-server

# Environment variables
Environment=PYTHONUNBUFFERED=1

# Security settings
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log /tmp

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
        print("\n🎯 The SDR server will now start automatically on boot!")
        print("🔄 Exiting - service is now managed by systemd")
        sys.exit(0)
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to set up service: {e}")
    except Exception as e:
        print(f"❌ Error setting up service: {e}")

# Check dependencies before importing them
if __name__ == "__main__":
    check_dependencies()
    check_and_setup_service()
    # If we get here, all dependencies are available

import asyncio
import json
import logging
import socket
import time
import websockets
import psutil
import os
import signal
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sdr_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Enhanced frequency parsing and band mapping (from sdrctl.py)
def parse_frequency(freq_str, current_mode=None):
    """Parse frequency string with enhanced format support and band mapping"""
    freq_str = str(freq_str).upper().strip()
    
    # Band frequency mappings
    # Updated frequencies for 125MHz upconverter (target + 125MHz)
    band_freqs = {
        '160M': 1840000,    # 1.840 MHz (no upconverter - too low)
        '80M': 128573000,   # 3.573 + 125 MHz = 128.573 MHz  
        '60M': 5357000,     # 5.357 MHz (no upconverter - too low)
        '40M': 132074000,   # 7.074 + 125 MHz = 132.074 MHz
        '30M': 10136000,    # 10.136 MHz (no upconverter - too low)
        '20M': 139074000,   # 14.074 + 125 MHz = 139.074 MHz
        '17M': 143100000,   # 18.100 + 125 MHz = 143.100 MHz
        '15M': 21074000,    # 21.074 MHz (no upconverter - HF direct)
        '12M': 24915000,    # 24.915 MHz (no upconverter - HF direct)
        '10M': 28074000,    # 28.074 MHz (no upconverter - HF direct)
        '6M': 50313000      # 50.313 MHz (no upconverter - VHF direct)
    }
    
    # FT8 frequencies for digital modes (with 125MHz upconverter)
    ft8_freqs = {
        "160M": 1840000,    # 1.840 MHz (no upconverter - too low)
        "80M": 128573000,   # 3.573 + 125 MHz = 128.573 MHz
        "60M": 5357000,     # 5.357 MHz (no upconverter - too low)
        "40M": 132074000,   # 7.074 + 125 MHz = 132.074 MHz
        "30M": 10136000,    # 10.136 MHz (no upconverter - too low)
        "20M": 139074000,   # 14.074 + 125 MHz = 139.074 MHz
        "17M": 143100000,   # 18.100 + 125 MHz = 143.100 MHz
        "15M": 21074000,    # 21.074 MHz (no upconverter - HF direct)
        "12M": 24915000,    # 24.915 MHz (no upconverter - HF direct)
        "10M": 28074000,    # 28.074 MHz (no upconverter - HF direct)
        "6M": 50313000      # 50.313 MHz (no upconverter - VHF direct)
    }
    
    # FT4 frequencies for digital modes
    ft4_freqs = {
        "160M": 1843000,    # 1.843 MHz
        "80M": 3575000,     # 3.575 MHz
        "40M": 7047500,     # 7.0475 MHz
        "20M": 14080000,    # 14.080 MHz
        "15M": 21140000,    # 21.140 MHz
        "10M": 28180000,    # 28.180 MHz
        "6M": 50318000      # 50.318 MHz
    }
    
    # Mode-specific band frequency selection
    if current_mode and current_mode.lower() == 'ft4' and freq_str in ft4_freqs:
        return ft4_freqs[freq_str]
    elif current_mode and current_mode.lower() in ['ft8', 'ft4'] and freq_str in ft8_freqs:
        return ft8_freqs[freq_str]
    elif freq_str in band_freqs:
        return band_freqs[freq_str]
    
    # Handle numeric frequencies
    if freq_str.endswith('M'):
        return int(float(freq_str[:-1]) * 1e6)
    elif freq_str.endswith('K'):
        return int(float(freq_str[:-1]) * 1e3)
    else:
        # Handle raw frequencies (could be decimal or integer)
        try:
            # If it contains a decimal, assume MHz
            if '.' in freq_str:
                return int(float(freq_str) * 1e6)
            else:
                return int(freq_str)
        except ValueError:
            raise ValueError(f"Invalid frequency format: {freq_str}")

def format_freq_display(freq_hz):
    """Format frequency for display"""
    if freq_hz >= 1000000:
        return f"{freq_hz / 1000000:.3f}"
    elif freq_hz >= 1000:
        return f"{freq_hz / 1000:.1f}k"
    else:
        return f"{freq_hz}"

@dataclass
class SDRDevice:
    """Represents an SDR device with comprehensive configuration"""
    serial_number: str
    device_type: str
    host_ip: str
    local_index: int  # Index on the host
    global_index: int  # Global enumerated index
    status: str = "offline"
    sdr_name: Optional[str] = None        # User-defined name for the SDR
    
    # Basic SDR Configuration
    frequency: Optional[float] = None  # Hz - must be set
    modulation: Optional[str] = None   # AM, FM, USB, LSB, CW - must be set
    band: Optional[str] = None        # Amateur radio band (2m, 70cm, 40m, etc.)
    sample_rate: int = 24000          # Default 24k
    gain: Optional[float] = None      # RF gain setting
    bias_tee: bool = False            # Default off
    agc: str = "auto"            # "auto" or level setting
    squelch: int = 0                  # Default 0/off
    ppm: int = 0                      # Range -20 to +20
    dc_correction: bool = False       # Default off
    edge_correction: bool = False     # Default off
    deemphasis: bool = False          # Default off
    direct_i: bool = False            # Default off
    direct_q: bool = False            # Default off
    offset_tuning: bool = False       # Default off
    
    # Converter Configuration
    converter: Optional[int] = None       # 0=none, 1=upconverter, 2=downconverter
    converter_type: Optional[str] = None  # "none", "upconverter", "downconverter"
    converter_offset: Optional[int] = None # Frequency offset in Hz
    converter_description: Optional[str] = None # Description text
    
    # Modem/Decoder Configuration
    modem_decoder: Optional[str] = None    # FT4, FT8, ADS-B, APRS, JT65, MSK, Q65
    modem_lead_in: Optional[int] = None   # Lead-in time in ms
    modem_sample_rate: int = 48000        # Default 48kHz
    modem_trailing: Optional[int] = None  # Trailing time in ms
    modem_audio_device: Optional[str] = None  # Audio device selection
    modem_debug: bool = False             # Default off

@dataclass
class SDRHost:
    """Represents an SDR host connection"""
    ip_address: str
    websocket: object
    connected_at: datetime
    last_heartbeat: datetime
    sdr_devices: List[SDRDevice]
    host_id: str

class SDRServer:
    def __init__(self, udp_port=4210, ws_port=4010):
        self.udp_port = udp_port
        self.ws_port = ws_port
        self.hosts: Dict[str, SDRHost] = {}  # Only actual SDR hosts
        self.control_points: Dict[str, dict] = {}  # WebSocket control clients
        self.sdr_global_counter = 0
        self.running = False
        
        # Get local IP address
        self.local_ip = self._get_local_ip()
        logger.info(f"SDR Server initialized on IP: {self.local_ip}")

    def _get_local_ip(self) -> str:
        """Get the local IP address"""
        try:
            # Connect to a remote address to determine local IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

    def _check_port_in_use(self, port: int) -> bool:
        """Check if a port is currently in use"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex(('localhost', port))
                return result == 0
        except Exception:
            return False

    def _find_processes_using_port(self, port: int) -> List[int]:
        """Find PIDs of processes using the specified port"""
        pids = []
        try:
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    # Get connections for this process
                    connections = proc.connections()
                    for conn in connections:
                        if conn.laddr and conn.laddr.port == port:
                            pids.append(proc.info['pid'])
                            logger.info(f"Found process {proc.info['name']} (PID: {proc.info['pid']}) using port {port}")
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        except (ImportError, NameError):
            # Fallback method using lsof if psutil is not available
            try:
                import subprocess
                result = subprocess.run(['lsof', '-ti', f':{port}'], 
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    pids = [int(pid.strip()) for pid in result.stdout.split() if pid.strip().isdigit()]
            except (subprocess.TimeoutExpired, FileNotFoundError, ValueError, ImportError):
                # If lsof is not available, try netstat (works on most systems)
                try:
                    import subprocess
                    result = subprocess.run(['netstat', '-tulpn'], 
                                          capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        for line in result.stdout.split('\n'):
                            if f':{port} ' in line:
                                # Extract PID from netstat output
                                parts = line.split()
                                for part in parts:
                                    if '/' in part:
                                        try:
                                            pid = int(part.split('/')[0])
                                            pids.append(pid)
                                        except ValueError:
                                            continue
                except (subprocess.TimeoutExpired, FileNotFoundError, ValueError, ImportError):
                    pass
        
        return pids

    def _cleanup_port(self, port: int) -> bool:
        """Clean up processes using the specified port"""
        logger.info(f"Checking for processes using port {port}...")
        
        if not self._check_port_in_use(port):
            logger.info(f"Port {port} is available")
            return True
        
        logger.warning(f"Port {port} is in use, attempting to clean up...")
        
        pids = self._find_processes_using_port(port)
        
        if not pids:
            logger.warning(f"Port {port} appears to be in use but no processes found")
            return False
        
        # Try to terminate processes gracefully first
        for pid in pids:
            try:
                logger.info(f"Attempting to terminate process {pid}...")
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                logger.info(f"Process {pid} already terminated")
            except PermissionError:
                logger.error(f"Permission denied when trying to terminate process {pid}")
            except Exception as e:
                logger.error(f"Error terminating process {pid}: {e}")
        
        # Wait a moment for graceful termination
        time.sleep(2)
        
        # Check if port is now free
        if not self._check_port_in_use(port):
            logger.info(f"Port {port} successfully cleaned up")
            return True
        
        # If still in use, try force kill
        remaining_pids = self._find_processes_using_port(port)
        for pid in remaining_pids:
            try:
                logger.warning(f"Force killing process {pid}...")
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                logger.info(f"Process {pid} already terminated")
            except PermissionError:
                logger.error(f"Permission denied when trying to force kill process {pid}")
            except Exception as e:
                logger.error(f"Error force killing process {pid}: {e}")
        
        # Final check
        time.sleep(1)
        if not self._check_port_in_use(port):
            logger.info(f"Port {port} successfully cleaned up after force kill")
            return True
        else:
            logger.error(f"Failed to clean up port {port}")
            return False

    def cleanup_ports(self) -> bool:
        """Clean up both UDP and WebSocket ports"""
        logger.info("Performing port cleanup before starting server...")
        
        udp_clean = self._cleanup_port(self.udp_port)
        ws_clean = self._cleanup_port(self.ws_port)
        
        if udp_clean and ws_clean:
            logger.info("All ports successfully cleaned up")
            return True
        else:
            if not udp_clean:
                logger.error(f"Failed to clean up UDP port {self.udp_port}")
            if not ws_clean:
                logger.error(f"Failed to clean up WebSocket port {self.ws_port}")
            return False

    def _get_decoder_for_modulation(self, modulation: str) -> str:
        """Get the appropriate decoder based on modulation type"""
        if not modulation:
            return "none"
        
        mod = modulation.lower().strip()
        
        # Map modulation types to decoders (return lowercase for storage)
        if mod in ["ft4"]:
            return "ft4"
        elif mod in ["ft8"]:
            return "ft8"
        elif mod in ["adsb", "ads-b"]:
            return "ads-b"
        elif mod in ["aprs"]:
            return "aprs"
        else:
            # For AM, FM, USB, LSB, CW, etc. - no specific decoder
            return "none"

    async def _send_modem_decoder_update(self, host, local_index: int, decoder_value: str):
        """Send updated modem_decoder value to host for config persistence"""
        try:
            # Convert "none" to empty/null for the host text command
            send_value = decoder_value if decoder_value != "none" else "None"
            # Send as a text command that the host can handle
            text_command = f"set {local_index} sdr_modem_decoder {send_value}"
            await host.websocket.send(text_command)
            logger.info(f"📡 Sent modem_decoder update: {send_value} to host {host.ip_address} for SDR #{local_index}")
        except Exception as e:
            logger.error(f"❌ Failed to send modem_decoder update to host: {e}")

    async def _update_decoder_for_modulation_change(self, sdr, host, modulation: str):
        """Automatically update decoder when modulation changes"""
        try:
            new_decoder = self._get_decoder_for_modulation(modulation)
            old_decoder = sdr.modem_decoder
            
            # Update server-side decoder
            sdr.modem_decoder = new_decoder
            
            # Send update to host for config persistence
            await self._send_modem_decoder_update(host, sdr.local_index, new_decoder)
            
            logger.info(f"🔄 Auto-updated decoder for SDR #{sdr.global_index}: '{old_decoder}' → '{new_decoder}' (modulation: {modulation})")
            
        except Exception as e:
            logger.error(f"❌ Failed to auto-update decoder: {e}")

    async def udp_broadcast_service(self):
        """UDP broadcast service to announce server availability"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        message = f"ShackMate SDR-Server, {self.local_ip}, {self.ws_port}"
        
        try:
            while self.running:
                try:
                    # Broadcast to subnet
                    sock.sendto(message.encode(), ('<broadcast>', self.udp_port))
                    logger.debug(f"Broadcasting: {message}")
                    await asyncio.sleep(5)  # Broadcast every 5 seconds
                except Exception as e:
                    logger.error(f"UDP broadcast error: {e}")
                    await asyncio.sleep(1)
        finally:
            sock.close()

    def _assign_global_sdr_indices(self):
        """Reassign global indices to all SDRs across all hosts"""
        global_index = 1
        for host in self.hosts.values():
            for sdr in host.sdr_devices:
                sdr.global_index = global_index
                global_index += 1
        
        logger.info(f"📊 Reassigned global SDR indices. Total SDRs: {global_index - 1}")

    async def cleanup_stale_hosts(self):
        """Periodic cleanup of stale hosts and hosts with no SDRs"""
        stale_timeout = 60  # seconds
        empty_host_timeout = 30  # seconds - disconnect hosts with no SDRs after 30 seconds
        while self.running:
            try:
                current_time = datetime.now()
                stale_hosts = []
                empty_hosts = []
                
                for host_id, host in self.hosts.items():
                    time_since_heartbeat = (current_time - host.last_heartbeat).total_seconds()
                    time_since_connection = (current_time - host.connected_at).total_seconds()
                    
                    # Check for stale hosts (no heartbeat)
                    if time_since_heartbeat > stale_timeout:
                        stale_hosts.append((host_id, host, time_since_heartbeat))
                    # Check for empty hosts (no SDRs after timeout)
                    elif len(host.sdr_devices) == 0 and time_since_connection > empty_host_timeout:
                        empty_hosts.append((host_id, host, time_since_connection))
                
                # Remove stale hosts
                for host_id, host, stale_time in stale_hosts:
                    sdr_count = len(host.sdr_devices)
                    logger.warning(f"🕒 Removing stale host {host.ip_address} (no heartbeat for {stale_time:.1f}s, had {sdr_count} SDRs)")
                    
                    # Close the WebSocket connection if it's still open
                    try:
                        await host.websocket.close()
                    except Exception:
                        pass  # Connection might already be closed
                    
                    # Remove from hosts
                    del self.hosts[host_id]
                    
                    # Broadcast host removal to other clients
                    await self._broadcast_host_update("host_removed", host_id, removed_host_info={
                        "ip_address": host.ip_address,
                        "sdr_count": sdr_count
                    })
                
                # Remove empty hosts (no SDR devices reported and no recent activity)
                for host_id, host, empty_time in empty_hosts:
                    # Don't disconnect monitoring clients that are actively sending commands
                    time_since_last_heartbeat = (current_time - host.last_heartbeat).total_seconds()
                    
                    # If host is actively communicating (heartbeat within 60s), keep it even without SDRs
                    if time_since_last_heartbeat < 60:
                        continue  # Skip removal - this is an active monitoring client
                    
                    logger.info(f"🚫 Removing inactive empty host {host.ip_address} (no SDRs for {empty_time:.1f}s, no activity for {time_since_last_heartbeat:.1f}s)")
                    
                    # Close the WebSocket connection
                    try:
                        await host.websocket.close()
                    except Exception:
                        pass  # Connection might already be closed
                    
                    # Remove from hosts
                    del self.hosts[host_id]
                
                # Reassign indices if any hosts were removed
                if stale_hosts or empty_hosts:
                    self._assign_global_sdr_indices()
                    logger.info(f"🧹 Cleanup complete. Active hosts: {len(self.hosts)} (removed {len(stale_hosts)} stale, {len(empty_hosts)} empty)")
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"❌ Error in stale host cleanup: {e}")
                await asyncio.sleep(30)

    async def _broadcast_host_update(self, update_type: str, affected_host_id: str, removed_host_info: dict = None):
        """Broadcast host changes to all connected clients except the affected host"""
        if not self.hosts:
            return
            
        # Prepare the broadcast message
        if update_type == "host_added":
            if affected_host_id not in self.hosts:
                return
                
            affected_host = self.hosts[affected_host_id]
            broadcast_msg = {
                "type": "host_update",
                "update_type": "host_added",
                "host_info": {
                    "host_id": affected_host_id,
                    "ip_address": affected_host.ip_address,
                    "connected_at": affected_host.connected_at.isoformat(),
                    "sdr_count": len(affected_host.sdr_devices),
                    "sdrs": []
                }
            }
            
            # Add SDR info with filtering (only non-defaults)
            for sdr in affected_host.sdr_devices:
                sdr_info = {
                    "global_index": sdr.global_index,
                    "device_type": sdr.device_type,
                    "serial_number": sdr.serial_number,
                    "local_index": sdr.local_index,
                    "status": sdr.status
                }
                
                # Only include non-default values
                if sdr.frequency is not None:
                    sdr_info["frequency"] = sdr.frequency
                if sdr.modulation is not None:
                    sdr_info["modulation"] = sdr.modulation
                if sdr.sample_rate != 24000:
                    sdr_info["sample_rate"] = sdr.sample_rate
                # Add other non-default values as needed...
                
                broadcast_msg["host_info"]["sdrs"].append(sdr_info)
                
        elif update_type == "host_removed":
            broadcast_msg = {
                "type": "host_update", 
                "update_type": "host_removed",
                "host_id": affected_host_id,
                "removed_host_info": removed_host_info or {}
            }
        else:
            logger.warning(f"Unknown broadcast update type: {update_type}")
            return
        
        # Send to all connected hosts except the affected one
        broadcast_count = 0
        for host_id, host in self.hosts.items():
            if host_id != affected_host_id:
                try:
                    await host.websocket.send(json.dumps(broadcast_msg))
                    broadcast_count += 1
                except Exception as e:
                    logger.error(f"Failed to broadcast to {host.ip_address}: {e}")
        
        logger.info(f"📢 Broadcasted {update_type} for host {affected_host_id} to {broadcast_count} clients")

    async def _broadcast_to_control_points(self, message: dict):
        """Broadcast a message to all connected control points"""
        if not self.control_points:
            logger.info("📢 No control points connected for broadcast")
            return
            
        broadcast_count = 0
        for control_point_id, control_point in self.control_points.items():
            try:
                await control_point["websocket"].send(json.dumps(message))
                broadcast_count += 1
                logger.info(f"📡 Sent broadcast to control point {control_point_id}")
            except Exception as e:
                logger.error(f"Failed to broadcast to control point {control_point_id}: {e}")
        
        logger.info(f"📢 Broadcasted message to {broadcast_count} control points")

    async def handle_websocket_connection(self, websocket):
        """Handle incoming WebSocket connections from SDR hosts"""
        client_ip = websocket.remote_address[0]
        host_id = f"{client_ip}_{int(time.time())}"
        
        logger.info(f"🔗 New WebSocket connection from {client_ip}")
        
        try:
            # Send welcome message
            welcome_msg = {
                "type": "welcome",
                "message": "Connected to ShackMate SDR Server v2.1.0-comprehensive-table",
                "host_id": host_id,
                "timestamp": datetime.now().isoformat()
            }
            await websocket.send(json.dumps(welcome_msg))
            
            # Send information about all currently connected SDR hosts (for controllers)
            # This allows controllers to immediately see what SDRs are available
            if self.hosts:
                host_info_msg = {
                    "type": "host_inventory",
                    "message": "Currently connected SDR hosts",
                    "hosts": []
                }
                
                for existing_host_id, existing_host in self.hosts.items():
                    host_info = {
                        "host_id": existing_host_id,
                        "ip_address": existing_host.ip_address,
                        "connected_at": existing_host.connected_at.isoformat(),
                        "sdr_count": len(existing_host.sdr_devices),
                        "sdrs": []
                    }
                    
                    # Only include non-default values in inventory to reduce noise
                    for sdr in existing_host.sdr_devices:
                        sdr_info = {
                            "global_index": sdr.global_index,
                            "device_type": sdr.device_type,
                            "serial_number": sdr.serial_number,
                            "local_index": sdr.local_index,
                            "status": sdr.status
                        }
                        
                        # Only include non-default configuration values
                        if sdr.frequency is not None:
                            sdr_info["frequency"] = int(sdr.frequency)
                        if sdr.modulation is not None:
                            sdr_info["modulation"] = sdr.modulation
                        if sdr.sample_rate != 24000:  # Default is 24000
                            sdr_info["sample_rate"] = sdr.sample_rate
                        if sdr.bias_tee:  # Default is False
                            sdr_info["bias_tee"] = sdr.bias_tee
                        if sdr.agc != "auto":  # Default is "auto"
                            sdr_info["agc"] = sdr.agc
                        if sdr.squelch != 0:  # Default is 0
                            sdr_info["squelch"] = sdr.squelch
                        if sdr.ppm != 0:  # Default is 0
                            sdr_info["ppm"] = sdr.ppm
                        if sdr.dc_correction:  # Default is False
                            sdr_info["dc_correction"] = sdr.dc_correction
                        if sdr.edge_correction:  # Default is False
                            sdr_info["edge_correction"] = sdr.edge_correction
                        if sdr.deemphasis:  # Default is False
                            sdr_info["deemphasis"] = sdr.deemphasis
                        if sdr.direct_i:  # Default is False
                            sdr_info["direct_i"] = sdr.direct_i
                        if sdr.direct_q:  # Default is False
                            sdr_info["direct_q"] = sdr.direct_q
                        if sdr.offset_tuning:  # Default is False
                            sdr_info["offset_tuning"] = sdr.offset_tuning
                        if sdr.modem_decoder is not None:
                            sdr_info["modem_decoder"] = sdr.modem_decoder
                        if sdr.modem_lead_in is not None:
                            sdr_info["modem_lead_in"] = sdr.modem_lead_in
                        if sdr.modem_sample_rate != 48000:  # Default is 48000
                            sdr_info["modem_sample_rate"] = sdr.modem_sample_rate
                        if sdr.modem_trailing is not None:
                            sdr_info["modem_trailing"] = sdr.modem_trailing
                        if sdr.modem_audio_device is not None:
                            sdr_info["modem_audio_device"] = sdr.modem_audio_device
                        if sdr.modem_debug:  # Default is False
                            sdr_info["modem_debug"] = sdr.modem_debug
                            
                        host_info["sdrs"].append(sdr_info)
                    
                    host_info_msg["hosts"].append(host_info)
                
                await websocket.send(json.dumps(host_info_msg))
                logger.info(f"📋 Sent filtered inventory of {len(self.hosts)} hosts with {sum(len(h.sdr_devices) for h in self.hosts.values())} total SDRs to {client_ip}")
            
            # Initially treat as control point until proven to be SDR host
            control_point = {
                "ip_address": client_ip,
                "websocket": websocket,
                "connected_at": datetime.now(),
                "last_activity": datetime.now(),
                "client_id": host_id
            }
            self.control_points[host_id] = control_point
            
            # Handle messages from this connection
            async for message in websocket:
                await self._handle_connection_message(host_id, message)
                
                # Update activity timestamp
                if host_id in self.control_points:
                    self.control_points[host_id]["last_activity"] = datetime.now()
                elif host_id in self.hosts:
                    self.hosts[host_id].last_heartbeat = datetime.now()
                
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"🔌 WebSocket connection closed for {client_ip}")
        except Exception as e:
            logger.error(f"❌ WebSocket error for {client_ip}: {e}")
        finally:
            # Enhanced stale host cleanup
            if host_id in self.hosts:
                host = self.hosts[host_id]
                sdr_count = len(host.sdr_devices)
                connection_duration = datetime.now() - host.connected_at
                
                # Broadcast host removal before deleting
                await self._broadcast_host_update("host_removed", host_id, removed_host_info={
                    "ip_address": host.ip_address,
                    "sdr_count": sdr_count
                })
                
                # Remove the host
                del self.hosts[host_id]
                
                # Reassign global indices for remaining hosts
                self._assign_global_sdr_indices()
                
                logger.info(f"🧹 Cleaned up stale host {client_ip} (had {sdr_count} SDRs, connected for {connection_duration})")
                logger.info(f"📊 Remaining active hosts: {len(self.hosts)} with {sum(len(h.sdr_devices) for h in self.hosts.values())} total SDRs")

    async def _handle_connection_message(self, connection_id: str, message: str):
        """Handle messages from any WebSocket connection (SDR host or control point)"""
        try:
            print(f"DEBUG: _handle_connection_message called with: {message[:50]}")
            logger.info(f"DEBUG: _handle_connection_message called with: {message[:50]}")
            
            message_stripped = message.strip().lower()
            
            # Debug logging
            logger.info(f"🔍 _handle_connection_message called - connection_id: {connection_id}")
            logger.info(f"🔍 Message preview: {message[:100]}...")
            logger.info(f"🔍 Connection in hosts: {connection_id in self.hosts}")
            logger.info(f"🔍 Connection in control_points: {connection_id in self.control_points}")
            
            # Update activity timestamp first
            if connection_id in self.control_points:
                self.control_points[connection_id]["last_activity"] = datetime.now()
            elif connection_id in self.hosts:
                self.hosts[connection_id].last_heartbeat = datetime.now()
            
            # Handle plain text commands (for control points)
            if message_stripped == "list":
                await self._handle_list_command(connection_id)
                return
            elif message_stripped == "status":
                await self._handle_get_system_status(connection_id)
                return
            elif message_stripped in ["modem", "ft8", "ft8status"]:
                await self._handle_ft8_status_command(connection_id)
                return
            elif message_stripped in ["ft8real", "realft8", "liveonly", "ft8", "decodes", "live"]:
                await self._handle_ft8_real_command(connection_id)
                return
            elif message_stripped.startswith("help"):
                await self._handle_help_command(connection_id)
                return
            elif message_stripped.startswith("rename sdr"):
                await self._handle_rename_command(connection_id, message_stripped)
                return
            elif message_stripped.startswith("set "):
                await self._handle_set_command(connection_id, message.strip())
                return
            elif message_stripped.startswith("get "):
                await self._handle_get_command(connection_id, message.strip())
                return
            # Check for simplified variable commands (name, freq, mode, etc.)
            elif await self._handle_simplified_command(connection_id, message.strip()):
                return
            
            # Try to parse as JSON (for SDR hosts and JSON commands)
            data = json.loads(message)
            msg_type = data.get("type")
            
            # Handle FT8 decode messages - broadcast to all clients
            if msg_type == "ft8_decode":
                await self._handle_ft8_decode_message(connection_id, data)
                return
            
            # Handle SDR discovery - this promotes control point to SDR host
            if msg_type == "sdr_discovery":
                await self._promote_to_sdr_host(connection_id, data)
                return
            
            # Route other JSON messages to existing host handler
            logger.info(f"🔍 Routing JSON message - connection_id: {connection_id}")
            logger.info(f"🔍 Is in hosts: {connection_id in self.hosts}")
            logger.info(f"🔍 Is in control_points: {connection_id in self.control_points}")
            
            # Check if this is a JSON API command (should go to control point handler)
            command = data.get("command")
            if command in ["list", "get_sdr", "set_sdr"]:
                logger.info(f"🎯 JSON API command detected: {command} - routing to control point handler")
                await self._handle_control_point_message(connection_id, data)
                return
            
            if connection_id in self.hosts:
                logger.info(f"🔀 Routing to SDR host handler")
                await self._handle_sdr_host_message(connection_id, data)
            else:
                logger.info(f"🔀 Routing to control point handler")
                # JSON commands from control points (like host_inventory requests)
                await self._handle_control_point_message(connection_id, data)
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from {connection_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling message from {connection_id}: {e}")

    async def _send_to_connection(self, connection_id: str, message: str):
        """Send message to either SDR host or control point"""
        try:
            if connection_id in self.hosts:
                await self.hosts[connection_id].websocket.send(message)
            elif connection_id in self.control_points:
                await self.control_points[connection_id]["websocket"].send(message)
            else:
                logger.error(f"Connection {connection_id} not found in hosts or control points")
        except Exception as e:
            logger.error(f"Error sending message to {connection_id}: {e}")

    async def _promote_to_sdr_host(self, connection_id: str, sdr_discovery_data: dict):
        """Promote a control point to an SDR host when it reports SDR devices"""
        if connection_id not in self.control_points:
            return
            
        control_point = self.control_points[connection_id]
        
        # Create SDR host from control point
        host = SDRHost(
            ip_address=control_point["ip_address"],
            websocket=control_point["websocket"], 
            connected_at=control_point["connected_at"],
            last_heartbeat=datetime.now(),
            sdr_devices=[],
            host_id=connection_id
        )
        
        # Move from control_points to hosts
        del self.control_points[connection_id]
        self.hosts[connection_id] = host
        
        # Process the SDR discovery
        await self._handle_sdr_discovery(connection_id, sdr_discovery_data)

    async def _handle_control_point_message(self, connection_id: str, data: dict):
        """Handle JSON messages from control points"""
        msg_type = data.get("type")
        command = data.get("command")
        
        # Debug logging
        logger.info(f"🔍 Control point message - type: {msg_type}, command: {command}")
        logger.info(f"🔧 Debug: command type = {type(command)}, command value = '{command}'")
        logger.info(f"🔧 Debug: command == 'get_sdr' = {command == 'get_sdr'}")
        logger.info(f"🔧 Debug: command in ['get_sdr', 'set_sdr'] = {command in ['get_sdr', 'set_sdr']}")
        
        # Handle JSON API commands first (these don't have a type field)
        if command in ["get_sdr", "set_sdr"]:
            # Handle JSON API commands for web controllers
            logger.info(f"🎯 SUCCESS! Processing JSON API command: {command}")
            sdr_id = data.get("sdr_id")
            if not sdr_id:
                response = {"error": "sdr_id is required"}
                logger.info(f"🚨 ERROR: No sdr_id provided")
                await self._send_to_connection(connection_id, json.dumps(response))
                return
            
            # Find the SDR host that has this SDR
            target_sdr = None
            target_host = None
            
            for host in self.hosts.values():
                for sdr in host.sdr_devices:
                    if sdr.global_index == sdr_id:
                        target_sdr = sdr
                        target_host = host
                        break
                if target_sdr:
                    break
            
            if not target_sdr or not target_host:
                response = {"error": f"SDR #{sdr_id} not found"}
                await self._send_to_connection(connection_id, json.dumps(response))
                return
            
            # Forward the JSON API command to the SDR host
            forward_message = {
                "type": "json_api_command",
                "command": command,
                "sdr_id": sdr_id,
                "local_index": target_sdr.local_index,
                "data": data,
                "response_to": connection_id  # So host knows where to send response
            }
            
            logger.info(f"📊 Forwarding JSON API command to host {target_host.ip_address}")
            await target_host.websocket.send(json.dumps(forward_message))
            logger.info(f"✅ JSON API command forwarded successfully")
            return  # IMPORTANT: return here to prevent falling through
        elif command == "list":
            # Handle JSON list command
            logger.info(f"🎯 SUCCESS! Processing JSON API command: list")
            await self._handle_list_command(connection_id)
            return  # IMPORTANT: return here to prevent falling through
        elif msg_type == "host_inventory":
            await self._handle_host_inventory_request(connection_id, data)
        elif msg_type == "get_system_status":
            await self._handle_get_system_status(connection_id)
        elif msg_type == "control_sdr":
            await self._handle_sdr_control_command(connection_id, data)
        elif msg_type == "system_status_broadcast":
            # Handle system status broadcasts - these should be sent to all control points
            await self._broadcast_to_control_points(data)
        else:
            logger.warning(f"Unknown message type from control point {connection_id}: {msg_type}, command: {command}")

    async def _handle_sdr_host_message(self, host_id: str, data: dict):
        """Handle JSON messages from SDR hosts"""
        msg_type = data.get("type")
        
        if msg_type == "sdr_status_update":
            await self._handle_sdr_status_update(host_id, data)
        elif msg_type == "heartbeat":
            await self._handle_heartbeat(host_id, data)
        elif msg_type == "control_sdr":
            await self._handle_sdr_control_command(host_id, data)
        elif msg_type == "json_api_response":
            await self._handle_json_api_response(host_id, data)
        else:
            logger.warning(f"Unknown message type from SDR host {host_id}: {msg_type}")

    async def _handle_host_message(self, host_id: str, message: str):
        """Handle messages from SDR hosts and controllers - supports both JSON and plain text"""
        try:
            # Check if it's a plain text command first
            message_stripped = message.strip().lower()
            
            # Update heartbeat for any valid message
            if host_id in self.hosts:
                self.hosts[host_id].last_heartbeat = datetime.now()
            
            # Handle plain text commands
            if message_stripped == "list":
                await self._handle_list_command(host_id)
                return
            elif message_stripped == "status":
                await self._handle_get_system_status(host_id)
                return
            elif message_stripped.startswith("help"):
                await self._handle_help_command(host_id)
                return
            elif message_stripped.startswith("rename sdr"):
                await self._handle_rename_command(host_id, message_stripped)
                return
            elif message_stripped.startswith("set "):
                await self._handle_set_command(host_id, message_stripped)
                return
            elif message_stripped.startswith("get "):
                await self._handle_get_command(host_id, message_stripped)
                return
            # Check for simplified variable commands (name, freq, mode, etc.)
            elif await self._handle_simplified_command(host_id, message.strip()):
                return
            
            # If not a plain text command, try to parse as JSON
            data = json.loads(message)
            msg_type = data.get("type")
            
            if host_id not in self.hosts:
                return
            
            host = self.hosts[host_id]
            host.last_heartbeat = datetime.now()
            
            if msg_type == "sdr_discovery":
                await self._handle_sdr_discovery(host_id, data)
            elif msg_type == "sdr_status_update":
                await self._handle_sdr_status_update(host_id, data)
            elif msg_type == "heartbeat":
                await self._handle_heartbeat(host_id, data)
            elif msg_type == "control_sdr":
                await self._handle_sdr_control_command(host_id, data)
            elif msg_type == "get_system_status":
                await self._handle_get_system_status(host_id)
            elif msg_type == "host_inventory":
                await self._handle_host_inventory_request(host_id, data)
            elif msg_type == "list":
                await self._handle_list_command(host_id)
            else:
                logger.warning(f"Unknown message type from {host_id}: {msg_type}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from {host_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling message from {host_id}: {e}")
    
    async def _handle_sdr_control_command(self, host_id: str, data: dict):
        """Handle SDR control commands from controllers"""
        global_index = data.get("global_index")
        sdr_command = data.get("sdr_command", {})
        
        if global_index is None:
            response = {"error": "global_index is required"}
        else:
            response = await self.send_sdr_command(global_index, sdr_command)
        
        # Send response back to controller
        try:
            host = self.hosts[host_id]
            await host.websocket.send(json.dumps(response))
        except Exception as e:
            logger.error(f"Failed to send response to {host_id}: {e}")
    
    async def _handle_get_system_status(self, connection_id: str):
        """Handle system status request from controllers"""
        try:
            # Check if this is a control point (should get text response) or SDR host (should get JSON)
            if connection_id in self.control_points:
                # Send readable text status for control points
                await self._send_text_system_status(connection_id)
            elif connection_id in self.hosts:
                # Send JSON status for SDR hosts
                status = self.get_system_status()
                host = self.hosts[connection_id]
                await host.websocket.send(json.dumps(status))
            else:
                logger.error(f"Connection {connection_id} not found in hosts or control points")
        except Exception as e:
            logger.error(f"Failed to send system status to {connection_id}: {e}")

    async def _send_text_system_status(self, connection_id: str):
        """Send readable text system status to control point"""
        try:
            # Create a readable status summary
            status_lines = []
            status_lines.append("=== SDR SYSTEM STATUS ===")
            status_lines.append("")
            
            # Basic system info
            status_lines.append(f"📊 SYSTEM OVERVIEW:")
            status_lines.append(f"   Server: ShackMate SDR Server v2.1.0")
            status_lines.append(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            status_lines.append(f"   Active Hosts: {len(self.hosts)}")
            status_lines.append(f"   Total SDRs: {sum(len(h.sdr_devices) for h in self.hosts.values())}")
            status_lines.append(f"   Control Points: {len(self.control_points)}")
            status_lines.append("")
            
            # SDR hosts summary
            if self.hosts:
                status_lines.append("🖥️  SDR HOSTS:")
                for host_id, host in self.hosts.items():
                    uptime = datetime.now() - host.connected_at
                    status_lines.append(f"   • {host.ip_address} ({len(host.sdr_devices)} SDRs) - Up: {str(uptime).split('.')[0]}")
                    for sdr in host.sdr_devices:
                        freq_str = f"{sdr.frequency/1e6:.3f} MHz" if sdr.frequency else "No freq"
                        status_lines.append(f"     └─ SDR #{sdr.global_index}: {sdr.status} - {freq_str} ({sdr.modulation or 'No mode'})")
            else:
                status_lines.append("🖥️  SDR HOSTS: No hosts connected")
            
            status_lines.append("")
            status_lines.append("💡 Type 'help' for available commands")
            
            # Send as plain text
            response_text = "\n".join(status_lines)
            control_point = self.control_points[connection_id]
            await control_point["websocket"].send(response_text)
            
        except Exception as e:
            logger.error(f"Failed to send text status to {connection_id}: {e}")

    async def _handle_ft8_status_command(self, connection_id: str):
        """Handle FT8/modem status request - send a readable text response"""
        try:
            control_point = self.control_points[connection_id]
            
            # Send a simple readable FT8 status message
            status_text = """=== FT8 MODEM STATUS ===

📻 To get detailed FT8 modem status:
   • The system automatically broadcasts FT8 decoder status
   • Status includes: RTL-SDR, audio pipeline, FT8 decoders, WebSocket services
   • Live decodes will appear automatically when signals are received

🔧 Available commands:
   • 'status' - Overall SDR system status
   • 'list' - List all SDRs  
   • 'help' - Show all commands

💡 FT8 status broadcasts are sent automatically from the iMac decoder system."""
            
            await control_point["websocket"].send(status_text)
            
        except Exception as e:
            logger.error(f"Failed to handle FT8 status command: {e}")

    async def _handle_ft8_real_command(self, connection_id: str):
        """Handle FT8 real decodes request - monitor actual processes and logs only"""
        try:
            control_point = self.control_points[connection_id]
            
            # Send immediate response
            await control_point["websocket"].send("🔄 Starting REAL FT8 monitoring (no simulated data)...")
            
            # Start the real FT8 monitor script
            import subprocess
            import asyncio
            from pathlib import Path
            
            async def start_real_monitor():
                try:
                    # Check if we're on the iMac
                    ft8_script_path = "/opt/ShackMate/ft8modem/ft8_real_monitor.py"
                    local_script_path = "./ft8_real_monitor.py"
                    
                    if Path(ft8_script_path).exists():
                        # Run from iMac location (we're on the iMac)
                        process = await asyncio.create_subprocess_exec(
                            'python3', ft8_script_path,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            cwd="/opt/ShackMate/ft8modem"
                        )
                        await control_point["websocket"].send("✅ Running real FT8 monitor on iMac...")
                    elif Path(local_script_path).exists():
                        # Try to SSH to iMac for real monitoring
                        await control_point["websocket"].send("🔗 Connecting to iMac for real FT8 monitoring...")
                        process = await asyncio.create_subprocess_exec(
                            'ssh', 'imacpro@imac-remote',
                            'cd /opt/ShackMate/ft8modem && python3 ft8_real_monitor.py',
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                        )
                    else:
                        # Fallback message
                        await control_point["websocket"].send("⚠️  Real monitor not available - ensure FT8 decoder processes are running on iMac")
                        return
                    
                    # Don't wait for completion - let it run in background
                    await control_point["websocket"].send("✅ Real FT8 monitoring started! Will show only live decodes from actual processes.")
                    
                    # Note: Process runs independently, monitoring real log files
                        
                except Exception as e:
                    await control_point["websocket"].send(f"❌ Error starting real monitor: {e}")
            
            # Start the background task
            asyncio.create_task(start_real_monitor())
            
        except Exception as e:
            logger.error(f"Failed to handle FT8 real command: {e}")

    async def _handle_ft8_decode_message(self, connection_id: str, ft8_data: dict):
        """Handle incoming FT8 decode message and broadcast to all connected clients"""
        try:
            # Validate FT8 data structure
            required_fields = ["sdr_number", "frequency", "message"]
            for field in required_fields:
                if field not in ft8_data:
                    logger.error(f"Missing required field '{field}' in FT8 decode message")
                    return
            
            # Add timestamp if not present
            if "timestamp" not in ft8_data:
                ft8_data["timestamp"] = int(datetime.now().timestamp())
            
            # Log the FT8 decode
            logger.info(f"📻 FT8 Decode: {ft8_data['sdr_number']} {ft8_data['frequency']} Hz - {ft8_data['message']}")
            
            # Prepare broadcast message
            broadcast_msg = {
                "type": "ft8_decode_broadcast",
                "source_connection": connection_id,
                "ft8_data": ft8_data,
                "server_timestamp": datetime.now().isoformat()
            }
            
            # Broadcast to all connected clients (hosts and control points)
            broadcast_count = 0
            
            # Send to all SDR hosts
            for host_id, host in self.hosts.items():
                if host_id != connection_id:  # Don't echo back to sender
                    try:
                        await host.websocket.send(json.dumps(broadcast_msg))
                        broadcast_count += 1
                    except Exception as e:
                        logger.error(f"Failed to send FT8 decode to host {host_id}: {e}")
            
            # Send to all control points
            for cp_id, control_point in self.control_points.items():
                if cp_id != connection_id:  # Don't echo back to sender
                    try:
                        await control_point["websocket"].send(json.dumps(broadcast_msg))
                        broadcast_count += 1
                    except Exception as e:
                        logger.error(f"Failed to send FT8 decode to control point {cp_id}: {e}")
            
            logger.info(f"📡 Broadcasted FT8 decode from {connection_id} to {broadcast_count} clients")
            
        except Exception as e:
            logger.error(f"Failed to handle FT8 decode message: {e}")

    def _format_ft8_status_text(self, status_data: dict) -> str:
        """Format FT8 status data as readable text"""
        lines = []
        lines.append("=== FT8 MODEM DECODER STATUS ===")
        lines.append("")
        
        # System info
        if "system_info" in status_data:
            sys_info = status_data["system_info"]
            lines.append(f"🖥️  System: {sys_info.get('host', 'Unknown')} ({sys_info.get('ip', 'Unknown IP')})")
            lines.append("")
        
        # SDR processes
        if "sdr_processes" in status_data:
            rtl = status_data["sdr_processes"].get("rtl_fm", {})
            lines.append(f"📡 RTL-SDR: {rtl.get('status', 'Unknown')} ({rtl.get('count', 0)} processes)")
        
        # Audio pipeline  
        if "audio_pipeline" in status_data:
            af2udp = status_data["audio_pipeline"].get("af2udp", {})
            lines.append(f"🔊 Audio Pipeline: {af2udp.get('status', 'Unknown')} ({af2udp.get('count', 0)} processes)")
        
        # Modem decoders
        if "modem_decoders" in status_data:
            ft8modem = status_data["modem_decoders"].get("ft8modem", {})
            lines.append(f"📻 FT8 Decoder: {ft8modem.get('status', 'Unknown')} ({ft8modem.get('count', 0)} processes)")
        
        # WebSocket services
        if "websocket_services" in status_data:
            ws = status_data["websocket_services"].get("websocketd", {})
            bridge = status_data["websocket_services"].get("ft8_bridge", {})
            lines.append(f"🌐 WebSocket: {ws.get('status', 'Unknown')} ({ws.get('count', 0)} processes)")
            lines.append(f"🔗 FT8 Bridge: {bridge.get('status', 'Unknown')} ({bridge.get('count', 0)} processes)")
        
        lines.append("")
        lines.append("💡 All processes running = FT8 decoder is operational")
        
        return "\n".join(lines)

    async def _handle_sdr_discovery(self, host_id: str, data: dict):
        """Handle SDR discovery from a host"""
        host = self.hosts[host_id]
        sdr_list = data.get("sdrs", [])
        
        # Clear existing SDRs for this host
        host.sdr_devices = []
        
        # Add new SDRs, preserving client-provided values without adding defaults
        for i, sdr_info in enumerate(sdr_list):
            sdr = SDRDevice(
                serial_number=sdr_info.get("serial_number", f"unknown_{i}"),
                device_type=sdr_info.get("device_type", "unknown"),
                host_ip=host.ip_address,
                local_index=i + 1,
                global_index=0,  # Will be assigned by _assign_global_sdr_indices
                status=sdr_info.get("status", "detected"),
                sdr_name=sdr_info.get("sdr_name")  # Only set if provided
            )
            
            # Only set configuration values if they were provided by the client
            # This respects the client's filtering decisions
            if "frequency" in sdr_info:
                sdr.frequency = sdr_info["frequency"]
            if "sdr_frequency" in sdr_info:  # Also check for sdr_frequency format
                sdr.frequency = sdr_info["sdr_frequency"]
            if "modulation" in sdr_info:
                sdr.modulation = sdr_info["modulation"]
            if "sdr_modulation" in sdr_info:  # Also check for sdr_modulation format
                sdr.modulation = sdr_info["sdr_modulation"]
            if "sample_rate" in sdr_info:
                sdr.sample_rate = sdr_info["sample_rate"]
            if "bias_tee" in sdr_info:
                sdr.bias_tee = sdr_info["bias_tee"]
            if "agc" in sdr_info:
                sdr.agc = sdr_info["agc"]
            if "squelch" in sdr_info:
                # Convert null/None squelch values to 0 default
                squelch_value = sdr_info["squelch"]
                if squelch_value is None:
                    sdr.squelch = 0
                else:
                    sdr.squelch = squelch_value
            if "ppm" in sdr_info:
                sdr.ppm = sdr_info["ppm"]
            if "dc_correction" in sdr_info:
                sdr.dc_correction = sdr_info["dc_correction"]
            if "edge_correction" in sdr_info:
                sdr.edge_correction = sdr_info["edge_correction"]
            if "deemphasis" in sdr_info:
                sdr.deemphasis = sdr_info["deemphasis"]
            if "direct_i" in sdr_info:
                sdr.direct_i = sdr_info["direct_i"]
            if "direct_q" in sdr_info:
                sdr.direct_q = sdr_info["direct_q"]
            if "offset_tuning" in sdr_info:
                sdr.offset_tuning = sdr_info["offset_tuning"]
            # Set modem_decoder to match SDR's modulation automatically
            if sdr.modulation:
                sdr.modem_decoder = sdr.modulation.lower()  # Store lowercase for consistency
                # Send lowercase version to host for consistent storage
                await self._send_modem_decoder_update(host, sdr.local_index, sdr.modulation.lower())
            elif "modem_decoder" in sdr_info:
                sdr.modem_decoder = sdr_info["modem_decoder"]
            if "modem_lead_in" in sdr_info:
                sdr.modem_lead_in = sdr_info["modem_lead_in"]
            if "modem_sample_rate" in sdr_info:
                sdr.modem_sample_rate = sdr_info["modem_sample_rate"]
            if "modem_trailing" in sdr_info:
                sdr.modem_trailing = sdr_info["modem_trailing"]
            if "modem_audio_device" in sdr_info:
                sdr.modem_audio_device = sdr_info["modem_audio_device"]
            if "modem_debug" in sdr_info:
                sdr.modem_debug = sdr_info["modem_debug"]
            
            # Set converter fields if provided
            if "converter" in sdr_info:
                sdr.converter = sdr_info["converter"]
            if "converter_type" in sdr_info:
                sdr.converter_type = sdr_info["converter_type"]
            if "converter_offset" in sdr_info:
                sdr.converter_offset = sdr_info["converter_offset"]
            if "converter_description" in sdr_info:
                sdr.converter_description = sdr_info["converter_description"]
            
            host.sdr_devices.append(sdr)
        
        # Reassign global indices
        self._assign_global_sdr_indices()
        
        logger.info(f"🔍 Host {host.ip_address} reported {len(sdr_list)} SDR devices with filtered config data")
        
        # Send acknowledgment to the connecting host
        response = {
            "type": "sdr_discovery_ack",
            "message": f"Registered {len(sdr_list)} SDR devices",
            "global_indices": [sdr.global_index for sdr in host.sdr_devices]
        }
        await host.websocket.send(json.dumps(response))
        
        # 🚀 BROADCAST NEW HOST INFO TO ALL OTHER CONNECTED CLIENTS
        await self._broadcast_host_update("host_added", host_id)

    async def _handle_sdr_status_update(self, host_id: str, data: dict):
        """Handle SDR status updates from a host"""
        host = self.hosts[host_id]
        local_index = data.get("local_index")
        status_data = data.get("status", {})
        
        # Find the SDR by local index
        for sdr in host.sdr_devices:
            if sdr.local_index == local_index:
                # Update SDR status - only update fields that are provided
                # This respects the client's filtering decisions
                if "status" in status_data:
                    sdr.status = status_data["status"]
                if "frequency" in status_data:
                    sdr.frequency = status_data["frequency"]
                if "sample_rate" in status_data:
                    sdr.sample_rate = status_data["sample_rate"]
                if "modulation" in status_data:
                    sdr.modulation = status_data["modulation"]
                if "bias_tee" in status_data:
                    sdr.bias_tee = status_data["bias_tee"]
                if "agc" in status_data:
                    sdr.agc = status_data["agc"]
                if "squelch" in status_data:
                    sdr.squelch = status_data["squelch"]
                if "ppm" in status_data:
                    sdr.ppm = status_data["ppm"]
                if "dc_correction" in status_data:
                    sdr.dc_correction = status_data["dc_correction"]
                if "edge_correction" in status_data:
                    sdr.edge_correction = status_data["edge_correction"]
                if "deemphasis" in status_data:
                    sdr.deemphasis = status_data["deemphasis"]
                if "direct_i" in status_data:
                    sdr.direct_i = status_data["direct_i"]
                if "direct_q" in status_data:
                    sdr.direct_q = status_data["direct_q"]
                if "offset_tuning" in status_data:
                    sdr.offset_tuning = status_data["offset_tuning"]
                if "modem_decoder" in status_data:
                    sdr.modem_decoder = status_data["modem_decoder"]
                if "modem_lead_in" in status_data:
                    sdr.modem_lead_in = status_data["modem_lead_in"]
                if "modem_sample_rate" in status_data:
                    sdr.modem_sample_rate = status_data["modem_sample_rate"]
                if "modem_trailing" in status_data:
                    sdr.modem_trailing = status_data["modem_trailing"]
                if "modem_audio_device" in status_data:
                    sdr.modem_audio_device = status_data["modem_audio_device"]
                if "modem_debug" in status_data:
                    sdr.modem_debug = status_data["modem_debug"]
                break
        
        logger.debug(f"📊 Updated SDR status for host {host.ip_address}, local index {local_index} with filtered data")

    async def _handle_heartbeat(self, host_id: str, data: dict):
        """Handle heartbeat from a host"""
        host = self.hosts[host_id]
        host.last_heartbeat = datetime.now()
        
        # Send heartbeat response
        response = {
            "type": "heartbeat_ack",
            "timestamp": datetime.now().isoformat(),
            "active_sdrs": len(host.sdr_devices)
        }
        await host.websocket.send(json.dumps(response))

    async def _handle_json_api_response(self, host_id: str, data: dict):
        """Handle JSON API response from SDR host and forward to original client"""
        response_to = data.get("response_to")
        response = data.get("response")
        
        if not response_to:
            logger.error("JSON API response missing response_to field")
            return
        
        if not response:
            logger.error("JSON API response missing response field")
            return
        
        logger.info(f"📦 Forwarding JSON API response from host {host_id} to client {response_to}")
        
        # Send the response to the original client
        await self._send_to_connection(response_to, json.dumps(response))
        logger.info(f"✅ JSON API response forwarded successfully")

    async def _handle_host_inventory_request(self, host_id: str, data: dict):
        """Handle host inventory request from controllers"""
        # Send current SDR inventory to the requesting host
        inventory = {
            "type": "host_inventory_response",
            "hosts": []
        }
        
        for hid, host in self.hosts.items():
            if hid != host_id and host.sdr_devices:  # Don't include the requesting host
                host_info = {
                    "host_id": hid,
                    "ip": host.ip_address,
                    "last_heartbeat": host.last_heartbeat.isoformat(),
                    "sdr_devices": []
                }
                
                for sdr in host.sdr_devices:
                    sdr_info = {
                        "global_index": sdr.global_index,
                        "serial_number": sdr.serial_number,
                        "device_type": sdr.device_type,
                        "local_index": sdr.local_index,
                        "host_ip": sdr.host_ip,
                        "status": sdr.status
                    }
                    
                    # Only include sdr_name if it was set
                    if sdr.sdr_name is not None:
                        sdr_info["sdr_name"] = sdr.sdr_name
                    
                    # Only include non-default configuration values
                    configuration = {}
                    if sdr.frequency is not None:
                        configuration["sdr_frequency"] = int(sdr.frequency)  # Store as sdr_frequency integer
                    if sdr.modulation is not None:
                        configuration["sdr_modulation"] = sdr.modulation  # Store as sdr_modulation
                    if sdr.sample_rate != 24000:  # Default is 24000
                        configuration["sample_rate"] = sdr.sample_rate
                    if sdr.bias_tee:  # Default is False
                        configuration["bias_tee"] = sdr.bias_tee
                    if sdr.agc != "auto":  # Default is "auto"
                        configuration["agc"] = sdr.agc
                    if sdr.squelch != 0:  # Default is 0
                        configuration["squelch"] = sdr.squelch
                    if sdr.ppm != 0:  # Default is 0
                        configuration["ppm"] = sdr.ppm
                    if sdr.dc_correction:  # Default is False
                        configuration["dc_correction"] = sdr.dc_correction
                    if sdr.edge_correction:  # Default is False
                        configuration["edge_correction"] = sdr.edge_correction
                    if sdr.deemphasis:  # Default is False
                        configuration["deemphasis"] = sdr.deemphasis
                    if sdr.direct_i:  # Default is False
                        configuration["direct_i"] = sdr.direct_i
                    if sdr.direct_q:  # Default is False
                        configuration["direct_q"] = sdr.direct_q
                    if sdr.offset_tuning:  # Default is False
                        configuration["offset_tuning"] = sdr.offset_tuning
                    if sdr.modem_decoder is not None:
                        configuration["modem_decoder"] = sdr.modem_decoder
                    if sdr.modem_lead_in is not None:
                        configuration["modem_lead_in"] = sdr.modem_lead_in
                    if sdr.modem_sample_rate != 48000:  # Default is 48000
                        configuration["modem_sample_rate"] = sdr.modem_sample_rate
                    if sdr.modem_trailing is not None:
                        configuration["modem_trailing"] = sdr.modem_trailing
                    if sdr.modem_audio_device is not None:
                        configuration["modem_audio_device"] = sdr.modem_audio_device
                    if sdr.modem_debug:  # Default is False
                        configuration["modem_debug"] = sdr.modem_debug
                    
                    # Only include configuration object if it has content
                    if configuration:
                        sdr_info["configuration"] = configuration
                    
                    host_info["sdr_devices"].append(sdr_info)
                
                inventory["hosts"].append(host_info)
        
        # Send inventory to requesting host
        requesting_host = self.hosts[host_id]
        await requesting_host.websocket.send(json.dumps(inventory))
        logger.info(f"📋 Sent filtered host inventory to {host_id}: {len(inventory['hosts'])} hosts with SDRs")

    def _get_ham_band(self, frequency_hz):
        """Convert frequency in Hz to ham radio band designation"""
        if not frequency_hz or not isinstance(frequency_hz, (int, float)) or frequency_hz <= 0:
            return "---"
        
        freq_mhz = frequency_hz / 1000000
        
        # Ham radio band lookup table (ITU Region 2 - Americas)
        ham_bands = [
            (1.8, 2.0, "160m"),      # 160 meters
            (3.5, 4.0, "80m"),       # 80 meters  
            (5.3, 5.4, "60m"),       # 60 meters
            (7.0, 7.3, "40m"),       # 40 meters
            (10.1, 10.15, "30m"),    # 30 meters
            (14.0, 14.35, "20m"),    # 20 meters
            (18.068, 18.168, "17m"), # 17 meters
            (21.0, 21.45, "15m"),    # 15 meters
            (24.89, 24.99, "12m"),   # 12 meters
            (28.0, 29.7, "10m"),     # 10 meters
            (50.0, 54.0, "6m"),      # 6 meters
            (144.0, 148.0, "2m"),    # 2 meters
            (222.0, 225.0, "1.25m"), # 1.25 meters
            (420.0, 450.0, "70cm"),  # 70 centimeters
            (902.0, 928.0, "33cm"),  # 33 centimeters
            (1240.0, 1300.0, "23cm"), # 23 centimeters
            (2300.0, 2450.0, "13cm"), # 13 centimeters
            (3300.0, 3500.0, "9cm"),  # 9 centimeters
            (5650.0, 5925.0, "6cm"),  # 6 centimeters
            (10000.0, 10500.0, "3cm"), # 3 centimeters
        ]
        
        # Check each ham band
        for min_freq, max_freq, band_name in ham_bands:
            if min_freq <= freq_mhz <= max_freq:
                return band_name
        
        # Non-ham bands - general categories
        if freq_mhz < 1.8:
            if freq_mhz < 0.3:
                return "LF"    # Low Frequency
            else:
                return "MF"    # Medium Frequency
        elif 1.8 <= freq_mhz < 30:
            return "HF"        # High Frequency (non-ham)
        elif 30 <= freq_mhz < 300:
            return "VHF"       # Very High Frequency (non-ham)
        elif 300 <= freq_mhz < 3000:
            return "UHF"       # Ultra High Frequency (non-ham)
        elif 3000 <= freq_mhz < 30000:
            return "SHF"       # Super High Frequency
        else:
            return "EHF"       # Extremely High Frequency

    async def _handle_list_command(self, host_id: str):
        """Handle list command - provides tabular host and SDR listing"""
        try:
            # Build table format list
            output_lines = []
            
            # Filter out hosts with no SDR devices and sort by IP address
            hosts_with_sdrs = [host for host in self.hosts.values() if len(host.sdr_devices) > 0]
            sorted_hosts = sorted(hosts_with_sdrs, key=lambda h: h.ip_address)
            
            # Collect all SDRs from all hosts and sort by global index
            all_sdrs = []
            for host in hosts_with_sdrs:
                all_sdrs.extend(host.sdr_devices)
            all_sdrs.sort(key=lambda s: s.global_index)
            
            if not sorted_hosts:
                output_lines.append("No hosts with SDR devices found")
            else:
                # HOSTS section
                output_lines.append("HOSTS:")
                for i, host in enumerate(sorted_hosts, 1):
                    # Determine SDR range for this host
                    sdr_indices = [sdr.global_index for sdr in host.sdr_devices]
                    min_sdr = min(sdr_indices)
                    max_sdr = max(sdr_indices)
                    sdr_range = f"[{min_sdr}-{max_sdr}]" if min_sdr != max_sdr else f"[{min_sdr}]"
                    
                    output_lines.append(f"Host #{i}: {host.ip_address} / SDR's {sdr_range} - Connected: {host.connected_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                
                output_lines.append("")
                
                # SDRS Table
                output_lines.append("SDRS:")
                
                # Table header  
                output_lines.append("┌──────┬──────────────────────┬──────────┬───────────┬──────┬──────┬──────┬──────┬─────┬───────────┬───────┬────────┬─────────┬──────────┐")
                output_lines.append("│ SDR# │         Name         │  Serial  │   Freq    │ Mode │ Band │ Gain │ AGC  │ PPM │ Converter │ BiasT │ Sample │ Squelch │  Status  │")
                output_lines.append("├──────┼──────────────────────┼──────────┼───────────┼──────┼──────┼──────┼──────┼─────┼───────────┼───────┼────────┼─────────┼──────────┤")
                
                # Loop through the sorted SDRs for display
                
                for sdr in all_sdrs:
                    # Get all SDR attributes with defaults and safe string handling
                    sdr_name = getattr(sdr, 'sdr_name', None)
                    device_type = getattr(sdr, 'device_type', 'Unknown')
                    display_name = (sdr_name if sdr_name else device_type)
                    display_name = str(display_name)[:20] if display_name else "Unknown"
                    
                    serial = getattr(sdr, 'serial_number', 'Unknown')
                    serial = str(serial)[:10] if serial else "Unknown"
                    
                    # Frequency
                    freq_str = "---"
                    frequency = getattr(sdr, 'frequency', None)
                    
                    # Debug logging for frequency
                    logger.info(f"🔍 DEBUG Display: SDR #{sdr.global_index} frequency = {frequency}, type = {type(frequency)}")
                    
                    if frequency and isinstance(frequency, (int, float)) and frequency > 0:
                        freq_mhz = frequency / 1000000
                        freq_str = f"{freq_mhz:.3f}"
                        logger.info(f"🔍 DEBUG Display: Converted {frequency} Hz → {freq_mhz:.3f} MHz → '{freq_str}'")
                    else:
                        logger.info(f"🔍 DEBUG Display: No valid frequency for SDR #{sdr.global_index}")
                    
                    # Mode/Modulation
                    modulation = getattr(sdr, 'modulation', 'None')
                    mode = str(modulation).upper()[:6] if modulation else "NONE"
                    
                    # Band (calculate from frequency using ham radio lookup)
                    band = self._get_ham_band(frequency)
                    
                    # Gain - check both 'gain' and 'sdr_gain' attributes
                    gain_val = getattr(sdr, 'sdr_gain', None) or getattr(sdr, 'gain', None)
                    if gain_val is not None and isinstance(gain_val, (int, float)):
                        gain = f"{gain_val:.1f}"
                    else:
                        gain = str(gain_val)[:6] if gain_val else "---"
                    
                    # AGC
                    agc_val = getattr(sdr, 'agc', None)
                    if agc_val and str(agc_val).lower() in ['automatic', 'auto']:
                        agc = "Auto"
                    else:
                        agc = str(agc_val)[:5] if agc_val else "---"
                    
                    # PPM
                    ppm_val = getattr(sdr, 'ppm', None)
                    if ppm_val is not None and isinstance(ppm_val, (int, float)):
                        ppm = f"{ppm_val:+.0f}"
                    else:
                        ppm = str(ppm_val)[:4] if ppm_val else "---"
                    
                    # Converted (upconverter status only - NOT bias_tee)
                    converted = "No"
                    
                    # Check for new converter system (converter and converter_type - the actual field names sent by host)
                    sdr_converter = getattr(sdr, 'converter', None)
                    sdr_converter_type = getattr(sdr, 'converter_type', None)
                    
                    # Debug logging for converter detection
                    serial = getattr(sdr, 'serial_number', 'Unknown')
                    logger.info(f"🔍 DEBUG Converter: SDR {serial} - converter={sdr_converter}, converter_type='{sdr_converter_type}'")
                    
                    if sdr_converter == 1 or (sdr_converter_type and sdr_converter_type.lower() == 'upconverter'):
                        converted = "UpConv"
                        logger.info(f"✅ DEBUG Converter: SDR {serial} - Set to UpConv")
                    elif sdr_converter == 2 or (sdr_converter_type and sdr_converter_type.lower() == 'dnconverter'):
                        converted = "DnConv"
                        logger.info(f"✅ DEBUG Converter: SDR {serial} - Set to DnConv")
                    else:
                        # Fallback to old upconverter field for backward compatibility
                        upconverter = getattr(sdr, 'upconverter', None)
                        if upconverter and str(upconverter).lower() in ['true', 'on', '1', 'enabled', 'yes']:
                            converted = "UpConv"
                            logger.info(f"✅ DEBUG Converter: SDR {serial} - Set to UpConv (legacy)")
                        else:
                            converted = "No"
                            logger.info(f"❌ DEBUG Converter: SDR {serial} - Set to No (upconverter={upconverter})")
                    
                    # Additional columns for comprehensive 14-column table
                    # BiasT column - separate bias_tee status  
                    bias_tee_val = getattr(sdr, 'sdr_bias_tee', None) or getattr(sdr, 'bias_tee', None)
                    if bias_tee_val is True or str(bias_tee_val).lower() in ['true', 'on', '1', 'enabled', 'yes']:
                        bias_tee_str = "ON"
                    elif bias_tee_val is False or str(bias_tee_val).lower() in ['false', 'off', '0', 'disabled', 'no']:
                        bias_tee_str = "OFF"
                    else:
                        bias_tee_str = "---"
                    
                    # Sample Rate column
                    sample_rate = getattr(sdr, 'sample_rate', None)
                    if sample_rate and isinstance(sample_rate, (int, float)):
                        if sample_rate >= 1000000:
                            sample_str = f"{sample_rate/1000000:.1f}M"
                        elif sample_rate >= 1000:
                            sample_str = f"{sample_rate/1000:.0f}k"
                        else:
                            sample_str = f"{sample_rate:.0f}"
                    else:
                        sample_str = "---"
                    
                    # Squelch column - check both 'squelch' and 'sdr_squelch' attributes
                    squelch = getattr(sdr, 'sdr_squelch', None) or getattr(sdr, 'squelch', None)
                    if squelch is not None and str(squelch).strip() != '':
                        try:
                            squelch_num = float(squelch)
                            squelch_str = f"{squelch_num:.0f}"
                        except (ValueError, TypeError):
                            squelch_str = str(squelch)[:7] if squelch else "---"
                    else:
                        squelch_str = "---"
                    
                    # Status column
                    status = getattr(sdr, 'status', None)
                    if status and str(status).lower() == 'active':
                        status_str = "Active"
                    elif status and str(status).lower() == 'inactive':
                        status_str = "Inactive"
                    else:
                        status_str = "Unknown"
                    
                    # Format comprehensive 14-column table row with improved alignment
                    global_idx = getattr(sdr, 'global_index', 0)
                    output_lines.append(f"│ {global_idx:4d} │ {display_name:<20s} │ {serial:>8s} │ {freq_str:>9s} │ {mode:^4s} │ {band:^4s} │ {gain:>4s} │ {agc:^4s} │ {ppm:>3s} │ {converted:^9s} │ {bias_tee_str:^5s} │ {sample_str:>6s} │ {squelch_str:>7s} │ {status_str:^8s} │")
                
                # Table footer
                output_lines.append("└──────┴──────────────────────┴──────────┴───────────┴──────┴──────┴──────┴──────┴─────┴───────────┴───────┴────────┴─────────┴──────────┘")
            
            # Add Modem Settings Table for Digital Mode SDRs
            digital_sdrs = []
            for sdr in all_sdrs:
                modulation = getattr(sdr, 'modulation', None)
                if modulation and modulation.lower() in ['ft4', 'ft8', 'ads-b', 'aprs']:
                    digital_sdrs.append(sdr)
            
            if digital_sdrs:
                output_lines.append("")  # Empty line separator
                output_lines.append("🎵 MODEM SETTINGS (Digital Mode SDRs)")
                output_lines.append("┌──────┬──────────┬─────────────┬──────────────┬─────────────┬──────────────┬───────────┐")
                output_lines.append("│ SDR# │ Decoder  │  Lead-in    │ Audio Rate   │  Trailing   │    Source    │   Debug   │")
                output_lines.append("├──────┼──────────┼─────────────┼──────────────┼─────────────┼──────────────┼───────────┤")
                
                for sdr in digital_sdrs:
                    global_idx = getattr(sdr, 'global_index', 0)
                    
                    # Use SDR's modulation as decoder, with fallback to modem_decoder
                    decoder = sdr.modulation if sdr.modulation else getattr(sdr, 'modem_decoder', 'Not Set')
                    if decoder in [None, '', '(not set)']:
                        decoder = 'NOT SET'
                    else:
                        decoder = decoder.upper()
                    
                    lead_in = getattr(sdr, 'modem_lead_in', 0)
                    if lead_in in [None, '', '(not set)']:
                        lead_in = 0
                    lead_in_str = f"{lead_in}ms"
                    
                    sample_rate = getattr(sdr, 'modem_sample_rate', 48000)
                    if sample_rate in [None, '', '(not set)']:
                        sample_rate = 48000
                    sample_rate_str = f"{sample_rate}Hz"
                    
                    trailing = getattr(sdr, 'modem_trailing', 0)
                    if trailing in [None, '', '(not set)']:
                        trailing = 0
                    trailing_str = f"{trailing}ms"
                    
                    audio_device = getattr(sdr, 'modem_audio_device', 'UDP')
                    if audio_device in [None, '', '(not set)']:
                        audio_device = 'UDP'
                    else:
                        audio_device = audio_device.upper()
                    
                    debug = getattr(sdr, 'modem_debug', False)
                    debug_str = "YES" if debug else "NO"
                    
                    # Format modem table row
                    output_lines.append(f"│ {global_idx:4d} │ {decoder:^8s} │ {lead_in_str:^11s} │ {sample_rate_str:^12s} │ {trailing_str:^11s} │ {audio_device:^12s} │ {debug_str:^9s} │")
                
                # Modem table footer
                output_lines.append("└──────┴──────────┴─────────────┴──────────────┴─────────────┴──────────────┴───────────┘")
            
            # Send response as plain text (not JSON)
            plain_text_output = "\n".join(output_lines)
            
            await self._send_to_connection(host_id, plain_text_output)
            logger.info(f"📋 Sent tabular host listing to {host_id}")
            
        except Exception as e:
            # Send error as plain text too
            error_message = f"ERROR: Failed to generate host listing: {e}"
            try:
                await self._send_to_connection(host_id, error_message)
            except:
                pass
            logger.error(f"Error generating host listing for {host_id}: {e}")

    async def _handle_help_command(self, host_id: str):
        """Handle help command - shows comprehensive available commands with examples"""
        try:
            help_text = """=== SDR Server Command Reference ===

📊 SYSTEM STATUS COMMANDS:
┌─────────────────────────────┬──────────────────────────────────────────────────────────────────────────────┐
│ TEXT COMMAND                │ JSON COMMAND                                                                 │
├─────────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ list                        │ {"type": "host_inventory"}                                                   │
│ status                      │ {"type": "get_system_status"}                                                │
│ help                        │ {"type": "help"}                                                             │
└─────────────────────────────┴──────────────────────────────────────────────────────────────────────────────┘

💾 BASIC VARIABLE MAPPING (Text Command ↔ JSON Variable):
┌─────────────────────┬─────────────────────┬───────────────────────────────────────────────────┐
│ TEXT COMMAND        │ JSON VARIABLE       │ DESCRIPTION                                       │
├─────────────────────┼─────────────────────┼───────────────────────────────────────────────────┤
│ name                │ sdr_name            │ Human-readable name for the SDR                   │
│ freq                │ sdr_frequency       │ Center frequency in Hz (e.g., 145500000)          │
│ mode                │ sdr_modulation      │ am, fm, usb, lsb, cw, digital                     │
│ band                │ sdr_band            │ Amateur radio band (2m, 70cm, 40m, etc.)          │
│ gain                │ sdr_gain            │ RF gain (0-50, or "auto")                         │
│ agc                 │ sdr_agc             │ AGC mode: auto, manual, fast, slow                │
│ ppm                 │ sdr_ppm             │ PPM frequency correction (-100 to 100)            │
│ biast               │ sdr_bias_tee        │ Bias tee control (true/false)                     │
│ converter           │ sdr_bias_tee        │ Upconverter/bias tee control (alias)              │
│ sample              │ sdr_sample_rate     │ Sample rate in Hz (e.g., 24000)                   │
│ squelch             │ sdr_squelch         │ Squelch level (0-100)                             │
│ status              │ sdr_status          │ Current status: offline, online, streaming        │
│ type                │ sdr_device_type     │ RTL-SDR device type                               │
│ serial              │ sdr_serial_number   │ Device serial number                              │
│ all                 │ all                 │ Get all variables at once                         │
│ leadin              │ sdr_modem_lead_in   │ Modem lead-in time in milliseconds (default: 0)   │
│ trailing            │ sdr_modem_trailing  │ Modem trailing time in milliseconds (default: 0)  │
│ audio               │ sdr_modem_sample_rate│ Modem audio rate in Hz (default: 48000)          │
│ source              │ sdr_modem_audio_device│ Modem audio source (default: UDP)               │
│ debug               │ sdr_modem_debug     │ Modem debug mode (on/off, default: off)           │
└─────────────────────┴─────────────────────┴───────────────────────────────────────────────────┘

🔧 ADVANCED VARIABLES (JSON Only):
┌─────────────────────┬─────────────────────┬───────────────────────────────────────────────────┐
│ JSON VARIABLE       │ TYPE                │ DESCRIPTION                                       │
├─────────────────────┼─────────────────────┼───────────────────────────────────────────────────┤
│ sdr_dc_correction   │ boolean             │ DC offset correction (true/false)                │
│ sdr_edge_correction │ boolean             │ Edge tuning correction (true/false)              │
│ sdr_deemphasis      │ boolean             │ FM deemphasis filter (true/false)                │
│ sdr_direct_i        │ boolean             │ Direct I-channel sampling (true/false)           │
│ sdr_direct_q        │ boolean             │ Direct Q-channel sampling (true/false)           │
│ sdr_offset_tuning   │ boolean             │ Offset tuning mode (true/false)                  │
└─────────────────────┴─────────────────────┴───────────────────────────────────────────────────┘

📻 MODEM/DECODER VARIABLES (Digital Modes):
┌─────────────────────────┬─────────────────────┬─────────────────────────────────────────────────┐
│ JSON VARIABLE           │ TYPE                │ DESCRIPTION                                     │
├─────────────────────────┼─────────────────────┼─────────────────────────────────────────────────┤
│ sdr_modem_decoder       │ string              │ Decoder type: FT4, FT8, ADS-B, APRS, JT65, MSK │
│ sdr_modem_lead_in       │ integer             │ Lead-in time in milliseconds                   │
│ sdr_modem_sample_rate   │ integer             │ Modem audio rate (default: 48000)              │
│ sdr_modem_trailing      │ integer             │ Trailing time in milliseconds                  │
│ sdr_modem_audio_device  │ string              │ Audio device selection                         │
│ sdr_modem_debug         │ boolean             │ Debug mode for modem (true/false)              │
└─────────────────────────┴─────────────────────┴─────────────────────────────────────────────────┘

🎛️ GET COMMANDS (Read SDR Settings):
┌─────────────────────────────┬─────────────────────────────────────────────────────────────────────────────┐
│ TEXT COMMAND                │ JSON COMMAND                                                                │
├─────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ name 5                      │ {"command":"get_sdr","sdr_id":5,"variable":"sdr_name"}                      │
│ freq 3                      │ {"command":"get_sdr","sdr_id":3,"variable":"sdr_frequency"}                 │
│ mode 8                      │ {"command":"get_sdr","sdr_id":8,"variable":"sdr_modulation"}                │
│ sample 12                   │ {"command":"get_sdr","sdr_id":12,"variable":"sdr_sample_rate"}              │
│ gain 1                      │ {"command":"get_sdr","sdr_id":1,"variable":"sdr_gain"}                      │
│ bias 4                      │ {"command":"get_sdr","sdr_id":4,"variable":"sdr_bias_tee"}                  │
│ agc 7                       │ {"command":"get_sdr","sdr_id":7,"variable":"sdr_agc"}                       │
│ squelch 2                   │ {"command":"get_sdr","sdr_id":2,"variable":"sdr_squelch"}                   │
│ ppm 6                       │ {"command":"get_sdr","sdr_id":6,"variable":"sdr_ppm"}                       │
│ status 9                    │ {"command":"get_sdr","sdr_id":9,"variable":"sdr_status"}                    │
│ type 10                     │ {"command":"get_sdr","sdr_id":10,"variable":"sdr_device_type"}              │
│ serial 11                   │ {"command":"get_sdr","sdr_id":11,"variable":"sdr_serial_number"}            │
│ leadin 5                    │ {"command":"get_sdr","sdr_id":5,"variable":"sdr_modem_lead_in"}             │
│ trailing 5                  │ {"command":"get_sdr","sdr_id":5,"variable":"sdr_modem_trailing"}            │
│ audio 5                     │ {"command":"get_sdr","sdr_id":5,"variable":"sdr_modem_sample_rate"}         │
│ source 5                    │ {"command":"get_sdr","sdr_id":5,"variable":"sdr_modem_audio_device"}        │
│ debug 5                     │ {"command":"get_sdr","sdr_id":5,"variable":"sdr_modem_debug"}               │
│ all 7                       │ {"command":"get_sdr","sdr_id":7,"variable":"all"}                           │
└─────────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘

⚙️ SET COMMANDS (Modify SDR Settings):
┌─────────────────────────────┬──────────────────────────────────────────────────────────────────────────────┐
│ TEXT COMMAND                │ JSON COMMAND                                                                 │
├─────────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ name 5 FM_Scanner           │ {"command":"set_sdr","sdr_id":5,"variable":"sdr_name","value":"FM_Scanner"}  │
│ freq 3 145500000            │ {"command":"set_sdr","sdr_id":3,"variable":"sdr_frequency","value":145500000}│
│ mode 8 fm                   │ {"command":"set_sdr","sdr_id":8,"variable":"sdr_modulation","value":"fm"}    │
│ band 2 2m                   │ {"command":"set_sdr","sdr_id":2,"variable":"sdr_band","value":"2m"}          │
│ sample 12 24000             │ {"command":"set_sdr","sdr_id":12,"variable":"sdr_sample_rate","value":24000} │
│ gain 1 25.4                 │ {"command":"set_sdr","sdr_id":1,"variable":"sdr_gain","value":25.4}          │
│ biast 4 true                │ {"command":"set_sdr","sdr_id":4,"variable":"sdr_bias_tee","value":true}      │
│ converter 4 false           │ {"command":"set_sdr","sdr_id":4,"variable":"sdr_bias_tee","value":false}     │
│ agc 7 manual                │ {"command":"set_sdr","sdr_id":7,"variable":"sdr_agc","value":"manual"}       │
│ squelch 2 5                 │ {"command":"set_sdr","sdr_id":2,"variable":"sdr_squelch","value":5}          │
│ ppm 6 -15                   │ {"command":"set_sdr","sdr_id":6,"variable":"sdr_ppm","value":-15}            │
│ leadin 5 1000               │ {"command":"set_sdr","sdr_id":5,"variable":"sdr_modem_lead_in","value":1000} │
│ trailing 5 500              │ {"command":"set_sdr","sdr_id":5,"variable":"sdr_modem_trailing","value":500} │
│ audio 5 48000               │ {"command":"set_sdr","sdr_id":5,"variable":"sdr_modem_sample_rate","value":48000}│
│ source 5 UDP                │ {"command":"set_sdr","sdr_id":5,"variable":"sdr_modem_audio_device","value":"UDP"}│
│ debug 5 on                  │ {"command":"set_sdr","sdr_id":5,"variable":"sdr_modem_debug","value":true}   │
└─────────────────────────────┴──────────────────────────────────────────────────────────────────────────────┘

� CONTROL COMMANDS:
┌─────────────────────────────┬──────────────────────────────────────────────────────────────────────────────┐
│ TEXT COMMAND                │ JSON COMMAND                                                                 │
├─────────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ start 5                     │ {"command":"control_sdr","sdr_id":5,"action":"start"}                       │
│ stop 5                      │ {"command":"control_sdr","sdr_id":5,"action":"stop"}                        │
│ restart 5                   │ {"command":"control_sdr","sdr_id":5,"action":"restart"}                     │
│ rename sdr 5 NewName        │ {"command":"set_sdr","sdr_id":5,"variable":"sdr_name","value":"NewName"}     │
└─────────────────────────────┴──────────────────────────────────────────────────────────────────────────────┘

�💡 TIPS:
• SDR indices are global (1-12 across all hosts)
• Use simplified commands (name, freq, mode, band, sample, gain, biast, agc, squelch, ppm) for quick interactive control
• Read-only commands: status, type, serial, all
• Use JSON commands for web applications and automation
• Advanced variables (dc_correction, edge_correction, etc.) require JSON commands
• Modem/decoder settings require JSON commands
• All frequency values can be Hz (145500000) or MHz (145.5) - both work!
• Boolean values: true/false (JSON) or 1/0 (text)
• Names preserve case, all other values converted to lowercase
• No "get" or "set" prefixes needed with simplified commands
• biast and converter are aliases for the same bias_tee setting
"""
            
            await self._send_to_connection(host_id, help_text)
            logger.info(f"📋 Sent comprehensive help text to {host_id}")
            
        except Exception as e:
            error_message = f"ERROR: Failed to show help: {e}"
            try:
                await self._send_to_connection(host_id, error_message)
            except:
                pass
            logger.error(f"Error sending help to {host_id}: {e}")

    async def _handle_simplified_command(self, connection_id: str, message: str) -> bool:
        """Handle simplified variable commands without get/set prefixes
        
        Supported commands:
        - name SDR# -> get sdr_name
        - name SDR# NewName -> set sdr_name  
        - freq SDR# -> get sdr_frequency
        - freq SDR# 145500000 -> set sdr_frequency
        - mode SDR# -> get sdr_modulation
        - mode SDR# fm -> set sdr_modulation
        - gain SDR# -> get sdr_gain
        - gain SDR# 25.4 -> set sdr_gain
        - agc SDR# -> get sdr_agc
        - agc SDR# auto -> set sdr_agc
        - ppm SDR# -> get sdr_ppm
        - ppm SDR# -15 -> set sdr_ppm
        - converter SDR# -> get sdr_bias_tee
        - converter SDR# true -> set sdr_bias_tee
        - sample SDR# -> get sdr_sample_rate
        - sample SDR# 24000 -> set sdr_sample_rate
        - squelch SDR# -> get sdr_squelch
        - squelch SDR# 5 -> set sdr_squelch
        - status SDR# -> get sdr_status
        - type SDR# -> get sdr_device_type
        - serial SDR# -> get sdr_serial_number
        - all SDR# -> get all variables
        """
        try:
            parts = message.split()
            logger.info(f"🚀 Simplified command check: '{message}' -> parts: {parts}")
            
            if len(parts) < 2:
                logger.info(f"❌ Not enough parts for simplified command: {len(parts)}")
                return False
                
            # Map simplified commands to actual variable names
            variable_map = {
                "name": "sdr_name",
                "freq": "sdr_frequency", 
                "mode": "sdr_modulation",
                "band": "sdr_band",
                "gain": "sdr_gain",
                "agc": "sdr_agc",
                "ppm": "sdr_ppm",
                "biast": "sdr_bias_tee",
                "converter": "sdr_bias_tee",
                "sample": "sdr_sample_rate",
                "squelch": "sdr_squelch",
                "status": "sdr_status",
                "type": "sdr_device_type",
                "serial": "sdr_serial_number",
                "all": "all",
                # Modem text commands
                "leadin": "sdr_modem_lead_in",
                "trailing": "sdr_modem_trailing", 
                "audio": "sdr_modem_sample_rate",
                "source": "sdr_modem_audio_device",
                "debug": "sdr_modem_debug"
            }
            
            command = parts[0].lower()
            logger.info(f"🔍 Checking if '{command}' is in variable_map: {command in variable_map}")
            
            if command not in variable_map:
                logger.info(f"❌ Command '{command}' not in variable_map: {list(variable_map.keys())}")
                return False
                
            # Must have at least variable and SDR number
            if len(parts) < 2:
                return False
                
            try:
                sdr_number = int(parts[1])
            except ValueError:
                return False
                
            variable = variable_map[command]
            
            if len(parts) == 2:
                # GET command: "name 5" -> "get sdr_name 5"
                get_command = f"get {variable} {sdr_number}"
                logger.info(f"✅ Simplified GET: '{message}' -> '{get_command}'")
                await self._handle_get_command(connection_id, get_command)
            else:
                # SET command: "name 5 NewName" -> "set sdr_name 5 NewName"
                value = " ".join(parts[2:])  # Allow spaces in values
                set_command = f"set {variable} {sdr_number} {value}"
                logger.info(f"✅ Simplified SET: '{message}' -> '{set_command}'")
                await self._handle_set_command(connection_id, set_command)
                
            return True
            
        except Exception as e:
            logger.error(f"Error handling simplified command '{message}': {e}")
            return False

    async def _handle_rename_command(self, connection_id: str, message: str):
        """Handle rename SDR command - both plain text and JSON formats
        
        Plain text format: rename sdr <global_index> <new_name>
        Example: rename sdr 5 FM_Scanner
        """
        try:
            # Parse plain text command: "rename sdr 5 FM_Scanner"
            parts = message.split()
            if len(parts) < 4:
                error_msg = "Usage: rename sdr <global_index> <new_name>\nExample: rename sdr 5 FM_Scanner"
                await self._send_to_connection(connection_id, error_msg)
                return
            
            try:
                global_index = int(parts[2])
                new_name = " ".join(parts[3:])  # Allow spaces in names
            except ValueError:
                error_msg = "Error: Global index must be a number\nExample: rename sdr 5 FM_Scanner"
                await self._send_to_connection(connection_id, error_msg)
                return
            
            # Find the SDR by global index
            target_sdr = None
            target_host = None
            
            for host in self.hosts.values():
                for sdr in host.sdr_devices:
                    if sdr.global_index == global_index:
                        target_sdr = sdr
                        target_host = host
                        break
                if target_sdr:
                    break
            
            if not target_sdr:
                error_msg = f"Error: SDR with global index {global_index} not found"
                await self._send_to_connection(connection_id, error_msg)
                return
            
            # Update the SDR name locally
            old_name = target_sdr.sdr_name or f"SDR #{global_index}"
            target_sdr.sdr_name = new_name
            
            # Send rename command to the host
            rename_message = {
                "type": "sdr_command",
                "local_index": target_sdr.local_index,
                "command": {
                    "type": "sdr_rename",
                    "name": new_name
                },
                "timestamp": datetime.now().isoformat()
            }
            
            await target_host.websocket.send(json.dumps(rename_message))
            
            # Send success confirmation
            success_msg = f"✅ Renamed SDR #{global_index} from '{old_name}' to '{new_name}'"
            await self._send_to_connection(connection_id, success_msg)
            
            logger.info(f"📝 Renamed SDR #{global_index} to '{new_name}' on host {target_host.ip_address}")
            
            # Broadcast the change to all connected clients
            await self._broadcast_host_update("sdr_renamed", target_host.host_id)
            
        except Exception as e:
            error_msg = f"Error processing rename command: {e}"
            await self._send_to_connection(connection_id, error_msg)
            logger.error(f"Error handling rename command from {connection_id}: {e}")

    async def _handle_set_command(self, connection_id: str, message: str):
        """Handle set command - set SDR variables
        
        Format: set <variable> <sdr_index> <value>
        Examples: 
          set name 5 FM_Scanner
          set frequency 3 145500000
          set gain 1 20.0
        """
        try:
            # Parse: "set name 5 FM_Scanner"
            parts = message.split()
            if len(parts) < 4:
                error_msg = ("Usage: set <variable> <sdr_index> <value>\\n"
                           "Examples:\\n"
                           "  set name 5 FM_Scanner\\n"
                           "  set frequency 3 145500000\\n"
                           "  set gain 1 20.0")
                await self._send_to_connection(connection_id, error_msg)
                return
            
            variable = parts[1]
            sdr_target = parts[2]
            value = " ".join(parts[3:])  # Allow spaces in values
            
            # Parse SDR target (can be just number like "5" or old format "sdr5")
            try:
                if sdr_target.startswith('sdr'):
                    # Support old format: sdr5 -> 5
                    global_index = int(sdr_target[3:])
                else:
                    # New simple format: 5 -> 5
                    global_index = int(sdr_target)
            except ValueError:
                error_msg = "Invalid SDR index (e.g., use 5 for SDR[5])"
                await self._send_to_connection(connection_id, error_msg)
                return
            
            # Find the SDR by global index
            target_sdr = None
            target_host = None
            
            for host in self.hosts.values():
                for sdr in host.sdr_devices:
                    if sdr.global_index == global_index:
                        target_sdr = sdr
                        target_host = host
                        break
                if target_sdr:
                    break
            
            if not target_sdr:
                error_msg = f"Error: SDR #{global_index} not found"
                await self._send_to_connection(connection_id, error_msg)
                return
            
            # Convert variable name to command type - map 'name' to 'sdr_name'
            if variable == "name":
                command_type = "sdr_name"
            elif variable in ["biast", "bias_tee", "biastee"]:
                command_type = "sdr_biasTee"
            else:
                command_type = f"sdr_{variable}" if not variable.startswith("sdr_") else variable
            
            # Build the command based on variable type
            command_data = {"type": command_type}
            
            # Handle different variable types
            if variable in ["sdr_name", "name"]:
                command_data["name"] = value  # Keep original case for names
            elif variable in ["sdr_frequency", "frequency", "freq"]:
                try:
                    # Get current mode for context-aware frequency parsing
                    current_mode = getattr(target_sdr, 'modulation', None)
                    
                    # Use enhanced frequency parsing with band mapping support
                    freq_value = parse_frequency(value, current_mode)
                    command_data["frequency"] = freq_value
                    
                    logger.info(f"🔄 Enhanced frequency parsing: '{value}' (mode: {current_mode}) → {freq_value} Hz")
                except ValueError as e:
                    error_msg = f"Invalid frequency format: {e}"
                    await self._send_to_connection(connection_id, error_msg)
                    return
            elif variable in ["sdr_modulation", "modulation", "mode"]:
                command_data["modulation"] = value.lower()  # Convert to lowercase
            elif variable in ["sdr_band", "band"]:
                command_data["sdr_band"] = value.lower()  # Store band in lowercase for consistency
            elif variable in ["sdr_agc", "agc"]:
                command_data["agc"] = value.lower()  # Convert to lowercase
            elif variable in ["sdr_gain", "gain"]:
                try:
                    command_data["sdr_gain"] = float(value)
                except ValueError:
                    error_msg = f"Gain must be a number, got: {value}"
                    await self._send_to_connection(connection_id, error_msg)
                    return
            elif variable in ["sdr_sample_rate", "sample_rate", "samplerate"]:
                try:
                    command_data["sdr_sample_rate"] = int(value)
                except ValueError:
                    error_msg = f"Sample rate must be an integer, got: {value}"
                    await self._send_to_connection(connection_id, error_msg)
                    return
            elif variable in ["sdr_biast", "bias_tee", "biastee", "sdr_bias_tee"]:
                # Handle on/off and 1/0 values, store as sdr_bias_tee
                command_data["sdr_bias_tee"] = str(1 if str(value).lower() in ["on", "1", "true", "yes"] else 0)
            else:
                # Generic variable - pass as string (convert to lowercase for consistency)
                if variable not in ["sdr_name", "name"]:
                    command_data[variable] = value.lower()
                else:
                    command_data[variable] = value
            
            # Send command to host
            set_message = {
                "type": "sdr_command",
                "local_index": target_sdr.local_index,
                "command": command_data,
                "timestamp": datetime.now().isoformat()
            }
            
            await target_host.websocket.send(json.dumps(set_message))
            
            # Update server's local SDR representation immediately
            if variable in ["sdr_name", "name"]:
                target_sdr.sdr_name = value  # Keep original case for names
                logger.info(f"📝 Updated server's local SDR #{global_index} name to '{value}'")
            elif variable in ["sdr_frequency", "frequency", "freq"]:
                # Use the converted Hz value, not the original MHz input
                target_sdr.frequency = float(command_data["frequency"])
                logger.info(f"📝 Updated server's local SDR #{global_index} frequency to {command_data['frequency']} Hz")
                logger.info(f"🔍 DEBUG: target_sdr.frequency = {target_sdr.frequency}, type = {type(target_sdr.frequency)}")
            elif variable in ["sdr_modulation", "modulation", "mode"]:
                old_modulation = target_sdr.modulation
                target_sdr.modulation = value.lower()  # Convert to lowercase
                logger.info(f"📝 Updated server's local SDR #{global_index} modulation to '{value.lower()}'")
                
                # Auto-update decoder when modulation changes
                await self._update_decoder_for_modulation_change(target_sdr, target_host, value.lower())
            elif variable in ["sdr_band", "band"]:
                target_sdr.band = value.lower()  # Store band in lowercase for consistency
                logger.info(f"📝 Updated server's local SDR #{global_index} band to '{target_sdr.band}'")
            elif variable in ["sdr_agc", "agc"]:
                target_sdr.agc = value.lower()  # Convert to lowercase
                logger.info(f"📝 Updated server's local SDR #{global_index} agc to {target_sdr.agc}")
            elif variable in ["sdr_gain", "gain"]:
                target_sdr.gain = float(value)  # Store as gain
                logger.info(f"📝 Updated server's local SDR #{global_index} gain to {target_sdr.gain}")
            elif variable in ["sdr_ppm", "ppm"]:
                target_sdr.ppm = int(float(value))  # Convert to int as required
                logger.info(f"📝 Updated server's local SDR #{global_index} ppm to {target_sdr.ppm}")
            elif variable in ["sdr_squelch", "squelch"]:
                target_sdr.squelch = int(float(value))  # Store as squelch (int field)
                logger.info(f"📝 Updated server's local SDR #{global_index} squelch to {target_sdr.squelch}")
            elif variable in ["sdr_biast", "bias_tee", "biastee", "sdr_bias_tee"]:
                # Handle on/off and 1/0 values, store as boolean in bias_tee field
                target_sdr.bias_tee = True if str(value).lower() in ["on", "1", "true", "yes"] else False
                logger.info(f"📝 Updated server's local SDR #{global_index} bias_tee to {target_sdr.bias_tee}")
            elif variable in ["sdr_sample_rate", "sample_rate", "samplerate"]:
                target_sdr.sample_rate = int(value)  # Store as sample_rate (int field)
                logger.info(f"📝 Updated server's local SDR #{global_index} sample_rate to {target_sdr.sample_rate}")
            # Modem variables
            elif variable in ["sdr_modem_lead_in", "leadin"]:
                try:
                    target_sdr.modem_lead_in = int(value) if value else 0
                    logger.info(f"📝 Updated server's local SDR #{global_index} modem_lead_in to {target_sdr.modem_lead_in}")
                except ValueError:
                    target_sdr.modem_lead_in = 0
                    logger.info(f"📝 Set default modem_lead_in=0 for SDR #{global_index} (invalid value: {value})")
            elif variable in ["sdr_modem_trailing", "trailing"]:
                try:
                    target_sdr.modem_trailing = int(value) if value else 0
                    logger.info(f"📝 Updated server's local SDR #{global_index} modem_trailing to {target_sdr.modem_trailing}")
                except ValueError:
                    target_sdr.modem_trailing = 0
                    logger.info(f"📝 Set default modem_trailing=0 for SDR #{global_index} (invalid value: {value})")
            elif variable in ["sdr_modem_sample_rate", "audio"]:
                try:
                    target_sdr.modem_sample_rate = int(value) if value else 48000
                    logger.info(f"📝 Updated server's local SDR #{global_index} modem_sample_rate to {target_sdr.modem_sample_rate}")
                except ValueError:
                    target_sdr.modem_sample_rate = 48000
                    logger.info(f"📝 Set default modem_sample_rate=48000 for SDR #{global_index} (invalid value: {value})")
            elif variable in ["sdr_modem_audio_device", "source"]:
                target_sdr.modem_audio_device = str(value) if value else "UDP"
                logger.info(f"📝 Updated server's local SDR #{global_index} modem_audio_device to {target_sdr.modem_audio_device}")
            elif variable in ["sdr_modem_debug", "debug"]:
                target_sdr.modem_debug = str(value).lower() in ["on", "1", "true", "yes"]
                logger.info(f"📝 Updated server's local SDR #{global_index} modem_debug to {target_sdr.modem_debug}")
            # Add other variables as needed
            
            # Forward the command to the actual host to update its config file
            if target_host:  # Only forward if we have a valid host connection
                try:
                    # Initialize simple_command
                    simple_command = None
                    
                    # Send simple text command format that hosts understand
                    if variable in ["sdr_frequency", "frequency", "freq"]:
                        # Send frequency as Hz integer, using sdr_frequency as variable name
                        simple_command = f"set {target_sdr.local_index} sdr_frequency {int(command_data['frequency'])}"
                    elif variable in ["sdr_modulation", "modulation", "mode"]:
                        # Use sdr_modulation as variable name  
                        simple_command = f"set {target_sdr.local_index} sdr_modulation {value.lower()}"
                    elif variable in ["sdr_band", "band"]:
                        simple_command = f"set {target_sdr.local_index} sdr_band {value.lower()}"
                    elif variable in ["sdr_name", "name"]:
                        simple_command = f"set {target_sdr.local_index} sdr_name {value}"
                    elif variable in ["sdr_agc", "agc"]:
                        simple_command = f"set {target_sdr.local_index} sdr_agc {value.lower()}"
                    elif variable in ["sdr_gain", "gain"]:
                        simple_command = f"set {target_sdr.local_index} sdr_gain {float(value)}"
                    elif variable in ["sdr_ppm", "ppm"]:
                        simple_command = f"set {target_sdr.local_index} sdr_ppm {int(float(value))}"
                    elif variable in ["sdr_squelch", "squelch"]:
                        simple_command = f"set {target_sdr.local_index} sdr_squelch {float(value)}"
                    elif variable in ["sdr_biast", "bias_tee", "biastee", "sdr_bias_tee"]:
                        # Convert on/off and 1/0 to integer value and send as sdr_bias_tee
                        biast_value = 1 if str(value).lower() in ["on", "1", "true", "yes"] else 0
                        simple_command = f"set {target_sdr.local_index} sdr_bias_tee {biast_value}"
                    elif variable in ["sdr_sample_rate", "sample_rate", "samplerate"]:
                        simple_command = f"set {target_sdr.local_index} sdr_sample_rate {int(value)}"
                    # Check if this is a modem variable that needs JSON command format
                    modem_variables = ["sdr_modem_lead_in", "leadin", "sdr_modem_trailing", "trailing", 
                                     "sdr_modem_sample_rate", "audio", "sdr_modem_audio_device", "source", 
                                     "sdr_modem_debug", "debug"]
                    
                    if variable in modem_variables:
                        # For modem variables, send JSON command instead of text command
                        # Map variable names to their sdr_ equivalents
                        json_variable = variable
                        if variable == "leadin":
                            json_variable = "sdr_modem_lead_in"
                        elif variable == "trailing":
                            json_variable = "sdr_modem_trailing"
                        elif variable == "audio":
                            json_variable = "sdr_modem_sample_rate"
                        elif variable == "source":
                            json_variable = "sdr_modem_audio_device"
                        elif variable == "debug":
                            json_variable = "sdr_modem_debug"
                        
                        # Convert values appropriately
                        json_value = value
                        if json_variable in ["sdr_modem_lead_in", "sdr_modem_trailing"]:
                            try:
                                json_value = int(value) if value else 0
                            except ValueError:
                                json_value = 0
                        elif json_variable == "sdr_modem_sample_rate":
                            try:
                                json_value = int(value) if value else 48000
                            except ValueError:
                                json_value = 48000
                        elif json_variable == "sdr_modem_audio_device":
                            json_value = str(value).lower() if value else "udp"
                        elif json_variable == "sdr_modem_debug":
                            json_value = str(value).lower() in ["on", "1", "true", "yes"]
                        
                        # Create JSON command for the host
                        json_command = {
                            "type": "sdr_command",
                            "local_index": target_sdr.local_index,
                            "command": {
                                "type": "set_sdr",
                                "variable": json_variable,
                                "value": json_value
                            }
                        }
                        
                        logger.info(f"📊 Forwarding JSON command to host {target_host.ip_address}: {json_command}")
                        await target_host.websocket.send(json.dumps(json_command))
                        logger.info(f"✅ JSON command forwarded to host successfully")
                    else:
                        # For non-modem variables, use simple text command format
                        simple_command = f"set {target_sdr.local_index} {variable} {value}"
                        
                        logger.info(f"📊 Forwarding simple command to host {target_host.ip_address}: {simple_command}")
                        await target_host.websocket.send(simple_command)
                        logger.info(f"✅ Simple command forwarded to host successfully")
                    
                except Exception as forward_error:
                    logger.error(f"❌ Failed to forward command to host {target_host.ip_address}: {forward_error}")
                    # Continue anyway - at least the server's local copy is updated
            else:
                logger.warning(f"No host connection found for SDR #{global_index}, only updating server's local copy")
            
            # Send confirmation
            success_msg = f"✅ Set {variable} = '{value}' for SDR #{global_index}"
            await self._send_to_connection(connection_id, success_msg)
            
            logger.info(f"📝 Set {variable}='{value}' for SDR #{global_index} on host {target_host.ip_address if target_host else 'unknown'}")
            
        except Exception as e:
            error_msg = f"Error processing set command: {e}"
            await self._send_to_connection(connection_id, error_msg)
            logger.error(f"Error handling set command from {connection_id}: {e}")

    async def _handle_get_command(self, connection_id: str, message: str):
        """Handle get command - get SDR variables
        
        Format: get <variable> <sdr_index>
        Examples:
          get name 5
          get frequency 3
          get status 1
        """
        try:
            # Parse: "get name 5"
            parts = message.split()
            if len(parts) != 3:
                error_msg = ("Usage: get <variable> <sdr_index>\\n"
                           "Examples:\\n"
                           "  get name 5\\n"
                           "  get frequency 3\\n"
                           "  get status 1")
                await self._send_to_connection(connection_id, error_msg)
                return
            
            variable = parts[1]
            sdr_target = parts[2]
            
            # Parse SDR target (can be just number like "5" or old format "sdr5")
            try:
                if sdr_target.startswith('sdr'):
                    # Support old format: sdr5 -> 5
                    global_index = int(sdr_target[3:])
                else:
                    # New simple format: 5 -> 5
                    global_index = int(sdr_target)
            except ValueError:
                error_msg = "Invalid SDR index (e.g., use 5 for SDR[5])"
                await self._send_to_connection(connection_id, error_msg)
                return
            
            # Find the SDR by global index
            target_sdr = None
            target_host = None
            
            for host in self.hosts.values():
                for sdr in host.sdr_devices:
                    if sdr.global_index == global_index:
                        target_sdr = sdr
                        target_host = host
                        break
                if target_sdr:
                    break
            
            if not target_sdr:
                error_msg = f"Error: SDR #{global_index} not found"
                await self._send_to_connection(connection_id, error_msg)
                return
            
            # Get the value from the SDR object
            if variable in ["sdr_name", "name"]:
                value = getattr(target_sdr, 'sdr_name', "(not found)")
            elif variable in ["sdr_frequency", "frequency", "freq"]:
                value = getattr(target_sdr, 'frequency', "(not found)")
            elif variable in ["sdr_band", "band"]:
                value = getattr(target_sdr, 'band', "(not found)")
            elif variable in ["sdr_gain", "gain"]:
                value = getattr(target_sdr, 'gain', "(not found)")
            elif variable in ["sdr_sample_rate", "sample_rate", "samplerate"]:
                value = getattr(target_sdr, 'sample_rate', "(not found)")
            elif variable in ["sdr_bias_tee", "bias_tee", "biastee", "biast"]:
                value = getattr(target_sdr, 'bias_tee', "(not found)")
            elif variable in ["sdr_squelch", "squelch"]:
                value = getattr(target_sdr, 'squelch', "(not found)")
            elif variable in ["sdr_ppm", "ppm"]:
                value = getattr(target_sdr, 'ppm', "(not found)")
            elif variable in ["sdr_modulation", "modulation", "mode"]:
                value = getattr(target_sdr, 'modulation', "(not found)")
            elif variable in ["sdr_agc", "agc"]:
                value = getattr(target_sdr, 'agc', "(not found)")
            elif variable in ["sdr_status", "status"]:
                value = getattr(target_sdr, 'status', "(not found)")
            elif variable in ["sdr_serial_number", "serial_number", "serial"]:
                value = getattr(target_sdr, 'serial_number', "(not found)")
            elif variable in ["sdr_device_type", "device_type", "type"]:
                value = getattr(target_sdr, 'device_type', "(not found)")
            else:
                # Try to get generic attribute
                value = getattr(target_sdr, variable, "(not found)")
            
            # Send result
            result_msg = f"📋 SDR #{global_index} {variable}: {value}"
            await self._send_to_connection(connection_id, result_msg)
            
            logger.info(f"📊 Got {variable}='{value}' for SDR #{global_index}")
            
        except Exception as e:
            error_msg = f"Error processing get command: {e}"
            await self._send_to_connection(connection_id, error_msg)
            logger.error(f"Error handling get command from {connection_id}: {e}")

    def get_sdr_by_global_index(self, global_index: int) -> Optional[tuple]:
        """Get SDR and its host by global index"""
        for host in self.hosts.values():
            for sdr in host.sdr_devices:
                if sdr.global_index == global_index:
                    return sdr, host
        return None

    async def send_sdr_command(self, global_index: int, command: dict) -> dict:
        """Send a command to a specific SDR by global index"""
        result = self.get_sdr_by_global_index(global_index)
        if not result:
            return {"error": f"SDR with global index {global_index} not found"}
        
        sdr, host = result
        
        # Validate and process SDR configuration commands
        command_type = command.get("type") or command.get("command")
        
        # Debug logging
        logger.info(f"🔍 send_sdr_command - command_type: {command_type}, command: {command}")
        
        # Enhanced JSON API - Handle unified set/get commands for all variables
        if command_type == "set_sdr":
            # Handle set commands for any SDR variable
            variable = command.get("variable")
            value = command.get("value")
            
            if not variable:
                return {"error": "Variable is required for set command"}
            if value is None:
                return {"error": "Value is required for set command"}
            
            # Map variable names to SDR attributes and validation
            if variable == "sdr_name":
                if not isinstance(value, str):
                    return {"error": "sdr_name must be a string"}
                sdr.sdr_name = value.strip()
                
            elif variable == "frequency":
                try:
                    freq_val = float(value)
                    if freq_val <= 0:
                        return {"error": "Frequency must be positive"}
                    sdr.frequency = freq_val
                except (ValueError, TypeError):
                    return {"error": "Frequency must be a number"}
                    
            elif variable == "sample_rate":
                try:
                    rate_val = int(value)
                    if rate_val <= 0:
                        return {"error": "Sample rate must be positive"}
                    sdr.sample_rate = rate_val
                except (ValueError, TypeError):
                    return {"error": "Sample rate must be an integer"}
                    
            elif variable == "bias_tee":
                sdr.bias_tee = bool(value)
                
            elif variable == "ppm":
                try:
                    ppm_val = int(value)
                    if not (-20 <= ppm_val <= 20):
                        return {"error": "PPM must be between -20 and +20"}
                    sdr.ppm = ppm_val
                except (ValueError, TypeError):
                    return {"error": "PPM must be an integer"}
                    
            else:
                return {"error": f"Unknown variable: {variable}"}
            
            logger.info(f"📝 Set {variable}='{value}' for SDR #{sdr.global_index}")
            return {"success": f"Set {variable} = '{value}' for SDR #{sdr.global_index}"}
        
        # Handle SDR configuration commands
        if command_type == "sdr_frequency":
            frequency = command.get("frequency")
            if frequency is None:
                return {"error": "Frequency must be specified"}
            if not isinstance(frequency, (int, float)) or frequency <= 0:
                return {"error": "Frequency must be a positive number"}
            sdr.frequency = frequency
            
        elif command_type == "sdr_modulation":
            modulation = command.get("modulation")
            valid_modes = ["AM", "FM", "USB", "LSB", "CW", "WFM", "NFM"]
            if modulation not in valid_modes:
                return {"error": f"Modulation must be one of: {', '.join(valid_modes)}"}
            sdr.modulation = modulation
            
        elif command_type == "sdr_sampleRate":
            sample_rate = command.get("sample_rate", 24000)
            if not isinstance(sample_rate, int) or sample_rate <= 0:
                return {"error": "Sample rate must be a positive integer"}
            sdr.sample_rate = sample_rate
            
        elif command_type == "sdr_biasTee":
            bias_tee = command.get("bias_tee") or command.get("biast") or command.get("value", 0)
            # Handle on/off and 1/0 values, store as sdr_bias_tee 
            sdr.sdr_bias_tee = 1 if str(bias_tee).lower() in ["on", "1", "true", "yes"] else 0
            
        elif command_type == "sdr_agc":
            agc = command.get("agc", "auto")
            sdr.agc = str(agc)
            
        elif command_type == "sdr_squelch":
            squelch = command.get("squelch", 0)
            if not isinstance(squelch, (int, float)):
                return {"error": "Squelch must be a number"}
            sdr.squelch = int(squelch)
            
        elif command_type == "sdr_ppm":
            ppm = command.get("ppm", 0)
            if not isinstance(ppm, (int, float)) or not (-20 <= ppm <= 20):
                return {"error": "PPM must be between -20 and +20"}
            sdr.ppm = int(ppm)
            
        elif command_type == "sdr_dc":
            sdr.dc_correction = bool(command.get("dc_correction", False))
            
        elif command_type == "sdr_edge":
            sdr.edge_correction = bool(command.get("edge_correction", False))
            
        elif command_type == "sdr_deemp":
            sdr.deemphasis = bool(command.get("deemphasis", False))
            
        elif command_type == "sdr_directI":
            sdr.direct_i = bool(command.get("direct_i", False))
            
        elif command_type == "sdr_directQ":
            sdr.direct_q = bool(command.get("direct_q", False))
            
        elif command_type == "sdr_offset":
            sdr.offset_tuning = bool(command.get("offset_tuning", False))
            
        # Handle modem configuration commands
        elif command_type == "modem_decoder":
            decoder = command.get("decoder")
            valid_decoders = ["FT4", "FT8", "ADS-B", "APRS", "JT65", "MSK", "Q65"]
            if decoder not in valid_decoders:
                return {"error": f"Decoder must be one of: {', '.join(valid_decoders)}"}
            sdr.modem_decoder = decoder
            
        elif command_type == "modem_leadIn":
            lead_in = command.get("lead_in", 1000)
            if not isinstance(lead_in, int) or lead_in < 0:
                return {"error": "Lead-in time must be a non-negative integer (ms)"}
            sdr.modem_lead_in = lead_in
            
        elif command_type == "modem_sampleRate":
            modem_sample_rate = command.get("sample_rate", 48000)
            if not isinstance(modem_sample_rate, int) or modem_sample_rate <= 0:
                return {"error": "Modem sample rate must be a positive integer"}
            sdr.modem_sample_rate = modem_sample_rate
            
        elif command_type == "modem_trailing":
            trailing = command.get("trailing", 1000)
            if not isinstance(trailing, int) or trailing < 0:
                return {"error": "Trailing time must be a non-negative integer (ms)"}
            sdr.modem_trailing = trailing
            
        elif command_type == "modem_audio":
            # This command returns available audio devices
            return {"audio_devices": ["default", "system", "pulse", "alsa"]}
            
        elif command_type == "modem_debug":
            sdr.modem_debug = bool(command.get("debug", False))
            
        elif command_type == "sdr_rename":
            new_name = command.get("name")
            if not new_name or not isinstance(new_name, str):
                return {"error": "Name is required and must be a string"}
            sdr.sdr_name = new_name.strip()
            logger.info(f"📝 Updated SDR name to '{sdr.sdr_name}' for SDR #{sdr.global_index}")
        
        elif command_type == "sdr_name":
            new_name = command.get("name")
            if not new_name or not isinstance(new_name, str):
                return {"error": "Name is required and must be a string"}
            sdr.sdr_name = new_name.strip()
            logger.info(f"📝 Updated SDR name to '{sdr.sdr_name}' for SDR #{sdr.global_index}")
        
        elif command_type == "get_sdr":
            # Handle get commands for retrieving SDR values
            variable = command.get("variable")
            if not variable:
                return {"error": "Variable is required for get command"}
            
            # Map of all available variables to their SDR attributes
            variable_map = {
                "sdr_name": sdr.sdr_name,
                "frequency": sdr.frequency,
                "modulation": sdr.modulation,
                "sample_rate": sdr.sample_rate,
                "bias_tee": sdr.bias_tee,
                "agc": sdr.agc,
                "squelch": sdr.squelch,
                "ppm": sdr.ppm,
                "dc_correction": sdr.dc_correction,
                "edge_correction": sdr.edge_correction,
                "deemphasis": sdr.deemphasis,
                "direct_i": sdr.direct_i,
                "direct_q": sdr.direct_q,
                "offset_tuning": sdr.offset_tuning,
                "status": sdr.status,
                "device_type": sdr.device_type,
                "serial_number": sdr.serial_number,
                "host_ip": sdr.host_ip,
                "local_index": sdr.local_index,
                "global_index": sdr.global_index,
                "modem_decoder": sdr.modem_decoder,
                "modem_lead_in": sdr.modem_lead_in,
                "modem_sample_rate": sdr.modem_sample_rate,
                "modem_trailing": sdr.modem_trailing,
                "modem_audio_device": sdr.modem_audio_device
            }
            
            if variable == "all":
                # Return all variables
                return {"variable": "all", "values": variable_map}
            elif variable in variable_map:
                value = variable_map[variable]
                return {"variable": variable, "value": value if value is not None else "(not set)"}
            else:
                return {"error": f"Unknown variable: {variable}. Available: {', '.join(sorted(variable_map.keys())) + ', all'}"}
        
        elif command_type == "set_sdr":
            # Handle set commands for any SDR variable
            variable = command.get("variable")
            value = command.get("value")
            
            if not variable:
                return {"error": "Variable is required for set command"}
            if value is None:
                return {"error": "Value is required for set command"}
            
            # Handle each variable type with appropriate validation
            if variable == "sdr_name":
                if not isinstance(value, str):
                    return {"error": "sdr_name must be a string"}
                sdr.sdr_name = value.strip()
                
            elif variable == "frequency":
                try:
                    freq_val = float(value)
                    if freq_val <= 0:
                        return {"error": "Frequency must be positive"}
                    sdr.frequency = freq_val
                except (ValueError, TypeError):
                    return {"error": "Frequency must be a number"}
                    
            elif variable == "modulation":
                valid_modes = ["AM", "FM", "USB", "LSB", "CW", "WFM", "NFM", "FT4", "FT8", "ADSB", "APRS"]
                if value not in valid_modes:
                    return {"error": f"Modulation must be one of: {', '.join(valid_modes)}"}
                old_modulation = sdr.modulation
                sdr.modulation = value
                
                # Auto-update decoder when modulation changes
                await self._update_decoder_for_modulation_change(sdr, host, value)
                
            elif variable == "sample_rate":
                try:
                    rate_val = int(value)
                    if rate_val <= 0:
                        return {"error": "Sample rate must be positive"}
                    sdr.sample_rate = rate_val
                except (ValueError, TypeError):
                    return {"error": "Sample rate must be an integer"}
                    
            elif variable == "bias_tee":
                sdr.bias_tee = bool(value)
                
            elif variable == "agc":
                sdr.agc = str(value)
                
            elif variable == "squelch":
                try:
                    sdr.squelch = int(value)
                except (ValueError, TypeError):
                    return {"error": "Squelch must be an integer"}
                    
            elif variable == "ppm":
                try:
                    ppm_val = int(value)
                    if not (-20 <= ppm_val <= 20):
                        return {"error": "PPM must be between -20 and +20"}
                    sdr.ppm = ppm_val
                except (ValueError, TypeError):
                    return {"error": "PPM must be an integer"}
                    
            elif variable in ["dc_correction", "edge_correction", "deemphasis", "direct_i", "direct_q", "offset_tuning"]:
                setattr(sdr, variable, bool(value))
                
            elif variable == "modem_decoder":
                valid_decoders = ["FT4", "FT8", "ADS-B", "APRS", "JT65", "MSK", "Q65", None]
                if value and value not in valid_decoders:
                    return {"error": f"Modem decoder must be one of: {', '.join([d for d in valid_decoders if d])} or null"}
                sdr.modem_decoder = value
                
            elif variable == "modem_sample_rate":
                try:
                    rate_val = int(value)
                    if rate_val <= 0:
                        return {"error": "Modem sample rate must be positive"}
                    sdr.modem_sample_rate = rate_val
                except (ValueError, TypeError):
                    return {"error": "Modem sample rate must be an integer"}
                    
            elif variable in ["modem_lead_in", "modem_trailing"]:
                try:
                    time_val = int(value) if value else None
                    setattr(sdr, variable, time_val)
                except (ValueError, TypeError):
                    return {"error": f"{variable} must be an integer or null"}
                    
            elif variable == "modem_audio_device":
                sdr.modem_audio_device = str(value) if value else None
                
            else:
                return {"error": f"Variable '{variable}' is not settable. Use get_sdr with variable='all' to see available variables."}
            
            logger.info(f"📝 Updated {variable} = '{value}' for SDR #{sdr.global_index}")
            return {"success": f"Set {variable} = '{value}' for SDR #{sdr.global_index}"}
        
        # For get commands and successful set commands, return immediately without sending to host
        if command_type in ["get_sdr", "set_sdr"]:
            # Already returned above in the respective handlers
            return {"error": "Unexpected flow - command should have returned earlier"}
        
        # Prepare command message for the host (for set/action commands)
        cmd_message = {
            "type": "sdr_command",
            "local_index": sdr.local_index,
            "command": command,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            await host.websocket.send(json.dumps(cmd_message))
            return {
                "success": f"Command '{command_type}' sent to SDR-{global_index} on host {host.ip_address}",
                "sdr_config": {
                    "frequency": sdr.frequency,
                    "modulation": sdr.modulation,
                    "sample_rate": sdr.sample_rate,
                    "bias_tee": sdr.bias_tee,
                    "agc": sdr.agc,
                    "squelch": sdr.squelch,
                    "ppm": sdr.ppm,
                    "modem_decoder": sdr.modem_decoder
                }
            }
        except Exception as e:
            return {"error": f"Failed to send command to SDR-{global_index}: {e}"}

    def get_system_status(self) -> dict:
        """Get overall system status"""
        total_sdrs = sum(len(host.sdr_devices) for host in self.hosts.values())
        
        status = {
            "server_ip": self.local_ip,
            "total_hosts": len(self.hosts),
            "total_sdrs": total_sdrs,
            "hosts": []
        }
        
        for host in self.hosts.values():
            host_info = {
                "ip_address": host.ip_address,
                "connected_at": host.connected_at.isoformat(),
                "last_heartbeat": host.last_heartbeat.isoformat(),
                "sdr_count": len(host.sdr_devices),
                "sdrs": [asdict(sdr) for sdr in host.sdr_devices]
            }
            status["hosts"].append(host_info)
        
        return status

    async def start_server(self):
        """Start the SDR server"""
        # Clean up ports before starting
        if not self.cleanup_ports():
            logger.error("Port cleanup failed - some ports may still be in use")
            logger.info("Attempting to start server anyway...")
        
        self.running = True
        logger.info(f"🚀 Starting SDR Server on {self.local_ip}:{self.ws_port}")
        
        # Start UDP broadcast service
        broadcast_task = asyncio.create_task(self.udp_broadcast_service())
        
        # Start stale host cleanup service
        cleanup_task = asyncio.create_task(self.cleanup_stale_hosts())
        
        # Start WebSocket server
        try:
            ws_server = await websockets.serve(
                self.handle_websocket_connection,
                "0.0.0.0",
                self.ws_port
            )
            
            logger.info(f"✅ SDR Server running - UDP broadcast on port {self.udp_port}, WebSocket on port {self.ws_port}")
            logger.info(f"🧹 Stale host cleanup enabled (60s timeout)")
            
            try:
                await asyncio.gather(broadcast_task, cleanup_task, ws_server.wait_closed())
            except KeyboardInterrupt:
                logger.info("🛑 Shutting down SDR Server...")
            finally:
                self.running = False
                broadcast_task.cancel()
                cleanup_task.cancel()
                ws_server.close()
                
        except OSError as e:
            if "Address already in use" in str(e):
                logger.error(f"❌ Port {self.ws_port} is still in use after cleanup. Please check for remaining processes.")
                logger.error("You may need to manually kill processes or wait for them to release the port.")
            else:
                logger.error(f"❌ Failed to start server: {e}")
            raise

# CLI Interface for testing
async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='SDR Server - Central management server for distributed SDR hosts')
    parser.add_argument('--cleanup-only', action='store_true', 
                       help='Only perform port cleanup and exit')
    parser.add_argument('--udp-port', type=int, default=4210,
                       help='UDP broadcast port (default: 4210)')
    parser.add_argument('--ws-port', type=int, default=4010,
                       help='WebSocket server port (default: 4010)')
    
    args = parser.parse_args()
    
    server = SDRServer(udp_port=args.udp_port, ws_port=args.ws_port)
    
    if args.cleanup_only:
        logger.info("Performing port cleanup only...")
        success = server.cleanup_ports()
        if success:
            logger.info("Port cleanup completed successfully")
            return
        else:
            logger.error("Port cleanup failed")
            return
    
    # Start the server
    await server.start_server()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSDR Server stopped.")
