#!/usr/bin/env python3
"""
Real FT8 Live Decode Monitor
Monitors actual FT8 decoder log files for real live decodes only
"""

import asyncio
import websockets
import json
import subprocess
import time
import re
from datetime import datetime
from pathlib import Path
import os

class RealFT8Monitor:
    def __init__(self):
        self.websocket_uri = "ws://10.146.1.241:4010"
        self.websocket = None
        self.monitoring = False
        
        # SDR to frequency mapping (with 125MHz upconverter)
        # These are the RTL-SDR tune frequencies (target + 125MHz)
        self.sdr_frequencies = {
            9: 143100000,   # SDR #9: 17m FT8 (18.100 + 125 MHz)
            10: 139074000,  # SDR #10: 20m FT8 (14.074 + 125 MHz)  
            11: 132074000,  # SDR #11: 40m FT8 (7.074 + 125 MHz)
            12: 128573000   # SDR #12: 80m FT8 (3.573 + 125 MHz)
        }
        
        # Target FT8 frequencies (what we're actually receiving)
        self.ft8_target_frequencies = {
            9: 18100000,   # SDR #9: 17m
            10: 14074000,  # SDR #10: 20m  
            11: 7074000,   # SDR #11: 40m
            12: 3573000    # SDR #12: 80m
        }
        
        self.ft8_bands = {
            143100000: "17m",  # Upconverted frequency for 18.100 MHz
            139074000: "20m",  # Upconverted frequency for 14.074 MHz
            132074000: "40m",  # Upconverted frequency for 7.074 MHz 
            128573000: "80m"   # Upconverted frequency for 3.573 MHz
        }
        
        # FT8 log file patterns
        self.log_patterns = [
            "/tmp/ft8modem_*.log",
            "/var/log/ft8modem_*.log", 
            "/opt/ShackMate/ft8modem/logs/*.log",
            "~/ft8modem_*.log"
        ]
    
    async def connect_websocket(self):
        """Connect to WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.websocket_uri)
            print(f"✅ Connected to WebSocket server: {self.websocket_uri}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to WebSocket: {e}")
            return False
    
    def check_real_ft8_processes(self):
        """Check for actual running FT8 processes only"""
        try:
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            process_lines = result.stdout.split('\n')
            
            real_decoders = {}
            
            for sdr_id, frequency in self.sdr_frequencies.items():
                band = self.ft8_bands[frequency]
                websocket_port = 4200 + sdr_id - 9
                
                # Look for actual ft8modem process
                ft8_running = any(f':{websocket_port}' in line and 'ft8modem' in line 
                                for line in process_lines)
                
                # Only include if actually running
                if ft8_running:
                    real_decoders[sdr_id] = {
                        "band": band,
                        "frequency": frequency,
                        "status": "ACTIVE",
                        "websocket_port": websocket_port,
                        "last_check": datetime.utcnow().isoformat()
                    }
            
            return real_decoders
            
        except Exception as e:
            print(f"Error checking processes: {e}")
            return {}
    
    def find_ft8_log_files(self):
        """Find actual FT8 log files"""
        log_files = []
        
        for pattern in self.log_patterns:
            try:
                import glob
                expanded_pattern = os.path.expanduser(pattern)
                files = glob.glob(expanded_pattern)
                log_files.extend(files)
            except:
                continue
        
        # Also check for recent log files in common locations
        common_locations = ["/tmp", "/var/log", "/opt/ShackMate/ft8modem"]
        for location in common_locations:
            try:
                if os.path.exists(location):
                    for file in os.listdir(location):
                        if "ft8" in file.lower() and file.endswith('.log'):
                            log_files.append(os.path.join(location, file))
            except:
                continue
        
        return list(set(log_files))  # Remove duplicates
    
    def parse_ft8_decode_line(self, line, sdr_id=None, band=None):
        """Parse an FT8 decode line"""
        # FT8 decode format: HHMMSS SNR DT FREQ ~ MESSAGE
        # Example: 074500  -8  0.2 1247 ~ CQ W1ABC FN32
        
        ft8_pattern = r'(\d{6})\s+([+-]?\d+)\s+([+-]?\d+\.?\d*)\s+(\d+)\s+~?\s+(.+)'
        match = re.match(ft8_pattern, line.strip())
        
        if match:
            time_str, snr, dt, freq_offset, message = match.groups()
            
            return {
                "time": time_str,
                "snr": int(snr),
                "dt": float(dt),
                "frequency_offset": int(freq_offset),
                "message": message.strip(),
                "raw_line": line.strip(),
                "sdr_id": sdr_id,
                "band": band
            }
        
        return None
    
    async def monitor_log_file(self, log_file, sdr_id, band):
        """Monitor a specific log file for new decodes"""
        try:
            # Use tail -f to follow the log file
            process = await asyncio.create_subprocess_exec(
                'tail', '-f', log_file,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            print(f"📂 Monitoring {log_file} for SDR #{sdr_id} ({band})")
            
            while self.monitoring and process.stdout:
                line = await process.stdout.readline()
                if not line:
                    break
                    
                line_text = line.decode().strip()
                if not line_text:
                    continue
                
                # Parse the decode line
                decode = self.parse_ft8_decode_line(line_text, sdr_id, band)
                if decode:
                    # Send real decode message
                    decode_msg = {
                        "type": "system_status_broadcast",
                        "broadcast_type": "ft8_real_decode", 
                        "timestamp": datetime.utcnow().isoformat(),
                        "sdr_info": {
                            "sdr_id": sdr_id,
                            "frequency": self.sdr_frequencies[sdr_id],
                            "band": band,
                            "mode": "FT8"
                        },
                        "decode": decode,
                        "source": "real_log_file"
                    }
                    
                    if self.websocket:
                        await self.websocket.send(json.dumps(decode_msg))
                        print(f"📻 Real decode: SDR #{sdr_id} ({band}) - {decode['message']}")
            
            # Clean up process
            if process.returncode is None:
                process.terminate()
                await process.wait()
                
        except Exception as e:
            print(f"Error monitoring log file {log_file}: {e}")
    
    def create_status_update(self, real_decoders, log_files):
        """Create status update for real processes only"""
        return {
            "type": "system_status_broadcast",
            "broadcast_type": "ft8_real_status",
            "timestamp": datetime.utcnow().isoformat(),
            "monitoring": True,
            "summary": {
                "real_active_decoders": len(real_decoders),
                "bands_active": [info["band"] for info in real_decoders.values()],
                "log_files_found": len(log_files),
                "note": "Showing only real live decodes from actual FT8 processes"
            },
            "active_decoders": real_decoders,
            "log_files": log_files
        }
    
    async def start_real_monitoring(self, duration_minutes=10):
        """Start monitoring real FT8 processes and log files"""
        if not await self.connect_websocket():
            return
        
        self.monitoring = True
        
        # Send start message
        start_msg = {
            "type": "system_status_broadcast",
            "broadcast_type": "ft8_real_monitor_started",
            "message": f"🔄 Starting REAL FT8 monitoring for {duration_minutes} minutes (no simulated data)",
            "duration_minutes": duration_minutes
        }
        await self.websocket.send(json.dumps(start_msg))
        
        try:
            # Check for real processes
            real_decoders = self.check_real_ft8_processes()
            log_files = self.find_ft8_log_files()
            
            # Send status
            status_msg = self.create_status_update(real_decoders, log_files)
            await self.websocket.send(json.dumps(status_msg))
            
            if not real_decoders and not log_files:
                # No real processes or log files found
                no_data_msg = {
                    "type": "system_status_broadcast", 
                    "broadcast_type": "ft8_no_real_data",
                    "message": "⚠️  No active FT8 processes or log files found. Start FT8 decoders to see live decodes.",
                    "suggestion": "Start actual FT8 decoder processes to see real decodes"
                }
                await self.websocket.send(json.dumps(no_data_msg))
                
            else:
                print(f"📊 Found {len(real_decoders)} real decoders, {len(log_files)} log files")
                
                # Start monitoring tasks for each log file
                monitor_tasks = []
                for log_file in log_files:
                    # Try to associate log file with SDR (basic heuristic)
                    for sdr_id, info in real_decoders.items():
                        task = asyncio.create_task(
                            self.monitor_log_file(log_file, sdr_id, info["band"])
                        )
                        monitor_tasks.append(task)
                        break  # One task per log file for now
                
                # Wait for monitoring duration
                if monitor_tasks:
                    await asyncio.wait_for(
                        asyncio.gather(*monitor_tasks, return_exceptions=True),
                        timeout=duration_minutes * 60
                    )
                else:
                    # Just wait and check status periodically
                    for _ in range(duration_minutes * 4):  # Check every 15 seconds
                        await asyncio.sleep(15)
                        real_decoders = self.check_real_ft8_processes()
                        status_msg = self.create_status_update(real_decoders, log_files)
                        await self.websocket.send(json.dumps(status_msg))
            
        except asyncio.TimeoutError:
            print(f"⏰ Real monitoring completed after {duration_minutes} minutes")
        except Exception as e:
            print(f"❌ Error in real monitoring: {e}")
        finally:
            self.monitoring = False
            
            # Send completion message
            end_msg = {
                "type": "system_status_broadcast",
                "broadcast_type": "ft8_real_monitor_stopped",
                "message": "⏹️  Real FT8 monitoring stopped"
            }
            await self.websocket.send(json.dumps(end_msg))
            
            if self.websocket:
                await self.websocket.close()

async def main():
    monitor = RealFT8Monitor()
    await monitor.start_real_monitoring(duration_minutes=5)

if __name__ == "__main__":
    asyncio.run(main())
