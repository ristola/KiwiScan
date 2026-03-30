#!/usr/bin/env python3
"""
FT8 Real Bridge - Capture actual ft8modem output and send to WebSocket server
This bridges the gap between the running ft8modem processes and the WebSocket broadcast
"""

import asyncio
import subprocess
import json
import websockets
import re
from datetime import datetime
import signal
import sys

class FT8RealBridge:
    def __init__(self):
        self.server_url = "ws://10.146.1.241:4010"
        self.websocket = None
        self.running = False
        self.ft8_processes = []
        
    async def connect_to_server(self):
        """Connect to the WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.server_url)
            print("✅ Connected to WebSocket server")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to server: {e}")
            return False
    
    def find_ft8_processes(self):
        """Find all running ft8modem processes and their output files"""
        try:
            result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
            processes = []
            
            for line in result.stdout.split('\n'):
                if '/usr/local/bin/ft8modem' in line and 'udp:' in line:
                    # Extract details from the command line
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if 'udp:' in part and i + 1 < len(parts):
                            try:
                                port = int(parts[i + 1])
                                sdr_number = f"sdr{port - 3100}"  # 3101->sdr1, etc.
                                
                                # Find the frequency in the command line
                                frequency = 14074000  # Default FT8 frequency
                                for j, p in enumerate(parts):
                                    if p.isdigit() and len(p) >= 8:  # Look for frequency
                                        freq_candidate = int(p)
                                        if 100000000 <= freq_candidate <= 200000000:  # Valid upconverted freq
                                            frequency = freq_candidate
                                            break
                                
                                processes.append({
                                    'port': port,
                                    'sdr_number': sdr_number,
                                    'frequency': frequency,
                                    'command_line': line.strip()
                                })
                            except ValueError:
                                pass
            
            return processes
        except Exception as e:
            print(f"Error finding ft8modem processes: {e}")
            return []
    
    def parse_ft8_decode(self, line, sdr_info):
        """Parse ft8modem output line into structured data"""
        try:
            # ft8modem output format: "HHMMSS  SNR  DT FREQ @  MESSAGE"
            # Example: "120000  -12  0.2 1234 @  CQ DX K1ABC FN20"
            
            line = line.strip()
            if not line or len(line) < 20:
                return None
            
            # Use regex to parse the format
            pattern = r'^(\d{6})\s+([-+]?\d+)\s+([-+]?\d+\.?\d*)\s+(\d+)\s*[@~]?\s*(.+)'
            match = re.match(pattern, line)
            
            if match:
                time_str, snr, dt, freq_offset, message = match.groups()
                
                # Calculate actual frequency
                freq_offset_hz = int(freq_offset)
                base_freq = 14074000  # Base FT8 frequency
                actual_freq = base_freq + freq_offset_hz + 125000000  # Add upconverter offset
                
                # Clean up message
                message = message.strip()
                
                return {
                    "type": "ft8_decode",
                    "sdr_number": sdr_info['sdr_number'],
                    "frequency": actual_freq,
                    "message": message,
                    "timestamp": int(datetime.now().timestamp()),
                    "snr": int(snr),
                    "dt": float(dt),
                    "time": time_str
                }
            
        except Exception as e:
            print(f"Error parsing FT8 line '{line}': {e}")
        
        return None
    
    async def monitor_ft8_process(self, sdr_info):
        """Monitor a single ft8modem process via TCP connection"""
        port = sdr_info['port']
        
        try:
            # Connect to the ft8modem TCP output
            reader, writer = await asyncio.open_connection('localhost', port + 1000)
            print(f"📡 Connected to {sdr_info['sdr_number']} on TCP port {port + 1000}")
            
            while self.running:
                try:
                    data = await asyncio.wait_for(reader.readline(), timeout=5.0)
                    if not data:
                        break
                        
                    line = data.decode('utf-8', errors='ignore').strip()
                    if line:
                        print(f"📥 {sdr_info['sdr_number']}: {line}")
                        
                        # Parse and send to WebSocket
                        ft8_data = self.parse_ft8_decode(line, sdr_info)
                        if ft8_data:
                            await self.send_ft8_decode(ft8_data)
                            
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"Error reading from {sdr_info['sdr_number']}: {e}")
                    break
            
            writer.close()
            await writer.wait_closed()
            
        except Exception as e:
            print(f"Failed to connect to {sdr_info['sdr_number']} TCP port {port + 1000}: {e}")
    
    async def monitor_ft8_via_netcat(self, sdr_info):
        """Alternative: Monitor ft8modem using netcat to UDP port"""
        port = sdr_info['port']
        
        try:
            # Use netcat to listen to UDP output
            process = await asyncio.create_subprocess_exec(
                'nc', '-u', '-l', str(port + 100),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            print(f"📡 Listening to {sdr_info['sdr_number']} via netcat on UDP {port + 100}")
            
            while self.running:
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), timeout=5.0)
                    if not line:
                        break
                        
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        print(f"📥 {sdr_info['sdr_number']}: {decoded}")
                        
                        # Parse and send to WebSocket
                        ft8_data = self.parse_ft8_decode(decoded, sdr_info)
                        if ft8_data:
                            await self.send_ft8_decode(ft8_data)
                            
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"Error reading from {sdr_info['sdr_number']}: {e}")
                    break
            
            process.terminate()
            await process.wait()
            
        except Exception as e:
            print(f"Failed to monitor {sdr_info['sdr_number']} via netcat: {e}")
    
    async def send_ft8_decode(self, ft8_data):
        """Send FT8 decode to WebSocket server"""
        if self.websocket:
            try:
                message = json.dumps(ft8_data)
                await self.websocket.send(message)
                print(f"📤 Sent to WS: {ft8_data['sdr_number']} {ft8_data['frequency']} Hz - {ft8_data['message']}")
            except Exception as e:
                print(f"Error sending to WebSocket: {e}")
                # Try to reconnect
                if await self.connect_to_server():
                    try:
                        await self.websocket.send(message)
                        print(f"📤 Resent: {ft8_data['sdr_number']} {ft8_data['frequency']} Hz - {ft8_data['message']}")
                    except:
                        pass
    
    async def run_bridge(self):
        """Main bridge function"""
        print("🚀 Starting FT8 Real Bridge")
        
        # Find ft8modem processes
        self.ft8_processes = self.find_ft8_processes()
        if not self.ft8_processes:
            print("❌ No ft8modem processes found")
            return
        
        print(f"📡 Found {len(self.ft8_processes)} ft8modem processes:")
        for proc in self.ft8_processes:
            print(f"  {proc['sdr_number']}: Port {proc['port']}, Freq ~{proc['frequency']} Hz")
        
        # Connect to WebSocket server
        if not await self.connect_to_server():
            return
        
        # Start monitoring tasks
        self.running = True
        monitor_tasks = []
        
        for sdr_info in self.ft8_processes:
            # Try TCP connection first, fallback to netcat
            task = asyncio.create_task(self.monitor_ft8_via_netcat(sdr_info))
            monitor_tasks.append(task)
        
        print(f"🎯 Monitoring {len(monitor_tasks)} FT8 streams...")
        print("Press Ctrl+C to stop")
        
        try:
            # Wait for all monitoring tasks
            await asyncio.gather(*monitor_tasks)
        except KeyboardInterrupt:
            print("\n🛑 Stopping bridge...")
        finally:
            self.running = False
            if self.websocket:
                await self.websocket.close()

def signal_handler(signum, frame):
    print("\n🛑 Received signal, stopping...")
    sys.exit(0)

async def main():
    # Set up signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    bridge = FT8RealBridge()
    await bridge.run_bridge()

if __name__ == "__main__":
    asyncio.run(main())
