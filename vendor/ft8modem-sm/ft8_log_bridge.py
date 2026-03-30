#!/usr/bin/env python3
"""
Simple FT8 Log to WebSocket Bridge
Reads ft8modem output from log file and sends JSON packets to WS:4010
"""

import asyncio
import websockets
import json
import sys
import signal
from datetime import datetime
import os
import time

class FT8LogBridge:
    def __init__(self, ws_url="ws://10.146.1.241:4010", sdr_id="SDR-10000003", log_file="/tmp/ft8modem_live.log"):
        self.ws_url = ws_url
        self.sdr_id = sdr_id
        self.log_file = log_file
        self.websocket = None
        self.running = False
        self.last_position = 0

    async def connect_websocket(self):
        """Connect to the main WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.ws_url)
            print(f"✅ Connected to WebSocket server: {self.ws_url}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to WebSocket: {e}")
            return False

    def read_new_lines(self):
        """Read new lines from log file since last read"""
        if not os.path.exists(self.log_file):
            return []
        
        try:
            with open(self.log_file, 'r') as f:
                f.seek(self.last_position)
                new_lines = f.readlines()
                self.last_position = f.tell()
                return [line.strip() for line in new_lines if line.strip()]
        except Exception as e:
            print(f"Error reading log file: {e}")
            return []

    def parse_ft8_line(self, line):
        """Parse FT8 decoder output and determine message type"""
        line = line.strip()
        if not line:
            return None
            
        # Skip non-decode messages
        skip_patterns = ["MODE:", "DEBUG:", "Starting", "Found"]
        if any(pattern in line for pattern in skip_patterns):
            return None
            
        # For INPUT: messages, create status update that will be broadcasted
        if "INPUT:" in line:
            return {
                "type": "sdr_status_update",
                "local_index": 0,  # Fake local index for FT8 decoder
                "status": {
                    "sdr_id": self.sdr_id,
                    "ft8_input_level": line.split("INPUT:")[-1].strip(),
                    "ft8_mode": "FT8",
                    "ft8_frequency": "14.074",
                    "timestamp": datetime.utcnow().isoformat(),
                    "decoder_status": "monitoring"
                }
            }
            
        # Check for actual FT8 decode - format as status update
        if any(char.isdigit() for char in line) and len(line) > 10:
            return {
                "type": "sdr_status_update", 
                "local_index": 0,  # Fake local index for FT8 decoder
                "status": {
                    "sdr_id": self.sdr_id,
                    "ft8_decode": line,
                    "ft8_mode": "FT8", 
                    "ft8_frequency": "14.074",
                    "timestamp": datetime.utcnow().isoformat(),
                    "decoder_status": "decoded"
                }
            }
        
        # General ft8modem status - format as status update
        return {
            "type": "sdr_status_update",
            "local_index": 0,  # Fake local index for FT8 decoder
            "status": {
                "sdr_id": self.sdr_id,
                "ft8_status": line,
                "ft8_mode": "FT8",
                "timestamp": datetime.utcnow().isoformat(),
                "decoder_status": "running"
            }
        }

    async def process_log_updates(self):
        """Monitor log file and send updates to WebSocket"""
        print(f"📡 Monitoring {self.log_file} for FT8 output...")
        
        while self.running:
            try:
                new_lines = self.read_new_lines()
                
                for line in new_lines:
                    message = self.parse_ft8_line(line)
                    if message and self.websocket:
                        try:
                            await self.websocket.send(json.dumps(message))
                            print(f"📤 Sent to WS:4010: {message['type']} - {line}")
                        except Exception as e:
                            print(f"❌ WebSocket send error: {e}")
                            break
                
                # Sleep briefly before checking for more updates
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"Error processing log updates: {e}")
                await asyncio.sleep(1)

    async def run(self):
        """Main bridge loop"""
        self.running = True
        
        try:
            # Connect to WebSocket server
            if not await self.connect_websocket():
                return
            
            # Send startup message
            startup_msg = {
                "type": "ft8_bridge_status",
                "sdr_id": self.sdr_id, 
                "timestamp": datetime.utcnow().isoformat(),
                "status": "FT8 bridge started",
                "log_file": self.log_file
            }
            await self.websocket.send(json.dumps(startup_msg))
            
            # Start monitoring log file
            await self.process_log_updates()
            
        except Exception as e:
            print(f"Bridge error: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up connections"""
        self.running = False
        
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"🛑 Received signal {signum}, shutting down...")
        self.running = False

async def main():
    if len(sys.argv) > 1:
        log_file = sys.argv[1]
    else:
        log_file = "/tmp/ft8modem_live.log"
        
    bridge = FT8LogBridge(log_file=log_file)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, bridge.signal_handler)
    signal.signal(signal.SIGTERM, bridge.signal_handler)
    
    print(f"🚀 Starting FT8 Log Bridge...")
    print(f"📁 Log file: {log_file}")
    print(f"🌐 WebSocket: ws://10.146.1.241:4010")
    
    await bridge.run()

if __name__ == "__main__":
    asyncio.run(main())
