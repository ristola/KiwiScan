#!/usr/bin/env python3
"""
FT8 WebSocket Bridge
Connects ft8modem output to existing WebSocket server on port 4010
"""

import asyncio
import websockets
import subprocess
import json
import sys
import signal
from datetime import datetime

class FT8WebSocketBridge:
    def __init__(self, ws_url="ws://10.146.1.241:4010", sdr_id="SDR-10000003"):
        self.ws_url = ws_url
        self.sdr_id = sdr_id
        self.ft8modem_process = None
        self.websocket = None
        self.running = False

    async def start_ft8modem(self):
        """Start ft8modem process and return stdout pipe"""
        cmd = [
            "/opt/ShackMate/ft8modem/ft8modem",
            "-e", self.sdr_id,
            "-r", "48000",
            "-j", "/usr/local/bin/jt9",
            "FT8",
            "udp:3100"
        ]
        
        print(f"Starting ft8modem: {' '.join(cmd)}")
        self.ft8modem_process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE  # Enable stdin for transmit commands
        )
        return self.ft8modem_process.stdout

    async def connect_websocket(self):
        """Connect to the main WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.ws_url)
            print(f"Connected to WebSocket server: {self.ws_url}")
            return True
        except Exception as e:
            print(f"Failed to connect to WebSocket: {e}")
            return False

    async def process_ft8_output(self, stdout):
        """Process ft8modem output and send to WebSocket"""
        while self.running:
            try:
                line = await asyncio.wait_for(stdout.readline(), timeout=1.0)
                if not line:
                    print("ft8modem process ended")
                    break
                    
                decoded_line = line.decode('utf-8').strip()
                if not decoded_line:
                    continue
                    
                print(f"FT8 Output: {decoded_line}")
                
                # Skip non-decode output (like MODE: FT8, debug messages)
                if any(skip in decoded_line for skip in ["MODE:", "DEBUG:", "INPUT:", "OK:"]):
                    continue
                
                # Create message for WebSocket server
                message = {
                    "type": "ft8_decode",
                    "sdr_id": self.sdr_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "frequency": "14.074",
                    "mode": "FT8",
                    "data": decoded_line
                }
                
                # Send to WebSocket server
                if self.websocket:
                    try:
                        await self.websocket.send(json.dumps(message))
                        print(f"Sent to WS:4010: {decoded_line}")
                    except Exception as e:
                        print(f"WebSocket send error: {e}")
                        break
                        
            except asyncio.TimeoutError:
                # Timeout is normal, just continue
                continue
            except Exception as e:
                print(f"Error processing output: {e}")
                break

    async def process_websocket_messages(self):
        """Process incoming WebSocket messages for transmission"""
        while self.running and self.websocket:
            try:
                message = await asyncio.wait_for(self.websocket.recv(), timeout=1.0)
                data = json.loads(message)
                
                # Check if this is a transmit request for our SDR
                if (data.get("type") == "ft8_transmit" and 
                    data.get("sdr_id") == self.sdr_id):
                    
                    text_to_send = data.get("text", "").strip()
                    if text_to_send:
                        print(f"Transmit request: {text_to_send}")
                        await self.send_to_ft8modem(text_to_send)
                        
            except asyncio.TimeoutError:
                # Timeout is normal, just continue
                continue
            except websockets.exceptions.ConnectionClosed:
                print("WebSocket connection closed")
                break
            except json.JSONDecodeError:
                print("Invalid JSON received from WebSocket")
                continue
            except Exception as e:
                print(f"Error processing WebSocket message: {e}")
                break

    async def send_to_ft8modem(self, text):
        """Send text to ft8modem for transmission"""
        if self.ft8modem_process and self.ft8modem_process.stdin:
            try:
                # ft8modem expects commands on stdin
                command = f"TX {text}\n"
                self.ft8modem_process.stdin.write(command.encode())
                await self.ft8modem_process.stdin.drain()
                print(f"Sent to ft8modem: {command.strip()}")
            except Exception as e:
                print(f"Error sending to ft8modem: {e}")

    async def run(self):
        """Main bridge loop"""
        self.running = True
        
        try:
            # Connect to WebSocket server
            if not await self.connect_websocket():
                return
            
            # Start ft8modem
            stdout = await self.start_ft8modem()
            
            # Run both output processing and WebSocket message processing concurrently
            await asyncio.gather(
                self.process_ft8_output(stdout),
                self.process_websocket_messages()
            )
            
        except Exception as e:
            print(f"Bridge error: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up processes and connections"""
        self.running = False
        
        if self.websocket:
            try:
                await self.websocket.close()
            except:
                pass
            
        if self.ft8modem_process:
            try:
                if self.ft8modem_process.returncode is None:
                    self.ft8modem_process.terminate()
                    await self.ft8modem_process.wait()
            except:
                pass

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"Received signal {signum}, shutting down...")
        self.running = False

async def main():
    bridge = FT8WebSocketBridge()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, bridge.signal_handler)
    signal.signal(signal.SIGTERM, bridge.signal_handler)
    
    await bridge.run()

if __name__ == "__main__":
    asyncio.run(main())
