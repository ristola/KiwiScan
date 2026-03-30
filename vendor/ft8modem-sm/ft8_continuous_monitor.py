#!/usr/bin/env python3
"""
Continuous FT8 Live Monitor
Monitors FT8 decoder processes and sends real-time updates
"""

import asyncio
import websockets
import json
import time
import subprocess
from datetime import datetime
from pathlib import Path

class ContinuousFT8Monitor:
    def __init__(self):
        self.websocket_uri = "ws://10.146.1.241:4010"
        self.websocket = None
        self.monitoring = False
        self.update_interval = 15  # seconds
        
        # FT8 SDR configuration
        self.sdr_frequencies = {
            9: 18100000,   # 17m
            10: 14074000,  # 20m  
            11: 7074000,   # 40m
            12: 3573000    # 80m
        }
        
        self.ft8_bands = {
            18100000: "17m",
            14074000: "20m", 
            7074000: "40m",
            3573000: "80m"
        }
    
    async def connect_websocket(self):
        """Connect to WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.websocket_uri)
            print(f"✅ Connected to WebSocket server: {self.websocket_uri}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to WebSocket: {e}")
            return False
    
    def check_ft8_processes(self):
        """Check which FT8 processes are running"""
        try:
            # Check for ft8modem processes
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            process_lines = result.stdout.split('\n')
            
            active_decoders = {}
            
            for sdr_id, frequency in self.sdr_frequencies.items():
                band = self.ft8_bands[frequency]
                websocket_port = 4200 + sdr_id - 9
                
                # Look for ft8modem process for this SDR
                ft8_running = any(f':{websocket_port}' in line and 'ft8modem' in line 
                                for line in process_lines)
                
                # For demo purposes, show all SDRs as active with simulated data
                # Remove this condition if you want only real process detection
                if True:  # Changed from: if ft8_running:
                    status = "ACTIVE" if ft8_running else "SIMULATED"
                    active_decoders[sdr_id] = {
                        "band": band,
                        "frequency": frequency,
                        "status": status,
                        "real_process": ft8_running,
                        "last_check": datetime.utcnow().isoformat()
                    }
            
            return active_decoders
            
        except Exception as e:
            print(f"Error checking processes: {e}")
            # Return simulated data even on error
            active_decoders = {}
            for sdr_id, frequency in self.sdr_frequencies.items():
                band = self.ft8_bands[frequency]
                active_decoders[sdr_id] = {
                    "band": band,
                    "frequency": frequency,
                    "status": "SIMULATED",
                    "real_process": False,
                    "last_check": datetime.utcnow().isoformat()
                }
            return active_decoders
    
    def create_status_update(self, active_decoders):
        """Create status update message"""
        real_decoders = sum(1 for info in active_decoders.values() if info.get("real_process", False))
        simulated_decoders = len(active_decoders) - real_decoders
        
        return {
            "type": "system_status_broadcast",
            "broadcast_type": "ft8_continuous_status",
            "timestamp": datetime.utcnow().isoformat(),
            "monitoring": True,
            "update_interval": self.update_interval,
            "summary": {
                "total_decoders": len(active_decoders),
                "real_active_decoders": real_decoders,
                "simulated_decoders": simulated_decoders,
                "bands_active": [info["band"] for info in active_decoders.values()],
                "note": "Showing simulated decodes for demo - real processes not required"
            },
            "active_decoders": active_decoders
        }
    
    def create_simulated_decode(self, sdr_id, band, frequency):
        """Create a simulated real-time decode message"""
        current_time = datetime.utcnow()
        time_str = current_time.strftime("%H%M%S")
        
        # Realistic FT8 messages for different bands
        messages_by_band = {
            "17m": ["CQ VK3ABC QF22", "W1XYZ K2ABC FN20", "JA1DEF VK3ABC R-12"],
            "20m": ["CQ DX EA1ABC IN70", "VE3XYZ W5ABC EM12", "UA9DEF EA1ABC R+05"], 
            "40m": ["CQ G0ABC IO91", "VK2XYZ G0ABC QF56", "W6DEF G0ABC R-08"],
            "80m": ["CQ W9ABC EN37", "JA7XYZ W9ABC PM95", "VE1DEF W9ABC R+12"]
        }
        
        import random
        message = random.choice(messages_by_band.get(band, ["CQ TEST ABC123"]))
        snr = random.randint(-20, 5)
        dt = round(random.uniform(-2.0, 2.0), 1)
        freq_offset = random.randint(500, 2500)
        
        return {
            "type": "system_status_broadcast", 
            "broadcast_type": "ft8_live_decode",
            "timestamp": current_time.isoformat(),
            "sdr_info": {
                "sdr_id": sdr_id,
                "frequency": frequency,
                "band": band,
                "mode": "FT8"
            },
            "decode": {
                "time": time_str,
                "snr": snr,
                "dt": dt,
                "frequency_offset": freq_offset,
                "message": message,
                "raw_line": f"{time_str} {snr:+3d} {dt:4.1f} {freq_offset:4d} ~  {message}"
            }
        }
    
    async def send_continuous_updates(self):
        """Send continuous FT8 status and decode updates"""
        print(f"🔄 Starting continuous FT8 monitoring (every {self.update_interval}s)")
        
        decode_counter = 0
        
        while self.monitoring and self.websocket:
            try:
                # Check active decoders
                active_decoders = self.check_ft8_processes()
                
                # Send status update
                status_msg = self.create_status_update(active_decoders)
                await self.websocket.send(json.dumps(status_msg))
                print(f"📊 Status update sent - {len(active_decoders)} active decoders")
                
                # Send simulated decodes for active decoders
                for sdr_id, info in active_decoders.items():
                    # Send 1-3 random decodes per active SDR
                    import random
                    decode_count = random.randint(1, 3)
                    
                    for _ in range(decode_count):
                        decode_msg = self.create_simulated_decode(
                            sdr_id, info["band"], info["frequency"]
                        )
                        await self.websocket.send(json.dumps(decode_msg))
                        decode_counter += 1
                        await asyncio.sleep(0.5)  # Small delay between decodes
                
                print(f"📻 Sent {decode_counter} total decodes so far")
                
                # Wait for next update cycle
                await asyncio.sleep(self.update_interval)
                
            except Exception as e:
                print(f"❌ Error in continuous monitoring: {e}")
                break
    
    async def start_monitoring(self, duration_minutes=10):
        """Start continuous monitoring for specified duration"""
        if await self.connect_websocket():
            self.monitoring = True
            
            # Send initial message
            start_msg = {
                "type": "system_status_broadcast",
                "broadcast_type": "ft8_monitor_started", 
                "message": f"🔄 Starting continuous FT8 monitoring for {duration_minutes} minutes",
                "duration_minutes": duration_minutes,
                "update_interval": self.update_interval
            }
            await self.websocket.send(json.dumps(start_msg))
            
            try:
                # Run monitoring for specified duration
                await asyncio.wait_for(
                    self.send_continuous_updates(), 
                    timeout=duration_minutes * 60
                )
            except asyncio.TimeoutError:
                print(f"⏰ Monitoring completed after {duration_minutes} minutes")
            
            # Send completion message
            end_msg = {
                "type": "system_status_broadcast",
                "broadcast_type": "ft8_monitor_stopped",
                "message": "⏹️  Continuous FT8 monitoring stopped"
            }
            await self.websocket.send(json.dumps(end_msg))
            
            await self.websocket.close()

async def main():
    monitor = ContinuousFT8Monitor()
    
    # Start monitoring for 5 minutes (adjust as needed)
    await monitor.start_monitoring(duration_minutes=5)

if __name__ == "__main__":
    asyncio.run(main())
