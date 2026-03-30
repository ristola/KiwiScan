#!/usr/bin/env python3
"""
FT8 Decoder Status and Live Message Monitor
Monitors all FT8/FT4 SDRs and their decoder processes
"""

import asyncio
import websockets
import json
import subprocess
import psutil
import time
import re
from datetime import datetime, timezone
from pathlib import Path

class FT8DecoderMonitor:
    def __init__(self):
        self.websocket_uri = "ws://10.146.1.241:4010"
        self.websocket = None
        
        # Band mapping for FT8 frequencies
        self.ft8_bands = {
            1840000: "160m",
            3573000: "80m", 
            7074000: "40m",
            10136000: "30m",
            14074000: "20m",
            18100000: "17m",
            21074000: "15m",
            24915000: "12m",
            28074000: "10m"
        }
        
        # SDR to frequency mapping (from your system)
        self.sdr_frequencies = {
            9: 18100000,   # SDR #9: 17m
            10: 14074000,  # SDR #10: 20m  
            11: 7074000,   # SDR #11: 40m
            12: 3573000    # SDR #12: 80m
        }
    
    def get_band_from_frequency(self, freq_hz):
        """Get amateur radio band from frequency"""
        return self.ft8_bands.get(freq_hz, f"{freq_hz/1e6:.3f}MHz")
    
    def get_process_info(self, process_name):
        """Get information about running processes"""
        processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent']):
            try:
                if process_name.lower() in proc.info['name'].lower():
                    processes.append({
                        'pid': proc.info['pid'],
                        'name': proc.info['name'],
                        'command': ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else '',
                        'cpu': round(proc.info['cpu_percent'], 1)
                    })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return processes
    
    def check_port_status(self, port):
        """Check if a network port is in use"""
        try:
            result = subprocess.run(['lsof', '-i', f':{port}'], 
                                  capture_output=True, text=True, timeout=5)
            return "LISTENING" if result.stdout else "CLOSED"
        except:
            return "UNKNOWN"
    
    def get_sdr_decoder_status(self):
        """Get comprehensive status of all FT8 SDR decoders"""
        status = {
            "type": "system_status_broadcast",
            "broadcast_type": "ft8_decoder_status", 
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_ft8_sdrs": len(self.sdr_frequencies),
                "active_decoders": 0,
                "bands_active": []
            },
            "sdr_decoders": {},
            "process_status": {
                "rtl_fm": {"running": 0, "processes": []},
                "af2udp": {"running": 0, "processes": []}, 
                "ft8modem": {"running": 0, "processes": []}
            },
            "network_ports": {}
        }
        
        # Check process status
        rtl_processes = self.get_process_info('rtl_fm')
        af2udp_processes = self.get_process_info('af2udp')
        ft8modem_processes = self.get_process_info('ft8modem')
        
        status["process_status"]["rtl_fm"]["running"] = len(rtl_processes)
        status["process_status"]["rtl_fm"]["processes"] = rtl_processes
        status["process_status"]["af2udp"]["running"] = len(af2udp_processes)
        status["process_status"]["af2udp"]["processes"] = af2udp_processes
        status["process_status"]["ft8modem"]["running"] = len(ft8modem_processes)
        status["process_status"]["ft8modem"]["processes"] = ft8modem_processes
        
        # Check each FT8 SDR
        for sdr_id, frequency in self.sdr_frequencies.items():
            band = self.get_band_from_frequency(frequency)
            
            # Check if processes are running for this SDR
            rtl_running = any(str(frequency) in proc['command'] for proc in rtl_processes)
            af2udp_port = 3100 + sdr_id - 9  # Base port 3100
            websocket_port = 4200 + sdr_id - 9  # Base port 4200
            
            af2udp_running = any(str(af2udp_port) in proc['command'] for proc in af2udp_processes)
            ft8_running = any(str(websocket_port) in proc['command'] for proc in ft8modem_processes)
            
            decoder_active = rtl_running and af2udp_running and ft8_running
            if decoder_active:
                status["summary"]["active_decoders"] += 1
                status["summary"]["bands_active"].append(band)
            
            status["sdr_decoders"][f"SDR_{sdr_id}"] = {
                "sdr_id": sdr_id,
                "frequency": frequency,
                "band": band,
                "status": "ACTIVE" if decoder_active else "INACTIVE",
                "processes": {
                    "rtl_fm": "RUNNING" if rtl_running else "STOPPED",
                    "af2udp": f"RUNNING (UDP:{af2udp_port})" if af2udp_running else "STOPPED",
                    "ft8modem": f"RUNNING (WS:{websocket_port})" if ft8_running else "STOPPED"
                },
                "ports": {
                    "udp_audio": af2udp_port,
                    "websocket": websocket_port,
                    "udp_status": self.check_port_status(af2udp_port),
                    "websocket_status": self.check_port_status(websocket_port)
                }
            }
        
        return status
    
    def create_sample_decode_message(self, sdr_id=11, band="40m"):
        """Create a sample FT8 decode message for testing"""
        frequency = self.sdr_frequencies.get(sdr_id, 7074000)
        current_time = datetime.utcnow()
        
        # Sample FT8 decode messages (realistic format)
        sample_decodes = [
            "074500  -8  0.2 1247 ~  CQ W1ABC FN32",
            "074515 -12  0.1 1523 ~  W1ABC K1XYZ FN32", 
            "074530  -6  0.3  987 ~  K1XYZ W1ABC R-15",
            "074545 -10  0.2 1247 ~  W1ABC K1XYZ RR73",
            "074600  -4  0.1 1523 ~  K1XYZ W1ABC 73",
            "074615  -9  0.2 2134 ~  CQ DX JA1ABC PM95",
            "074630 -15  0.1  876 ~  JA1ABC VK2XYZ QM78",
            "074645  -7  0.3 1456 ~  VK2XYZ JA1ABC R-08"
        ]
        
        message = {
            "type": "system_status_broadcast",
            "broadcast_type": "ft8_live_decodes",
            "timestamp": current_time.isoformat(),
            "sdr_info": {
                "sdr_id": sdr_id,
                "frequency": frequency,
                "band": band,
                "mode": "FT8"
            },
            "decode_session": {
                "start_time": current_time.isoformat(),
                "decode_count": len(sample_decodes),
                "band_activity": "HIGH"
            },
            "decodes": []
        }
        
        # Process each decode line
        for i, decode_line in enumerate(sample_decodes):
            # Parse FT8 decode format: HHMMSS SNR DT FREQ ~ MESSAGE
            parts = decode_line.split()
            if len(parts) >= 6:
                decode_time = parts[0]
                snr = parts[1]
                dt = parts[2] 
                freq_offset = parts[3]
                message_text = ' '.join(parts[5:])  # Skip the '~'
                
                decode_entry = {
                    "time": decode_time,
                    "snr": int(snr),
                    "dt": float(dt),
                    "frequency_offset": int(freq_offset),
                    "message": message_text,
                    "decode_index": i + 1
                }
                message["decodes"].append(decode_entry)
        
        return message
    
    def format_decoder_status_text(self, status):
        """Format decoder status as readable text"""
        lines = []
        lines.append("=== FT8 DECODER SYSTEM STATUS ===")
        lines.append("")
        
        # Summary
        summary = status["summary"]
        lines.append(f"📊 OVERVIEW:")
        lines.append(f"   Total FT8 SDRs: {summary['total_ft8_sdrs']}")
        lines.append(f"   Active Decoders: {summary['active_decoders']}")
        lines.append(f"   Bands Active: {', '.join(summary['bands_active']) if summary['bands_active'] else 'None'}")
        lines.append("")
        
        # Process status
        proc_status = status["process_status"]
        lines.append("🔧 PROCESS STATUS:")
        lines.append(f"   RTL_FM: {proc_status['rtl_fm']['running']} running")
        lines.append(f"   AF2UDP: {proc_status['af2udp']['running']} running") 
        lines.append(f"   FT8MODEM: {proc_status['ft8modem']['running']} running")
        lines.append("")
        
        # Individual SDR status
        lines.append("📻 FT8 SDR DECODERS:")
        for sdr_key, sdr_info in status["sdr_decoders"].items():
            lines.append(f"   • SDR #{sdr_info['sdr_id']} ({sdr_info['band']}) - {sdr_info['status']}")
            lines.append(f"     Frequency: {sdr_info['frequency']/1e6:.3f} MHz")
            lines.append(f"     RTL_FM: {sdr_info['processes']['rtl_fm']}")
            lines.append(f"     AF2UDP: {sdr_info['processes']['af2udp']}")
            lines.append(f"     FT8MODEM: {sdr_info['processes']['ft8modem']}")
            lines.append("")
        
        lines.append("💡 Use 'ft8' command to get live decode messages")
        
        return "\n".join(lines)
    
    def format_decode_message_text(self, decode_msg):
        """Format decode message as readable text"""
        lines = []
        sdr_info = decode_msg["sdr_info"]
        session = decode_msg["decode_session"]
        
        lines.append(f"=== LIVE FT8 DECODES: SDR #{sdr_info['sdr_id']} ({sdr_info['band']}) ===")
        lines.append(f"Frequency: {sdr_info['frequency']/1e6:.3f} MHz")
        lines.append(f"Activity Level: {session['band_activity']}")
        lines.append(f"Decodes: {session['decode_count']} messages")
        lines.append("")
        lines.append("TIME    SNR  DT  FREQ  MESSAGE")
        lines.append("-" * 50)
        
        for decode in decode_msg["decodes"]:
            lines.append(f"{decode['time']} {decode['snr']:+3d} {decode['dt']:4.1f} {decode['frequency_offset']:4d}  {decode['message']}")
        
        lines.append("")
        lines.append("🔄 Live decodes update automatically")
        
        return "\n".join(lines)
    
    async def connect_websocket(self):
        """Connect to WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.websocket_uri)
            print(f"✅ Connected to WebSocket server: {self.websocket_uri}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to WebSocket: {e}")
            return False
    
    async def send_decoder_status(self):
        """Send comprehensive decoder status"""
        try:
            status = self.get_sdr_decoder_status()
            
            if self.websocket:
                await self.websocket.send(json.dumps(status))
                print("✅ FT8 decoder status sent to WebSocket")
                return True
        except Exception as e:
            print(f"❌ Error sending decoder status: {e}")
            return False
    
    async def send_sample_decodes(self):
        """Send sample decode messages for all active bands"""
        try:
            # Send decodes for each FT8 SDR
            for sdr_id, frequency in self.sdr_frequencies.items():
                band = self.get_band_from_frequency(frequency)
                decode_msg = self.create_sample_decode_message(sdr_id, band)
                
                if self.websocket:
                    await self.websocket.send(json.dumps(decode_msg))
                    print(f"✅ Sample decodes sent for SDR #{sdr_id} ({band})")
                    await asyncio.sleep(0.5)  # Small delay between messages
                    
        except Exception as e:
            print(f"❌ Error sending sample decodes: {e}")
    
    async def run_status_check(self):
        """Run status check and send updates"""
        try:
            if await self.connect_websocket():
                await self.send_decoder_status()
                await asyncio.sleep(1)
                await self.send_sample_decodes()
                await self.websocket.close()
        except Exception as e:
            print(f"Error: {e}")

async def main():
    monitor = FT8DecoderMonitor()
    await monitor.run_status_check()

if __name__ == "__main__":
    asyncio.run(main())
