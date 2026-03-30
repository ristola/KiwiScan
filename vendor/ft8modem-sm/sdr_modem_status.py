#!/usr/bin/env python3
"""
SDR Modem Status Display
Sends a comprehensive status display to WebSocket clients showing SDR modem decoder information
"""

import asyncio
import websockets
import json
import subprocess
import psutil
from datetime import datetime
import os

class SDRModemStatus:
    def __init__(self, ws_url="ws://10.146.1.241:4010"):
        self.ws_url = ws_url
        self.websocket = None

    async def connect_websocket(self):
        """Connect to the WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.ws_url)
            print(f"✅ Connected to WebSocket server: {self.ws_url}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to WebSocket: {e}")
            return False

    def get_process_info(self, process_name):
        """Get information about running processes using ps command"""
        processes = []
        try:
            # Use ps command which works reliably on macOS
            result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                for line in lines[1:]:  # Skip header
                    if process_name.lower() in line.lower():
                        parts = line.split()
                        if len(parts) >= 11:
                            pid = parts[1]
                            cpu = parts[2]
                            mem = parts[3]
                            command = ' '.join(parts[10:])
                            processes.append({
                                'pid': pid,
                                'cpu': cpu,
                                'mem': mem,
                                'command': command
                            })
        except Exception as e:
            print(f"Error getting process info for {process_name}: {e}")
        return processes

    def get_sdr_modem_status(self):
        """Get comprehensive SDR modem decoder status"""
        status = {
            "timestamp": datetime.utcnow().isoformat(),
            "system_info": {
                "host": "iMac-Pro.local",
                "ip": "10.146.1.241"
            },
            "sdr_processes": {},
            "modem_decoders": {},
            "audio_pipeline": {},
            "websocket_services": {}
        }

        # Check RTL-SDR processes
        rtl_processes = self.get_process_info('rtl_fm')
        status["sdr_processes"]["rtl_fm"] = {
            "count": len(rtl_processes),
            "status": "RUNNING" if rtl_processes else "STOPPED",
            "processes": rtl_processes
        }

        # Check af2udp processes  
        af2udp_processes = self.get_process_info('af2udp')
        status["audio_pipeline"]["af2udp"] = {
            "count": len(af2udp_processes),
            "status": "RUNNING" if af2udp_processes else "STOPPED", 
            "processes": af2udp_processes
        }

        # Check ft8modem processes
        ft8modem_processes = self.get_process_info('ft8modem')
        status["modem_decoders"]["ft8modem"] = {
            "count": len(ft8modem_processes),
            "status": "RUNNING" if ft8modem_processes else "STOPPED",
            "processes": ft8modem_processes
        }

        # Check websocketd processes
        websocketd_processes = self.get_process_info('websocketd')
        status["websocket_services"]["websocketd"] = {
            "count": len(websocketd_processes),
            "status": "RUNNING" if websocketd_processes else "STOPPED",
            "processes": websocketd_processes
        }

        # Check bridge processes
        bridge_processes = self.get_process_info('ft8_log_bridge')
        status["websocket_services"]["ft8_bridge"] = {
            "count": len(bridge_processes),
            "status": "RUNNING" if bridge_processes else "STOPPED",
            "processes": bridge_processes
        }

        # Check network ports
        try:
            result = subprocess.run(['netstat', '-an'], capture_output=True, text=True)
            netstat_output = result.stdout
            
            # Check specific ports
            ports_to_check = [3100, 3101, 3102, 3103, 4200, 4201, 4202, 4203, 4010]
            port_status = {}
            
            for port in ports_to_check:
                if f"*.{port}" in netstat_output or f":{port}" in netstat_output:
                    port_status[port] = "LISTENING"
                else:
                    port_status[port] = "CLOSED"
            
            status["network_ports"] = port_status
            
        except Exception as e:
            status["network_ports"] = {"error": str(e)}

        return status

    def format_status_display(self, status):
        """Format status into a readable display"""
        lines = []
        lines.append("=" * 80)
        lines.append("🎯 SDR MODEM DECODER STATUS DISPLAY")
        lines.append("=" * 80)
        lines.append(f"📅 Timestamp: {status['timestamp']}")
        lines.append(f"🏠 Host: {status['system_info']['host']} ({status['system_info']['ip']})")
        lines.append("")

        # SDR Receivers
        lines.append("📡 SDR RECEIVERS:")
        rtl_status = status["sdr_processes"]["rtl_fm"]
        lines.append(f"   RTL-SDR (rtl_fm): {rtl_status['status']} ({rtl_status['count']} processes)")
        for proc in rtl_status["processes"]:
            lines.append(f"      PID {proc['pid']}: {proc['command'][:70]}... (CPU: {proc['cpu']}%)")

        lines.append("")

        # Audio Pipeline
        lines.append("🔊 AUDIO PIPELINE:")
        af2udp_status = status["audio_pipeline"]["af2udp"]
        lines.append(f"   Audio Forwarder (af2udp): {af2udp_status['status']} ({af2udp_status['count']} processes)")
        for proc in af2udp_status["processes"]:
            lines.append(f"      PID {proc['pid']}: {proc['command']} (CPU: {proc['cpu']}%)")

        lines.append("")

        # Modem Decoders
        lines.append("📻 MODEM DECODERS:")
        ft8_status = status["modem_decoders"]["ft8modem"]
        lines.append(f"   FT8 Decoder (ft8modem): {ft8_status['status']} ({ft8_status['count']} processes)")
        for proc in ft8_status["processes"]:
            lines.append(f"      PID {proc['pid']}: {proc['command'][:70]}... (CPU: {proc['cpu']}%)")

        lines.append("")

        # WebSocket Services
        lines.append("🌐 WEBSOCKET SERVICES:")
        ws_status = status["websocket_services"]["websocketd"] 
        bridge_status = status["websocket_services"]["ft8_bridge"]
        lines.append(f"   WebSocket Daemon: {ws_status['status']} ({ws_status['count']} processes)")
        lines.append(f"   FT8 Bridge: {bridge_status['status']} ({bridge_status['count']} processes)")

        lines.append("")

        # Network Ports
        lines.append("🔌 NETWORK PORTS:")
        ports = status["network_ports"]
        if "error" not in ports:
            lines.append("   UDP Audio Ports:")
            for port in [3100, 3101, 3102, 3103]:
                lines.append(f"      {port}: {ports.get(port, 'UNKNOWN')}")
            lines.append("   WebSocket Ports:")
            for port in [4200, 4201, 4202, 4203]:
                lines.append(f"      {port}: {ports.get(port, 'UNKNOWN')}")
            lines.append(f"   Main WebSocket: 4010: {ports.get(4010, 'UNKNOWN')}")
        else:
            lines.append(f"   Error checking ports: {ports['error']}")

        lines.append("")

        # Overall Status Summary
        lines.append("📊 OVERALL STATUS SUMMARY:")
        total_processes = (rtl_status['count'] + af2udp_status['count'] + 
                          ft8_status['count'] + ws_status['count'] + bridge_status['count'])
        lines.append(f"   Total Active Processes: {total_processes}")
        
        if rtl_status['count'] > 0 and af2udp_status['count'] > 0:
            lines.append("   ✅ Audio Pipeline: OPERATIONAL")
        else:
            lines.append("   ❌ Audio Pipeline: NEEDS ATTENTION")
            
        if ft8_status['count'] > 0:
            lines.append("   ✅ FT8 Decoders: OPERATIONAL")
        else:
            lines.append("   ❌ FT8 Decoders: STOPPED")

        if ports.get(4010) == "LISTENING":
            lines.append("   ✅ WebSocket Server: OPERATIONAL")
        else:
            lines.append("   ❌ WebSocket Server: NOT ACCESSIBLE")

        lines.append("=" * 80)

        return "\n".join(lines)

    async def send_status_display(self):
        """Send status display to WebSocket clients"""
        try:
            # Get status information
            status = self.get_sdr_modem_status()
            display_text = self.format_status_display(status)
            
            # Clear screen message
            clear_message = {
                "type": "clear_screen",
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Status display message - use broadcast type to reach all control points
            status_message = {
                "type": "system_status_broadcast",
                "broadcast_type": "status_display",
                "title": "SDR Modem Decoder Status",
                "content": display_text,
                "raw_data": status,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if self.websocket:
                # Send clear screen first
                await self.websocket.send(json.dumps(clear_message))
                await asyncio.sleep(0.1)
                
                # Send status display
                await self.websocket.send(json.dumps(status_message))
                print("✅ Status display sent to WebSocket clients")
                return True
                
        except Exception as e:
            print(f"❌ Error sending status display: {e}")
            return False

    async def run(self):
        """Main execution"""
        try:
            if await self.connect_websocket():
                await self.send_status_display()
                await self.websocket.close()
        except Exception as e:
            print(f"Error: {e}")

async def main():
    status_display = SDRModemStatus()
    await status_display.run()

if __name__ == "__main__":
    asyncio.run(main())
