#!/usr/bin/env python3
"""
Test FT8 Broadcast - Check if WebSocket server broadcasts FT8 data between clients
"""

import asyncio
import websockets
import json

async def test_ft8_broadcast():
    """Test if FT8 data sent by one client is broadcast to others"""
    try:
        # Connect listener first
        async with websockets.connect("ws://10.146.1.241:4010") as listener_ws:
            print("🎧 Listener connected")
            
            # Connect sender
            async with websockets.connect("ws://10.146.1.241:4010") as sender_ws:
                print("📡 Sender connected")
                
                # Send FT8 data
                ft8_data = {
                    "type": "ft8_decode",
                    "sdr_number": "sdr3",
                    "frequency": 139074789,
                    "message": "CQ DX K1TEST FN20"
                }
                
                await sender_ws.send(json.dumps(ft8_data))
                print(f"📤 Sent: {json.dumps(ft8_data)}")
                
                # Check if listener receives the broadcast
                found_broadcast = False
                for i in range(5):
                    try:
                        msg = await asyncio.wait_for(listener_ws.recv(), timeout=1.0)
                        print(f"📥 Listener received: {msg}")
                        
                        if "ft8_decode" in msg:
                            print("✅ SUCCESS: FT8 data was broadcast to other clients!")
                            found_broadcast = True
                            break
                            
                    except asyncio.TimeoutError:
                        print(f"⏰ Waiting for broadcast... {i+1}/5")
                        
                if not found_broadcast:
                    print("❌ FT8 data was NOT broadcast to other clients")
                    print("💡 Server likely doesn't relay client messages to other clients")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_ft8_broadcast())
