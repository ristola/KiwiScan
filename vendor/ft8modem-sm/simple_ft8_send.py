#!/usr/bin/env python3
"""
Simple FT8 Send Test
"""

import asyncio
import websockets
import json

async def send_ft8_test():
    try:
        uri = "ws://10.146.1.241:4010"
        async with websockets.connect(uri) as ws:
            print("✅ Connected to server")
            
            # Send FT8 decode message
            ft8_msg = {
                "type": "ft8_decode",
                "sdr_number": "sdr1",
                "frequency": 139074123,
                "message": "CQ TEST K1ABC FN20"
            }
            
            print(f"📤 Sending: {json.dumps(ft8_msg)}")
            await ws.send(json.dumps(ft8_msg))
            print("✅ Message sent successfully!")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(send_ft8_test())
