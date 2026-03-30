#!/usr/bin/env python3
"""
Simple WebSocket Test Client
Sends a test message to WS:4010 to verify connection
"""

import asyncio
import websockets
import json
from datetime import datetime

async def test_websocket():
    try:
        print("🔗 Connecting to ws://10.146.1.241:4010...")
        websocket = await websockets.connect("ws://10.146.1.241:4010")
        print("✅ Connected successfully!")
        
        # Send test message
        test_message = {
            "type": "ft8_test",
            "sdr_id": "SDR-10000003",
            "timestamp": datetime.utcnow().isoformat(),
            "frequency": "14.074",
            "mode": "FT8",
            "data": "TEST MESSAGE FROM FT8 BRIDGE",
            "test": True
        }
        
        await websocket.send(json.dumps(test_message))
        print("📤 Test message sent:")
        print(json.dumps(test_message, indent=2))
        
        # Keep connection open briefly
        await asyncio.sleep(2)
        
        await websocket.close()
        print("✅ Test completed")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket())
