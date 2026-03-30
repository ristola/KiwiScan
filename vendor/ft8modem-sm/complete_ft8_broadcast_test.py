#!/usr/bin/env python3
"""
Complete FT8 Broadcast Test
"""

import asyncio
import websockets
import json

async def test_complete_ft8_broadcast():
    """Complete test to verify FT8 broadcast functionality"""
    
    # Test 1: Single connection to verify server is working
    print("🔍 Test 1: Verifying server connection...")
    try:
        uri = "ws://10.146.1.241:4010"
        async with websockets.connect(uri) as ws:
            print("✅ Server connection successful")
            
            # Send a simple command to test
            await ws.send("help")
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=2.0)
                print(f"📥 Help response received: {response[:100]}...")
            except asyncio.TimeoutError:
                print("⏰ No help response")
    except Exception as e:
        print(f"❌ Server connection failed: {e}")
        return
    
    print("\n" + "="*50)
    
    # Test 2: Dual connection broadcast test
    print("🔍 Test 2: Testing FT8 broadcast between clients...")
    
    try:
        # Set up listener
        async with websockets.connect(uri) as listener_ws:
            print("🎧 Listener connected")
            
            # Set up sender 
            async with websockets.connect(uri) as sender_ws:
                print("📡 Sender connected")
                
                # Send FT8 decode message
                ft8_data = {
                    "type": "ft8_decode",
                    "sdr_number": "sdr1",
                    "frequency": 139074123,
                    "message": "CQ TEST K1ABC FN20"
                }
                
                print(f"📤 Sending FT8 data: {json.dumps(ft8_data)}")
                await sender_ws.send(json.dumps(ft8_data))
                
                # Listen for broadcast
                print("🔍 Waiting for broadcast to listener...")
                found_broadcast = False
                
                for attempt in range(5):
                    try:
                        msg = await asyncio.wait_for(listener_ws.recv(), timeout=1.0)
                        print(f"📥 Listener received ({attempt+1}): {msg[:100]}...")
                        
                        # Check if this is our FT8 broadcast
                        try:
                            data = json.loads(msg)
                            if data.get("type") == "ft8_decode_broadcast":
                                print("✅ SUCCESS: FT8 data was broadcast to listener!")
                                print(f"   Broadcast data: {data}")
                                found_broadcast = True
                                break
                            elif "ft8_decode" in msg:
                                print("✅ SUCCESS: FT8 data found in message!")
                                found_broadcast = True
                                break
                        except json.JSONDecodeError:
                            if "ft8_decode" in msg:
                                print("✅ SUCCESS: FT8 data found in text message!")
                                found_broadcast = True
                                break
                                
                    except asyncio.TimeoutError:
                        print(f"⏰ Waiting... {attempt+1}/5")
                
                if not found_broadcast:
                    print("❌ FT8 data was NOT broadcast to listener")
                    print("💡 This suggests the server doesn't relay messages between clients")
                    
    except Exception as e:
        print(f"❌ Broadcast test failed: {e}")
    
    print("\n" + "="*50)
    print("🏁 Test complete")

if __name__ == "__main__":
    asyncio.run(test_complete_ft8_broadcast())
