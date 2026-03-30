#!/bin/bash
# Test script for MODE command functionality

echo "Testing MODE command in ft8modem"
echo "================================="
echo ""

# Start ft8modem in FT8 mode with UDP (no actual audio device needed)
# We'll send MODE command to test the functionality
echo "Starting ft8modem in FT8 mode..."
echo ""

# Use a temporary FIFO for bidirectional communication
FIFO_IN=$(mktemp -u)
FIFO_OUT=$(mktemp -u)
mkfifo "$FIFO_IN"
mkfifo "$FIFO_OUT"

# Start ft8modem in background, reading from FIFO_IN
/opt/ShackMate/ft8modem/ft8modem -r 48000 FT8 udp:9999 < "$FIFO_IN" > "$FIFO_OUT" 2>&1 &
MODEM_PID=$!

# Give it time to start
sleep 2

# Open FIFO for writing
exec 3>"$FIFO_IN"
exec 4<"$FIFO_OUT"

echo "Test 1: Query current mode (should return FT8)"
echo "MODE" >&3
sleep 1

echo ""
echo "Test 2: Switch to FT4 mode"
echo "MODE FT4" >&3
sleep 2

echo ""
echo "Test 3: Query mode again (should return FT4)"
echo "MODE" >&3
sleep 1

echo ""
echo "Test 4: Switch back to FT8"
echo "MODE FT8" >&3
sleep 2

echo ""
echo "Test 5: Query mode one more time (should return FT8)"
echo "MODE" >&3
sleep 1

echo ""
echo "Sending QUIT command..."
echo "QUIT" >&3

# Wait for modem to exit
wait $MODEM_PID 2>/dev/null

# Close FIFOs
exec 3>&-
exec 4<&-
rm -f "$FIFO_IN" "$FIFO_OUT"

echo ""
echo "Test completed!"
echo ""
echo "To manually test, run:"
echo "  /opt/ShackMate/ft8modem/ft8modem -r 48000 FT8 udp:9999"
echo ""
echo "Then type:"
echo "  MODE       <- Shows current mode"
echo "  MODE FT4   <- Switches to FT4"
echo "  MODE FT8   <- Switches to FT8"
echo "  QUIT       <- Exits"
