import subprocess
import time
import os

DB_PATH = "../src/nanodb"
DB_LOG = "../src/wal.log"

def run_command(proc, command):
    """Sends a command to the database and returns the response."""
    # Write command to stdin (mimicking user typing)
    proc.stdin.write(f"{command}\n".encode())
    proc.stdin.flush()
    
    # Read the response line (blocking until response arrives)
    output = proc.stdout.readline().decode().strip()
    
    # If the CLI prints a prompt (nanodb> ), we might need to read again 
    # or handle the prompt. For simplicity, we assume one line of output per command 
    # and handle the prompt in the loop or strip it.
    
    # Note: Our C++ code prints "nanodb> " *before* waiting for input.
    # So when we read response, we are reading the result of the previous command.
    return output

def start_db():
    """Starts the DB process."""
    if os.path.exists(DB_LOG):
        # Optional: Start fresh for the test? 
        # For persistence testing, we might want to keep it.
        # Let's clean up for a clean start first.
        pass

    proc = subprocess.Popen(
        [DB_PATH],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0 # Unbuffered
    )
    
    # Read the startup messages ("Initializing...", "Ready...", "nanodb> ")
    # We just consume them so we can get to the interactive part
    for _ in range(3): 
        proc.stdout.readline()
        
    return proc

def test_basic_operations():
    print("--- Testing Basic PUT/GET/DEL ---")
    proc = start_db()
    
    # 1. PUT
    print("PUT user1 -> OK")
    proc.stdin.write(b"PUT user1 Alice\n")
    proc.stdin.flush()
    # Read "nanodb> " prompt (from previous loop) -> wait, the logic needs to align 
    # with the C++ print order. 
    # Simplest way: just read lines until we see the output we want.
    
    # Let's simplify the interaction for the script:
    # The C++ prints "OK" followed by "nanodb> ". 
    # We read line by line.
    
    # Reading the "OK"
    out = proc.stdout.readline().decode().strip()
    assert out == "nanodb> OK", f"Expected 'OK', got '{out}'"

    # Reading the prompt "nanodb> " is tricky because it has no newline.
    # A robust integration test usually ignores prompts or waits for ready state.
    # For this script, we can ignore the prompt check since we know the order.

    # 2. GET
    print("GET user1 -> Alice")
    proc.stdin.write(b"GET user1\n")
    proc.stdin.flush()
    # Skip the prompt printed after the previous command
    proc.stdout.read(8) # Read "nanodb> " bytes roughly, or just rely on readline
    
    out = proc.stdout.readline().decode().strip()
    assert out == "Alice", f"Expected 'Alice', got '{out}'"

    # 3. DELETE
    print("DEL user1 -> OK (Deleted)")
    proc.stdin.write(b"DEL user1\n")
    proc.stdin.flush()
    proc.stdout.read(8) # Skip prompt
    
    out = proc.stdout.readline().decode().strip()
    assert out == "OK (Deleted)", f"Expected 'OK (Deleted)', got '{out}'"

    # 4. Verify Delete
    print("GET user1 -> (nil)")
    proc.stdin.write(b"GET user1\n")
    proc.stdin.flush()
    proc.stdout.read(8) # Skip prompt
    
    out = proc.stdout.readline().decode().strip()
    assert out == "(nil)", f"Expected '(nil)', got '{out}'"

    # Clean exit
    proc.stdin.write(b"EXIT\n")
    proc.communicate() # Wait for process to close
    print("Basic Operations Passed!\n")

def test_persistence():
    print("--- Testing Persistence (Crash Recovery) ---")
    
    # 1. Start DB, Write Data, Exit
    if os.path.exists(DB_LOG): os.remove(DB_LOG) # Clean start
    
    proc = start_db()
    proc.stdin.write(b"PUT persist_key data_survives\n")
    proc.stdin.flush()
    out = proc.stdout.readline().decode().strip() # "OK"
    
    proc.stdin.write(b"EXIT\n")
    proc.communicate()
    
    print("Data written. Database stopped.")
    time.sleep(1) 
    
    # 2. Restart DB and Verify
    print("Restarting DB...")
    proc = start_db()
    
    proc.stdin.write(b"GET persist_key\n")
    proc.stdin.flush()
    
    # There might be a prompt issue here too, let's just read the result line
    # Depending on your C++ prompt output logic (`print_prompt` called after loop),
    # the buffer might contain "nanodb> ".
    
    # A cleaner hack for this script: Read until we find the value or fail
    found = False
    for _ in range(5): # Try reading 5 lines max
        line = proc.stdout.readline().decode().strip()
        if "data_survives" in line:
            found = True
            break
            
    assert found, "Persistence Failed: Could not find 'data_survives' after restart."
    
    proc.stdin.write(b"EXIT\n")
    proc.communicate()
    print("Persistence Passed!\n")

if __name__ == "__main__":
    try:
        test_basic_operations()
        test_persistence()
        print("ALL SYSTEMS GO: NanoDB is ready for production (sort of).")
    except Exception as e:
        print(f"TEST FAILED: {e}")
