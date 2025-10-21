# Batch runner for managing large scraping jobs in Codespaces
import subprocess
import time
from datetime import datetime

def run_batch(start_idx, end_idx, batch_name):
    """Run a specific batch of municipalities"""
    print(f"Starting batch {batch_name}: {start_idx}-{end_idx}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Modify the range in scraper_codespaces.py
    with open("scraper_codespaces.py", "r") as f:
        content = f.read()
    
    # Replace the range
    old_range = "muni_indices = list(range(2500, 3176))"
    new_range = f"muni_indices = list(range({start_idx}, {end_idx + 1}))"
    content = content.replace(old_range, new_range)
    
    with open(f"scraper_batch_{batch_name}.py", "w") as f:
        f.write(content)
    
    # Run the batch
    try:
        result = subprocess.run(["python", f"scraper_batch_{batch_name}.py"], 
                              capture_output=True, text=True, timeout=14400)  # 4 hour timeout
        
        print(f"Batch {batch_name} completed successfully!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return True
        
    except subprocess.TimeoutExpired:
        print(f"Batch {batch_name} timed out after 4 hours")
        return False
    except Exception as e:
        print(f"Batch {batch_name} failed: {e}")
        return False

# Define batches (adjust based on your Codespaces usage)
batches = [
    (2500, 2600, "01"),  # 100 municipalities
    (2601, 2700, "02"),  # 100 municipalities  
    (2701, 2800, "03"),  # 100 municipalities
    (2801, 2900, "04"),  # 100 municipalities
    (2901, 3000, "05"),  # 100 municipalities
    (3001, 3100, "06"),  # 100 municipalities
    (3101, 3175, "07"),  # 75 municipalities
]

if __name__ == "__main__":
    print("Available batches:")
    for i, (start, end, name) in enumerate(batches):
        print(f"  {i+1}. Batch {name}: {start}-{end} ({end-start+1} municipalities)")
    
    choice = input("\nEnter batch number to run (1-7): ")
    
    try:
        batch_idx = int(choice) - 1
        if 0 <= batch_idx < len(batches):
            start, end, name = batches[batch_idx]
            run_batch(start, end, name)
        else:
            print("Invalid batch number")
    except ValueError:
        print("Please enter a valid number")
