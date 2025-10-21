#!/usr/bin/env python3
"""Small test harness to try starting the Chrome driver via get_chrome_driver().

- Imports get_chrome_driver from scraper.scraper_codespaces
- Attempts to start the driver, navigate to example.org, prints title on success
- On failure prints exception and prints the path to any chromedriver log files in /tmp created recently
"""
import time
import glob
import os
import sys

# Ensure parent directory is on sys.path so `import scraper` works when running
# this script from inside the package directory.
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(THIS_DIR)
if PARENT not in sys.path:
    sys.path.insert(0, PARENT)

from scraper.scraper_codespaces import get_chrome_driver

print("Starting test_driver_start.py")
try:
    driver, profile_dir = get_chrome_driver()
    print("Driver started. Navigating to https://example.org ...")
    driver.get("https://example.org")
    time.sleep(1)
    print("Title:", driver.title)
    driver.quit()
    print("Driver quit cleanly. Cleaning profile_dir:", profile_dir)
    try:
        if profile_dir and os.path.isdir(profile_dir):
            import shutil
            shutil.rmtree(profile_dir, ignore_errors=True)
            print("Removed profile_dir")
    except Exception as e:
        print("Error removing profile_dir:", e)
    sys.exit(0)
except Exception as e:
    print("Driver failed to start:", repr(e))
    # show recent chromedriver logs
    logs = sorted(glob.glob('/tmp/chromedriver_*.log'), key=os.path.getmtime, reverse=True)
    if logs:
        log = logs[0]
        print("Found chromedriver log:", log)
        try:
            with open(log, 'rb') as f:
                data = f.read(2048)
                print("--- first 2KB of chromedriver log ---")
                print(data.decode('utf-8', errors='replace'))
                print("--- end log snippet ---")
        except Exception as e2:
            print("Could not read log file:", e2)
    else:
        print("No chromedriver logs found in /tmp")
    sys.exit(2)
