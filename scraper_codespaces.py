# Bundestag Scraper for GitHub Codespaces - 2021 version
# Single-threaded version for cloud stability
import os
import time
import csv
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException, NoAlertPresentException, TimeoutException
import uuid
import tempfile
import shutil
import subprocess
import signal
import glob
import traceback
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import concurrent.futures

def safe_quit(driver, profile_dir):
    try:
        driver.quit()
    except Exception:
        pass
    finally:
        if profile_dir:
            try:
                shutil.rmtree(profile_dir, ignore_errors=True)
            except Exception:
                pass

def _kill_existing_chrome_processes():
    """Intenta terminar procesos chromedriver / chrome huérfanos que puedan bloquear perfiles."""
    try:
        out = subprocess.check_output(["ps", "aux"], text=True)
        for line in out.splitlines():
            if ("chromedriver" in line or "chrome" in line or "chromium" in line) and "grep" not in line:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        pid = int(parts[1])
                        os.kill(pid, signal.SIGTERM)
                    except Exception:
                        # ignore failures (process may have exited)
                        pass
    except Exception:
        pass

def _find_chrome_binary():
    """Detecta un binario de Chrome/Chromium disponible en el contenedor."""
    candidates = [
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/snap/bin/chromium",
    ]
    def _is_runnable(path):
        try:
            res = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=3)
            if res.returncode == 0:
                return True
            # some wrappers may exit non-zero but print a helpful message
            return False
        except Exception:
            return False

    for p in candidates:
        if p and os.path.exists(p):
            if _is_runnable(p):
                return p
            else:
                print(f"Skipping non-runnable Chrome candidate: {p}")
    # fallback a PATH
    for name in ("chromium-browser", "chromium", "google-chrome", "google-chrome-stable", "chrome"):
        p = shutil.which(name)
        if p:
            if _is_runnable(p):
                return p
            else:
                print(f"Skipping non-runnable Chrome candidate in PATH: {p}")
    return None

def _cleanup_stale_profiles(max_age_seconds=600):
    """Remove stale chrome profile dirs in /tmp older than max_age_seconds."""
    now = time.time()
    patterns = ["/tmp/chrome_profile_*", "/tmp/*chrome_user_data*", "/tmp/*hrome_profile_*"]
    for pat in patterns:
        for p in glob.glob(pat):
            try:
                if os.path.isdir(p):
                    mtime = os.path.getmtime(p)
                    if now - mtime > max_age_seconds:
                        shutil.rmtree(p, ignore_errors=True)
                        print(f"Removed stale profile: {p}")
            except Exception:
                pass

def get_chrome_driver():
    """Chrome driver setup: unique user-data-dir + webdriver-manager + process cleanup + retries"""
    max_attempts = 3
    last_exc = None

    # Remove old temporary profiles immediately to avoid collisions
    _cleanup_stale_profiles(0)

    chrome_bin = _find_chrome_binary()
    if not chrome_bin:
        print("Warning: no Chrome/Chromium binary found in expected locations. Install chromium-browser or google-chrome.")
    else:
        # muestra la ruta detectada para debugging
        print(f"Using Chrome binary: {chrome_bin}")

    for attempt in range(1, max_attempts + 1):
        user_data_dir = tempfile.mkdtemp(prefix="chrome_profile_")
        profile_dir = user_data_dir  # name used by safe_quit cleanup
        options = webdriver.ChromeOptions()
        # Use the modern headless mode in Chrome
        options.add_argument("--headless=chrome")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument("--disable-background-timer-throttling")
        options.add_argument("--disable-renderer-backgrounding")
        options.add_argument("--disable-backgrounding-occluded-windows")
        options.add_argument("--remote-debugging-port=0")
        # enable verbose logging for Chrome/chromedriver to /tmp
        options.add_argument("--enable-logging")
        options.add_argument("--v=1")
        # additional flags to improve stability in container environments
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-features=VizDisplayCompositor")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--use-gl=swiftshader")

        if chrome_bin:
            options.binary_location = chrome_bin

        print(f"[get_chrome_driver] attempt={attempt} user_data_dir={user_data_dir} chrome_bin={chrome_bin}")
        try:
            # If CHROME_REMOTE_URL is set, try connecting to a remote chromedriver server
            remote_url = os.environ.get("CHROME_REMOTE_URL") or os.environ.get("CHROMEDRIVER_REMOTE_URL")
            if remote_url:
                print(f"[get_chrome_driver] attempting remote connection to {remote_url}")
                try:
                    driver = webdriver.Remote(command_executor=remote_url, options=options)
                    driver.set_page_load_timeout(30)
                    return driver, profile_dir
                except Exception as e_remote:
                    last_exc = e_remote
                    print(f"Remote chromedriver connection failed: {e_remote}")
                    traceback.print_exc()
                    try:
                        shutil.rmtree(user_data_dir, ignore_errors=True)
                    except Exception:
                        pass
                    _kill_existing_chrome_processes()
                    time.sleep(1)
                    continue

            # Use webdriver-manager to ensure matching chromedriver
            # but the returned path may contain unexpected newlines in this env.
            wdm_path = ChromeDriverManager().install()
            # try to find the chromedriver executable under the parent folder using glob
            import glob
            driver_dir = os.path.dirname(wdm_path)
            candidates = glob.glob(os.path.join(driver_dir, '**', 'chromedriver'), recursive=True)
            chromedriver_path = None
            if candidates:
                # prefer executable candidate
                for c in candidates:
                    if os.path.isfile(c) and os.access(c, os.X_OK):
                        chromedriver_path = c
                        break
            if not chromedriver_path:
                # fallback to returned path (may still work)
                chromedriver_path = wdm_path
            # write chromedriver log to a unique file so we can inspect crashes
            chromedriver_log = f"/tmp/chromedriver_{uuid.uuid4().hex}.log"
            service = Service(chromedriver_path, log_path=chromedriver_log)
            print(f"[get_chrome_driver] using chromedriver={chromedriver_path} log={chromedriver_log}")
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(30)
            # quick sanity check: ensure the session is responsive
            try:
                driver.execute_script("return 1")
            except Exception as sanity_err:
                # force cleanup and surface an error to trigger retry
                try:
                    driver.quit()
                except Exception:
                    pass
                raise Exception(f"Sanity check failed after starting driver: {sanity_err}")
            return driver, profile_dir
        except Exception as e:
            last_exc = e
            # Log brief error to help debugging
            print(f"get_chrome_driver attempt {attempt} failed: {e}")
            traceback.print_exc()
            # Clean up temp profile from failed attempt
            try:
                shutil.rmtree(user_data_dir, ignore_errors=True)
            except Exception:
                pass

            # Try to kill any leftover chrome/chromedriver processes that may hold locks
            _kill_existing_chrome_processes()

            # small backoff before retry
            time.sleep(1)

    # si llegamos aquí, fallaron todos los intentos
    raise last_exc

def scrape_single_muni(idx):
    max_attempts = 2
    attempt = 0
    
    while attempt < max_attempts:
        driver = None
        profile_dir = None
        
        # Log start of attempt
        with open("scraped_munis.log", "a") as logf:
            logf.write(f"{idx},started,attempt_{attempt}\n")
        
        try:
            # Create driver instance
            driver, profile_dir = get_chrome_driver()
            print(f"\n--- Processing municipality #{idx} ---")

            main_url = "https://wahlen.votemanager.de/"
            driver.get(main_url)

            page_num = (idx - 1) // 10 + 1
            row_on_page = ((idx - 1) % 10) + 1

            # Navigate to correct page
            if page_num > 1:
                try:
                    # Iterate pages with a progress bar so the user can see activity
                    from tqdm import trange
                    for i in trange(page_num - 1, desc=f"Clicking weiter", leave=False):
                        time.sleep(0.3)

                        clicked = False
                        selectors = [
                            "#ergebnisTabelle_next > a",
                            "#ergebnisTabelle_next a",
                            "a[aria-label='Next']",
                            ".paginate_button.next a",
                            ".page-item.next a"
                        ]

                        for selector in selectors:
                            try:
                                WebDriverWait(driver, 3).until(
                                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                                )

                                driver.execute_script(f"""
                                    var btn = document.querySelector('{selector}');
                                    if (btn) {{ 
                                        btn.scrollIntoView(true);
                                        btn.click();
                                    }}
                                """)

                                clicked = True
                                break

                            except Exception:
                                continue

                        if not clicked:
                            raise Exception("Could not click weiter button with any selector")

                        time.sleep(0.3)

                except Exception as e:
                    print(f"Error clicking 'weiter' button for municipality #{idx}: {e}")
                    if driver:
                        driver.quit()
                    attempt += 1
                    continue

            # Find municipality row and check Bundesland
            try:
                muni_row = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, f"/html/body/div[3]/div/div/div/div/table/tbody/tr[{row_on_page}]"))
                )
                
                # Check if Bayern (skip if so)
                bundesland_cell = muni_row.find_element(By.XPATH, "./td[3]")
                bundesland = bundesland_cell.text.strip()
                
                if bundesland == "Bayern":
                    print(f"Municipality #{idx} is in Bayern - skipping (no data available)")
                    with open("scraped_munis.log", "a") as logf:
                        logf.write(f"{idx},bayern_skip\n")
                    if driver:
                        safe_quit(driver, profile_dir)
                    attempt = max_attempts
                    continue
                
                # Get municipality info
                muni_link = muni_row.find_element(By.XPATH, "./td[1]/a")
                muni_url = muni_link.get_attribute("href")
                muni_name = muni_link.text.strip().replace(" ", "_")
                print(f"Found municipality: {muni_name} (Bundesland: {bundesland})")
                
            except Exception as e:
                print(f"Could not find municipality link for #{idx}: {e}")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt += 1
                continue

            # Go to municipality page
            driver.get(muni_url)

            # Find Bundestagswahl 2021
            try:
                WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.XPATH, "//td[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'bundestag') or contains(text(), '2021')]"))
                )
                table = driver.find_element(By.XPATH, "/html/body/div/div[2]/table/tbody")
                rows = table.find_elements(By.TAG_NAME, "tr")
            except Exception:
                print(f"No Bundestagswahl or 2021 election found for municipality #{idx}, skipping.")
                with open("scraped_munis.log", "a") as logf:
                    logf.write(f"{idx},no_bundestagswahl\n")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt = max_attempts
                continue

            # Find exact 2021 Bundestag election
            found = False
            election_link = None

            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    year_text = cells[0].text.strip()
                    election_text = cells[1].text.strip()
                    
                    has_2021 = "2021" in year_text
                    has_bundestag = "bundestag" in election_text.lower()
                    
                    if has_2021 and has_bundestag:
                        try:
                            election_link = cells[1].find_element(By.TAG_NAME, "a")
                            found = True
                            print(f"Found 2021 Bundestag election: '{election_text}' (Year: {year_text})")
                            break
                        except:
                            continue

            if not found:
                print(f"Bundestagswahl 2021 link not found for municipality #{idx}")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt += 1
                continue

            # Click election link
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", election_link)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", election_link)
                WebDriverWait(driver, 10).until(EC.url_changes(muni_url))
            except UnexpectedAlertPresentException:
                try:
                    alert = driver.switch_to.alert
                    alert.accept()
                except NoAlertPresentException:
                    pass
                print("pop up window. election not available")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt += 1
                continue
            except Exception as e:
                print(f"Error after clicking election link: {e}")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt += 1
                continue

            # Click 'mehr ...' link
            try:
                mehr_link = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "mehr"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", mehr_link)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", mehr_link)
            except TimeoutException:
                print("Timeout: 'mehr ...' link not found, skipping municipality.")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt += 1
                continue
            except Exception as e:
                print(f"Error finding/clicking 'mehr ...' link: {e}")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt += 1
                continue

            # Wait for results page
            WebDriverWait(driver, 10).until(EC.url_contains("ergebnis.html"))

            # Click 'weitere' dropdown
            try:
                weitere_dropdown = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'dropdown-toggle') and contains(text(), 'weitere')]"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", weitere_dropdown)
                weitere_dropdown.click()
                time.sleep(0.5)
            except Exception:
                print("Dropdown click done.")

            # Click Open Data link
            try:
                opendata_link = driver.find_element(By.XPATH, "//a[contains(@class, 'dropdown-item') and contains(., 'Open Data')]")
                driver.execute_script("arguments[0].scrollIntoView(true);", opendata_link)
                driver.execute_script("arguments[0].click();", opendata_link)
            except Exception:
                print("Empty page error. No data available")
                with open("scraped_munis.log", "a") as logf:
                    logf.write(f"{idx},no_opendata\n")
                if driver:
                    safe_quit(driver, profile_dir)
                attempt = max_attempts
                continue

            # Wait for OpenData page
            WebDriverWait(driver, 10).until(EC.url_contains("opendata.html"))
            print("Arrived at OpenData page:", driver.current_url)
            time.sleep(0.5)

            # Collect CSV links
            csv_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.csv')]")
            csv_url_list = []
            for link in csv_links:
                href = link.get_attribute("href")
                text = link.text.strip()
                csv_url_list.append({"text": text, "url": href})

            # Save results
            os.makedirs("2021/data_links", exist_ok=True)
            muni_name_safe = muni_name.replace("/", "_").replace("\\", "_")
            output_file = f"2021/data_links/{muni_name_safe}_data_links.csv"
            
            with open(output_file, "w", encoding="utf-8", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["text", "url"])
                writer.writeheader()
                writer.writerows(csv_url_list)
            
            print(f"All found CSV URLs saved to {output_file}")

            # Log success
            with open("scraped_munis.log", "a") as logf:
                logf.write(f"{idx},success\n")
            
            if driver:
                safe_quit(driver, profile_dir)
            break  # Success!

        except Exception as e:
            # Handle errors
            if "ERR_INTERNET_DISCONNECTED" in str(e) or "net::" in str(e):
                print("Internet connection lost")
            else:
                print(f"Error scraping municipality #{idx} (attempt {attempt + 1}): {e}")
            
            with open("scraped_munis.log", "a") as logf:
                logf.write(f"{idx},failed,attempt_{attempt},{str(e)[:100]}\n")
            
            if driver:
                safe_quit(driver, profile_dir)
            attempt += 1

def main():
    """Main execution function - NO THREADING for Codespaces"""
    # Start with a small test range
    muni_indices = list(range(2501, 2510))  # Test with 10 municipalities
    
    print(f"Starting scraper for {len(muni_indices)} municipalities (2500-2509)")
    
    # Resume logic
    try:
        with open("scraped_munis.log", "r") as logf:
            scraped = set(int(line.split(",")[0]) for line in logf if "success" in line or "bayern_skip" in line or "no_bundestagswahl" in line or "no_opendata" in line)
        print(f"Found {len(scraped)} already processed municipalities")
    except FileNotFoundError:
        scraped = set()
        print("No previous log found, starting fresh")

    # Filter out completed municipalities
    muni_indices = [i for i in muni_indices if i not in scraped]
    print(f"{len(muni_indices)} municipalities remaining to process")
    
    if not muni_indices:
        print("All municipalities already processed!")
        return

    # Process municipalities ONE BY ONE (no threading)
    for idx in tqdm(muni_indices, desc="Scraping municipalities"):
        scrape_single_muni(idx)
        
        # Brief pause between municipalities
        time.sleep(1)
        
        # Progress update every 5 municipalities
        if idx % 5 == 0:
            print(f"Completed municipality #{idx}")

    print("Scraping complete!")

if __name__ == "__main__":
    main()
