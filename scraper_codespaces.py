# Bundestag Scraper for GitHub Codespaces - 2021 version
# Optimized for municipalities 2500-3175
import os
import time
import csv
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException, NoAlertPresentException, TimeoutException

def get_chrome_driver():
    """Chrome driver setup optimized for Codespaces"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-images")  # Speed up loading
    
    # Use system ChromeDriver in Codespaces
    try:
        driver = webdriver.Chrome('/usr/bin/chromedriver', options=options)
    except:
        # Fallback to default path
        driver = webdriver.Chrome(options=options)
    
    driver.set_page_load_timeout(20)
    return driver

def scrape_single_muni(idx):
    max_attempts = 2
    attempt = 0
    
    while attempt < max_attempts:
        driver = get_chrome_driver()
        
        # Log start of attempt
        with open("scraped_munis.log", "a") as logf:
            logf.write(f"{idx},started,attempt_{attempt}\n")
        
        try:
            print(f"\n--- Processing municipality #{idx} ---") 

            main_url = "https://wahlen.votemanager.de/"
            driver.get(main_url)

            page_num = (idx - 1) // 10 + 1
            row_on_page = ((idx - 1) % 10) + 1

            # Navigate to correct page
            if page_num > 1:
                try:
                    for i in range(page_num - 1):
                        time.sleep(0.4)
                        
                        # Try multiple selectors for weiter button
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
                            raise Exception("Could not click weiter button")
                        
                        time.sleep(0.5)
                        
                except Exception as e:
                    print(f"Error navigating to page {page_num}: {e}")
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
                    print(f"Municipality #{idx} is in Bayern - skipping")
                    with open("scraped_munis.log", "a") as logf:
                        logf.write(f"{idx},bayern_skip\n")
                    driver.quit()
                    attempt = max_attempts
                    continue
                
                # Get municipality info
                muni_link = muni_row.find_element(By.XPATH, "./td[1]/a")
                muni_url = muni_link.get_attribute("href")
                muni_name = muni_link.text.strip().replace(" ", "_")
                print(f"Found: {muni_name} ({bundesland})")
                
            except Exception as e:
                print(f"Could not find municipality #{idx}: {e}")
                driver.quit()
                attempt += 1
                continue

            # Navigate to municipality page
            driver.get(muni_url)

            # Find Bundestagswahl 2021
            try:
                WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.XPATH, "//td[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'bundestag') or contains(text(), '2021')]"))
                )
                table = driver.find_element(By.XPATH, "/html/body/div/div[2]/table/tbody")
                rows = table.find_elements(By.TAG_NAME, "tr")
            except Exception:
                print("No Bundestagswahl 2021 found")
                with open("scraped_munis.log", "a") as logf:
                    logf.write(f"{idx},no_bundestagswahl\n")
                driver.quit()
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
                            print(f"Found: '{election_text}' ({year_text})")
                            break
                        except:
                            continue

            if not found:
                print("No 2021 Bundestag link found")
                driver.quit()
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
                print("Election not available (popup)")
                driver.quit()
                attempt += 1
                continue
            except Exception as e:
                print(f"Error clicking election: {e}")
                driver.quit()
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
            except Exception as e:
                print("'mehr' link not found")
                driver.quit()
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
                driver.quit()
                attempt = max_attempts
                continue

            # Wait for OpenData page
            WebDriverWait(driver, 10).until(EC.url_contains("opendata.html"))
            print("Arrived at OpenData page")
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
            
            print(f"Saved: {output_file}")

            # Log success
            with open("scraped_munis.log", "a") as logf:
                logf.write(f"{idx},success\n")
            
            driver.quit()
            break  # Success!

        except Exception as e:
            # Handle errors
            if "ERR_INTERNET_DISCONNECTED" in str(e) or "net::" in str(e):
                print("Internet connection lost")
            else:
                print(f"Error (attempt {attempt + 1}): {e}")
            
            with open("scraped_munis.log", "a") as logf:
                logf.write(f"{idx},failed,attempt_{attempt},{e}\n")
            
            driver.quit()
            attempt += 1

def main():
    """Main execution function"""
    # Target range: 2500-3175
    muni_indices = list(range(2500, 3176))
    
    print(f"Starting scraper for {len(muni_indices)} municipalities (2500-3175)")
    
    # Resume logic
    try:
        with open("scraped_munis.log") as logf:
            scraped = set(int(line.split(",")[0]) for line in logf if "success" in line or "bayern_skip" in line or "no_bundestagswahl" in line or "no_opendata" in line)
        print(f"Found {len(scraped)} already processed municipalities")
    except FileNotFoundError:
        scraped = set()
        print("No previous log found, starting fresh")

    # Filter out completed municipalities
    remaining = [i for i in muni_indices if i not in scraped]
    print(f"{len(remaining)} municipalities remaining to process")
    
    if not remaining:
        print("All municipalities already processed!")
        return

    # Process municipalities
    for idx in tqdm(remaining, desc="Scraping"):
        scrape_single_muni(idx)
        
        # Progress checkpoint every 10 municipalities
        if idx % 10 == 0:
            completed = len([i for i in muni_indices if i not in remaining[:remaining.index(idx)+1:]])
            print(f"Progress: {completed}/{len(muni_indices)} municipalities")

    print("Scraping complete!")

if __name__ == "__main__":
    main()
