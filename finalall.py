import time
import os
import re
import pandas as pd
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox
import threading
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def prepare_export_row(orig_date, orig_name, scraped_results=None):
    if scraped_results is None:
        scraped_results = [('', '', '', '', '', '', '')] * 8
    else:
        scraped_results = scraped_results[:8] + [('', '', '', '', '', '', '')] * (8 - len(scraped_results))

    if isinstance(orig_date, (pd.Timestamp, datetime)):
        date_str = orig_date.strftime('%Y-%m-%d')
    else:
        date_str = str(orig_date).split()[0]

    return [
        date_str,
        orig_name,
        *(d[0].upper() for d in scraped_results),   # Dog Name uppercase
        *(d[1] for d in scraped_results),           # Trainer
        *(d[2] for d in scraped_results),           # Dam
        *(d[3] for d in scraped_results),           # Time
        *(d[4] for d in scraped_results),           # Mgn
        *(d[5] for d in scraped_results),           # Split
        *(d[6] for d in scraped_results),           # SP
    ]


def dog_name_to_url(name):
    s = name.lower()
    s = re.sub(r"[^\w\s-]", "", s)  # Remove punctuation
    s = re.sub(r"\s+", "-", s)      # Replace spaces with hyphens
    return f"https://www.thegreyhoundrecorder.com.au/greyhounds/{s}/"


def scrape_worker(df_slice, worker_id):
    print(f"[Worker {worker_id}] Starting with {len(df_slice)} rows")

    options = Options()
    # options.add_argument("--start-maximized")
    # options.add_argument("--headless")  # Uncomment to run headless
    driver = webdriver.Chrome(options=options)

    export_rows = []

    try:
        driver.get("https://www.thegreyhoundrecorder.com.au/search/")

        try:
            close_button_xpath = '//button[contains(@class, "CloseButton__ButtonElement-sc-79mh24-0")]'
            close_button = WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, close_button_xpath))
            )
            close_button.click()
            print(f"[Worker {worker_id}] Modal closed on first load.")
        except Exception:
            print(f"[Worker {worker_id}] No modal displayed or already closed.")

        for idx, row in df_slice.iterrows():
            try:
                orig_date = row.iloc[0]
                orig_name = row.iloc[1]

                if isinstance(orig_date, (pd.Timestamp, datetime)):
                    dog_date_str = orig_date.strftime('%d/%m/%y')
                else:
                    dog_date_str = pd.to_datetime(orig_date).strftime('%d/%m/%y')

                print(f"[Worker {worker_id}] Processing row {idx + 1}: {orig_name} - {dog_date_str}")

                dog_url = dog_name_to_url(orig_name)
                print(f"[Worker {worker_id}] Navigating directly to dog URL: {dog_url}")
                driver.get(dog_url)

                time.sleep(2)  # Allow page to load

                date_link_xpath = f'//tbody//a[text()="{dog_date_str}"]'
                try:
                    date_link_elem = WebDriverWait(driver, 12).until(
                        EC.element_to_be_clickable((By.XPATH, date_link_xpath))
                    )
                except Exception:
                    print(f"[Worker {worker_id}] Date '{dog_date_str}' not found or not clickable for dog '{orig_name}'. Skipping.")
                    export_rows.append(prepare_export_row(orig_date, orig_name, None))
                    continue

                date_link_elem.click()
                print(f"[Worker {worker_id}] Clicked on date link: {dog_date_str}")

                rows_xpath = '//tbody[@data-v-284ac1bd]//tr[contains(@class, "results-event-selection")]'
                rows = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.XPATH, rows_xpath))
                )

                scraped_results = []
                for r in rows:
                    try:
                        placing_text = r.find_elements(By.TAG_NAME, 'td')[0].text.strip()
                        if placing_text.upper() == "SCR":
                            continue

                        # Extract dog name, trainer, dam (existing)
                        dog_name_result = r.find_element(By.CSS_SELECTOR,
                                                        'a.results-event-selection__link > span.results-event-selection__name').text.strip()
                        trainer_name = r.find_elements(By.TAG_NAME, 'td')[3].text.strip()
                        dam_links = r.find_elements(By.CSS_SELECTOR, 'a.results-event-selection__link')
                        dam_name = dam_links[-1].text.strip() if len(dam_links) >= 2 else ''

                        # New extra fields: time, mgn, split, SP
                        # Based on table header: Time at index 4, Mgn at index 5, Split at 6, SP at 11
                        cells = r.find_elements(By.TAG_NAME, 'td')
                        time_val = cells[4].text.strip() if len(cells) > 4 else ''
                        mgn_val = cells[5].text.strip() if len(cells) > 5 else ''
                        split_val = cells[6].text.strip() if len(cells) > 6 else ''
                        sp_val = cells[11].text.strip() if len(cells) > 11 else ''

                        scraped_results.append((dog_name_result, trainer_name, dam_name, time_val, mgn_val, split_val, sp_val))
                    except Exception as e:
                        print(f"[Worker {worker_id}] Error scraping row data: {e}")
                        continue

                export_rows.append(prepare_export_row(orig_date, orig_name, scraped_results))
                print(f"[Worker {worker_id}] Scraped {len(scraped_results)} results for {orig_name}")

            except Exception as e:
                print(f"[Worker {worker_id}] Exception processing row {idx + 1}: {e}")
                # Append empty row to keep format consistent
                try:
                    export_rows.append(prepare_export_row(row.iloc[0], row.iloc[1], None))
                except Exception as e2:
                    print(f"[Worker {worker_id}] Exception preparing empty export row: {e2}")
                continue

    finally:
        driver.quit()
        print(f"[Worker {worker_id}] Browser closed.")

    return export_rows


def run_scraping(file_path, num_browsers):
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        messagebox.showerror("Error", f"Failed to load Excel file:\n{e}")
        return

    total_rows = len(df)
    if total_rows == 0:
        messagebox.showwarning("Warning", "Selected Excel file has no data.")
        return

    num_browsers = int(num_browsers)
    if num_browsers < 1:
        messagebox.showerror("Error", "Number of browsers must be at least 1.")
        return
    if num_browsers > total_rows:
        num_browsers = total_rows

    chunks = [
        df.iloc[i * total_rows // num_browsers: (i + 1) * total_rows // num_browsers]
        for i in range(num_browsers)
    ]

    results = []
    with ThreadPoolExecutor(max_workers=num_browsers) as executor:
        futures = [executor.submit(scrape_worker, chunk, i + 1) for i, chunk in enumerate(chunks)]
        for i, future in enumerate(futures):
            try:
                worker_result = future.result()
                print(f"[Main] Worker {i + 1} returned {len(worker_result)} rows.")
                results.extend(worker_result)
            except Exception as e:
                print(f"[Main] Exception in worker {i + 1}: {e}")

    if not results:
        messagebox.showwarning("Warning", "No data scraped. Export skipped.")
        return

    columns = (
        ['Date', 'Name'] +
        [f'Dog{i + 1}' for i in range(8)] +
        [f'Trainer{i + 1}' for i in range(8)] +
        [f'Dam{i + 1}' for i in range(8)] +
        [f'Time{i + 1}' for i in range(8)] +
        [f'Mgn{i + 1}' for i in range(8)] +
        [f'Split{i + 1}' for i in range(8)] +
        [f'SP{i + 1}' for i in range(8)]
    )

    df_export = pd.DataFrame(results, columns=columns)

    input_folder = os.path.dirname(file_path)
    export_filename = "Updated_AI_Parallel_Result.xlsx"
    export_path = os.path.join(input_folder, export_filename)

    try:
        df_export.to_excel(export_path, index=False)
        messagebox.showinfo("Success", f"Scraping completed!\nResults saved to:\n{export_path}")
        print(f"[Main] Scraping done. Results saved to '{export_path}'")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to save Excel file:\n{e}")
        print(f"[Main] Exception saving Excel: {e}")


def browse_file():
    filename = filedialog.askopenfilename(
        title="Select Excel File",
        filetypes=[("Excel Files", "*.xlsx *.xls"), ("All Files", "*.*")]
    )
    if filename:
        file_path_var.set(filename)


def start_scraping():
    file_path = file_path_var.get()
    num_browsers = num_browsers_var.get()

    if not file_path:
        messagebox.showerror("Input Error", "Please select an Excel input file.")
        return

    if not num_browsers.isdigit() or int(num_browsers) < 1:
        messagebox.showerror("Input Error", "Please enter a valid positive integer for number of browsers.")
        return

    btn_start.config(state='disabled')
    status_var.set("Scraping in progress... Please wait.")

    def task():
        try:
            run_scraping(file_path, int(num_browsers))
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred:\n{e}")
        finally:
            btn_start.config(state='normal')
            status_var.set("Idle")

    threading.Thread(target=task, daemon=True).start()


# --- GUI Setup ---

root = tk.Tk()
root.title("Greyhound Scraper Parallel")

file_path_var = tk.StringVar()
num_browsers_var = tk.StringVar(value="3")
status_var = tk.StringVar(value="Idle")

frame = tk.Frame(root, padx=10, pady=10)
frame.pack()

tk.Label(frame, text="Input Excel File:").grid(row=0, column=0, sticky='w')
entry_file = tk.Entry(frame, width=50, textvariable=file_path_var)
entry_file.grid(row=0, column=1, padx=5, pady=5)
tk.Button(frame, text="Browse...", command=browse_file).grid(row=0, column=2, padx=5)

tk.Label(frame, text="Number of Browsers:").grid(row=1, column=0, sticky='w')
entry_num = tk.Entry(frame, width=5, textvariable=num_browsers_var)
entry_num.grid(row=1, column=1, sticky='w', padx=5, pady=5)

btn_start = tk.Button(frame, text="🚀 Start Scraping", command=start_scraping)
btn_start.grid(row=2, column=0, columnspan=3, pady=10)

status_label = tk.Label(frame, textvariable=status_var, fg="blue")
status_label.grid(row=3, column=0, columnspan=3)

root.mainloop()
