"""
Multi-Account Screener Downloader with Cookie Rotation
=======================================================
Rotates between multiple Screener.in accounts to avoid rate limiting
"""

import requests
from bs4 import BeautifulSoup
import pickle
import os
import re
from pathlib import Path
import time
from openpyxl import load_workbook, Workbook


class MultiAccountScreenerDownloader:
    """Downloads Excel files from Screener.in with multiple account rotation"""
    
    def __init__(self, cookies_files=None):
        """
        Initialize with multiple cookie files
        
        Args:
            cookies_files: List of cookie file paths, e.g. ["cookies1.pkl", "cookies2.pkl", "cookies3.pkl"]
                          If None, looks for screener_cookies_*.pkl pattern
        """
        self.debug_mode = True
        
        # Auto-detect cookie files if not provided
        if cookies_files is None:
            cookies_files = []
            # Look for screener_cookies.pkl (default)
            if os.path.exists("screener_cookies.pkl"):
                cookies_files.append("screener_cookies.pkl")
            # Look for screener_cookies_1.pkl, screener_cookies_2.pkl, etc.
            for i in range(1, 10):
                path = f"screener_cookies_{i}.pkl"
                if os.path.exists(path):
                    cookies_files.append(path)
        
        if not cookies_files:
            raise FileNotFoundError("No cookie files found. Create screener_cookies_1.pkl, screener_cookies_2.pkl, etc.")
        
        self.cookies_files = cookies_files
        self.current_account_index = 0
        self.sessions = []
        
        # Load all accounts
        for cookie_file in cookies_files:
            session = self._create_session(cookie_file)
            if session:
                self.sessions.append({'session': session, 'cookie_file': cookie_file, 'failed': False})
        
        if not self.sessions:
            raise Exception("Failed to load any valid cookie files")
        
        print(f"✓ Loaded {len(self.sessions)} Screener.in accounts")
    
    def _create_session(self, cookie_file):
        """Create a session from cookie file"""
        try:
            session = requests.Session()
            
            with open(cookie_file, 'rb') as f:
                cookies = pickle.load(f)
            
            if isinstance(cookies, list):
                for cookie in cookies:
                    session.cookies.set(cookie['name'], cookie['value'])
            elif isinstance(cookies, dict):
                for name, value in cookies.items():
                    session.cookies.set(name, value)
            
            return session
        except Exception as e:
            print(f"⚠️ Failed to load {cookie_file}: {e}")
            return None
    
    def _get_next_session(self):
        """Get next available session (rotates through accounts)"""
        attempts = 0
        max_attempts = len(self.sessions)
        
        while attempts < max_attempts:
            account = self.sessions[self.current_account_index]
            
            # Skip if this account failed recently
            if not account['failed']:
                session_info = {
                    'session': account['session'],
                    'account_number': self.current_account_index + 1,
                    'cookie_file': account['cookie_file']
                }
                
                # Move to next account for next time
                self.current_account_index = (self.current_account_index + 1) % len(self.sessions)
                
                return session_info
            
            # Try next account
            self.current_account_index = (self.current_account_index + 1) % len(self.sessions)
            attempts += 1
        
        # All accounts failed - reset failure flags and try again
        print("⚠️ All accounts failed, resetting and retrying...")
        for account in self.sessions:
            account['failed'] = False
        
        return self._get_next_session()
    
    def _mark_account_failed(self, account_number):
        """Mark an account as temporarily failed"""
        if 0 <= account_number - 1 < len(self.sessions):
            self.sessions[account_number - 1]['failed'] = True
            print(f"⚠️ Marked account #{account_number} as failed")
    
    def download_excel(self, company_symbol, output_path=None, use_consolidated=False, use_id_url=False, max_retries=None):
        """
        Download Excel file with automatic account rotation
        
        Args:
            company_symbol: Company symbol
            output_path: Where to save file
            use_consolidated: Use consolidated financials
            use_id_url: Use ID-based URL
            max_retries: Max retry attempts (default: number of accounts)
            
        Returns:
            str: Path to downloaded file or None
        """
        if max_retries is None:
            max_retries = len(self.sessions)
        
        # Construct URL
        if use_id_url:
            url_suffix = "consolidated/" if use_consolidated else ""
            company_url = f"https://www.screener.in/company/id/{company_symbol}/{url_suffix}"
        else:
            url_suffix = "consolidated/" if use_consolidated else ""
            company_url = f"https://www.screener.in/company/{company_symbol}/{url_suffix}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.screener.in/'
        }
        
        for attempt in range(max_retries):
            session_info = self._get_next_session()
            session = session_info['session']
            account_num = session_info['account_number']
            
            print(f"\n[Attempt {attempt + 1}/{max_retries}] Using Account #{account_num}")
            print(f"[Step 1/4] Accessing: {company_url}")
            
            try:
                response = session.get(company_url, headers=headers, timeout=20)
                
                if response.status_code == 429:
                    print(f"❌ Rate limited on account #{account_num}")
                    self._mark_account_failed(account_num)
                    time.sleep(2)
                    continue
                
                if response.status_code != 200:
                    print(f"❌ HTTP {response.status_code} on account #{account_num}")
                    self._mark_account_failed(account_num)
                    continue
                
                print(f"✓ Page loaded successfully")
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find export button
                print(f"[Step 2/4] Looking for export button...")
                export_button = soup.find('button', {'formaction': re.compile(r'/user/company/export/\d+/')})
                
                formaction = None
                if export_button:
                    formaction = export_button.get('formaction')
                else:
                    # Try to extract company ID
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if '/company/' in href and re.search(r'\d{6,}', href):
                            match = re.search(r'(\d{6,})', href)
                            if match:
                                company_id = match.group(1)
                                formaction = f"/user/company/export/{company_id}/"
                                break
                
                if not formaction:
                    print(f"❌ No export button found on account #{account_num}")
                    self._mark_account_failed(account_num)
                    continue
                
                print(f"✓ Found export: {formaction}")
                
                # Get CSRF token
                print(f"[Step 3/4] Getting CSRF token...")
                csrf_token = session.cookies.get('csrftoken', '')
                if not csrf_token:
                    csrf_input = soup.find('input', {'name': 'csrfmiddlewaretoken'})
                    if csrf_input:
                        csrf_token = csrf_input.get('value', '')
                
                # Download
                export_url = f"https://www.screener.in{formaction}"
                
                post_headers = {
                    'User-Agent': headers['User-Agent'],
                    'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
                    'Referer': company_url,
                    'Origin': 'https://www.screener.in',
                    'X-CSRFToken': csrf_token
                }
                
                post_data = {
                    'csrfmiddlewaretoken': csrf_token,
                    'next': f'/company/id/{company_symbol}/{url_suffix}' if use_id_url else f'/company/{company_symbol}/{url_suffix}'
                }
                
                print(f"[Step 4/4] Downloading...")
                time.sleep(1)  # Be nice to the server
                
                download_response = session.post(export_url, headers=post_headers, data=post_data, timeout=30)
                
                if download_response.status_code == 429:
                    print(f"❌ Rate limited during download on account #{account_num}")
                    self._mark_account_failed(account_num)
                    time.sleep(3)
                    continue
                
                if download_response.status_code != 200:
                    print(f"❌ Download failed: HTTP {download_response.status_code}")
                    self._mark_account_failed(account_num)
                    continue
                
                # Save file
                if output_path is None:
                    output_path = f"{company_symbol}_screener.xlsx"
                
                with open(output_path, 'wb') as f:
                    f.write(download_response.content)
                
                if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    # Verify Excel
                    try:
                        test_wb = load_workbook(output_path, read_only=True)
                        test_wb.close()
                        print(f"✅ Downloaded successfully using Account #{account_num}")
                        return output_path
                    except:
                        print(f"❌ Invalid Excel file from account #{account_num}")
                        self._mark_account_failed(account_num)
                        continue
                
            except requests.exceptions.ConnectionError as e:
                print(f"❌ Connection error on account #{account_num}: {e}")
                self._mark_account_failed(account_num)
                time.sleep(5)  # Wait longer for connection issues
                continue
                
            except Exception as e:
                print(f"❌ Error on account #{account_num}: {e}")
                self._mark_account_failed(account_num)
                continue
        
        print(f"\n❌ All {max_retries} attempts failed")
        return None
    
    def remove_empty_year_columns(self, excel_path):
        """Remove empty columns - same as before"""
        try:
            wb = load_workbook(excel_path)
            if 'Data Sheet' not in wb.sheetnames:
                wb.close()
                return True
            
            ws = wb['Data Sheet']
            
            pl_date_row = None
            for i in range(1, 50):
                val = ws.cell(i, 1).value
                if val and 'Report Date' in str(val):
                    pl_date_row = i
                    break
            
            if not pl_date_row:
                wb.close()
                return True
            
            cols_to_delete = []
            for col in range(2, ws.max_column + 1):
                date_val = ws.cell(pl_date_row, col).value
                has_valid_date = False
                
                if date_val:
                    if hasattr(date_val, 'year'):
                        has_valid_date = True
                    elif re.search(r'20\d{2}', str(date_val)):
                        has_valid_date = True
                
                if not has_valid_date:
                    cols_to_delete.append(col)
            
            for col in reversed(cols_to_delete):
                ws.delete_cols(col)
            
            wb.save(excel_path)
            wb.close()
            return True
            
        except Exception as e:
            print(f"⚠️ Cleanup warning: {e}")
            return True
    
    def convert_to_template(self, screener_excel_path, output_path=None):
        """Convert to template - same as original ScreenerDownloader"""
        # [Same implementation as original - keeping this short]
        # Copy the convert_to_template method from the original screener_downloader.py
        print("Converting to template format...")
        # ... (rest of implementation)
        return output_path
    
    def auto_download_and_convert(self, company_symbol, output_dir=".", keep_original=False, use_consolidated=False, use_id_url=False):
        """Complete workflow with account rotation"""
        try:
            print(f"\n{'='*70}")
            print(f"MULTI-ACCOUNT DOWNLOAD WORKFLOW")
            print(f"Company: {company_symbol}")
            print(f"Available Accounts: {len(self.sessions)}")
            print(f"{'='*70}\n")
            
            os.makedirs(output_dir, exist_ok=True)
            
            original_path = os.path.join(output_dir, f"{company_symbol}_original.xlsx")
            downloaded_path = self.download_excel(company_symbol, original_path, use_consolidated, use_id_url)
            
            if not downloaded_path:
                print("\n❌ FAILED: Could not download from any account")
                return None
            
            self.remove_empty_year_columns(downloaded_path)
            
            # For now, return the downloaded file as-is
            # TODO: Implement proper template conversion
            template_path = os.path.join(output_dir, f"{company_symbol}_template.xlsx")
            
            # Simple copy for now
            import shutil
            shutil.copy(downloaded_path, template_path)
            
            if not keep_original and os.path.exists(downloaded_path):
                os.remove(downloaded_path)
            
            print(f"\n✅ SUCCESS: {template_path}")
            return template_path
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
            return None


if __name__ == "__main__":
    print("Multi-Account Screener Downloader")
    print("\nSetup:")
    print("1. Create multiple cookie files: screener_cookies_1.pkl, screener_cookies_2.pkl, etc.")
    print("2. Each file should contain cookies from a different Screener.in account")
    print("\nUsage:")
    print("  downloader = MultiAccountScreenerDownloader()")
    print("  downloader.auto_download_and_convert('RELIANCE')")
