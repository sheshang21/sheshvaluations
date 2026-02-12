"""
Screener.in Auto Downloader with Excel Converter
=================================================
Downloads Excel from Screener.in, removes blank columns, converts to template format
"""

import requests
from bs4 import BeautifulSoup
import pickle
import os
import re
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows


class ScreenerDownloader:
    """Downloads Excel files from Screener.in with authentication"""
    
    def __init__(self, cookies_path="screener_cookies.pkl"):
        """
        Initialize downloader with cookies
        
        Args:
            cookies_path: Path to pickled cookies file
        """
        self.cookies_path = cookies_path
        self.session = requests.Session()
        self._load_cookies()
        
    def _load_cookies(self):
        """Load cookies from pickle file"""
        if not os.path.exists(self.cookies_path):
            raise FileNotFoundError(f"Cookies file not found: {self.cookies_path}")
        
        with open(self.cookies_path, 'rb') as f:
            cookies = pickle.load(f)
        
        # Convert to requests cookies
        if isinstance(cookies, list):
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
        elif isinstance(cookies, dict):
            for name, value in cookies.items():
                self.session.cookies.set(name, value)
    
    def download_excel(self, company_symbol, output_path=None, use_consolidated=False):
        """
        Download Excel file from Screener.in by clicking Export button
        
        Args:
            company_symbol: Company symbol (e.g., 'HONASA')
            output_path: Where to save the file (optional)
            use_consolidated: Use consolidated financials (default: False)
            
        Returns:
            str: Path to downloaded file or None if failed
        """
        # Construct URL based on consolidated flag
        url_suffix = "consolidated/" if use_consolidated else ""
        company_url = f"https://www.screener.in/company/{company_symbol}/{url_suffix}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.screener.in/'
        }
        
        try:
            print(f"Accessing: {company_url}")
            response = self.session.get(company_url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"Error: Could not access page (Status: {response.status_code})")
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find export button
            export_button = soup.find('button', {'formaction': re.compile(r'/user/company/export/\d+/')})
            
            if not export_button:
                print("Error: Could not find export button")
                return None
            
            formaction = export_button.get('formaction')
            print(f"Found export URL: {formaction}")
            
            # Get CSRF token
            form = export_button.find_parent('form')
            csrf_token = None
            if form:
                csrf_input = form.find('input', {'name': 'csrfmiddlewaretoken'})
                if csrf_input:
                    csrf_token = csrf_input.get('value')
            
            if not csrf_token:
                csrf_token = self.session.cookies.get('csrftoken', '')
            
            # POST to export URL
            export_url = f"https://www.screener.in{formaction}"
            
            post_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
                'Referer': company_url,
                'Origin': 'https://www.screener.in'
            }
            
            post_data = {
                'csrfmiddlewaretoken': csrf_token,
                'next': f'/company/{company_symbol}/{url_suffix}'
            }
            
            print(f"Downloading from: {export_url}")
            download_response = self.session.post(export_url, headers=post_headers, data=post_data, timeout=30)
            
            if download_response.status_code != 200:
                print(f"Error: Download failed (Status: {download_response.status_code})")
                return None
            
            # Save file
            if output_path is None:
                output_path = f"{company_symbol}_screener.xlsx"
            
            with open(output_path, 'wb') as f:
                f.write(download_response.content)
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                print(f"✓ Downloaded: {output_path} ({os.path.getsize(output_path)} bytes)")
                return output_path
            else:
                print("Error: File empty or not created")
                return None
                
        except Exception as e:
            print(f"Error downloading: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def remove_empty_year_columns(self, excel_path):
        """
        Remove columns from Data Sheet that don't have data in financial rows
        
        Args:
            excel_path: Path to Excel file
            
        Returns:
            bool: True if successful
        """
        try:
            wb = load_workbook(excel_path)
            
            if 'Data Sheet' not in wb.sheetnames:
                print("Data Sheet not found")
                wb.close()
                return False
            
            ws = wb['Data Sheet']
            
            # Find P&L section
            pl_date_row = None
            for i in range(1, 50):
                val = ws.cell(i, 1).value
                if val and 'Report Date' in str(val):
                    pl_date_row = i
                    break
            
            if not pl_date_row:
                print("Could not find Report Date row")
                wb.close()
                return False
            
            # Check which columns have actual year data
            cols_to_delete = []
            for col in range(2, ws.max_column + 1):
                date_val = ws.cell(pl_date_row, col).value
                
                # Check if this column has a valid date
                has_valid_date = False
                if date_val:
                    if hasattr(date_val, 'year'):
                        has_valid_date = True
                    else:
                        if re.search(r'20\d{2}', str(date_val)):
                            has_valid_date = True
                
                if not has_valid_date:
                    cols_to_delete.append(col)
                    continue
                
                # Also check if column has any financial data (check Sales row)
                sales_row = pl_date_row + 1
                has_data = False
                for check_row in range(sales_row, min(sales_row + 10, ws.max_row + 1)):
                    val = ws.cell(check_row, col).value
                    if val and val != 0:
                        try:
                            float(val)
                            has_data = True
                            break
                        except:
                            pass
                
                if not has_data:
                    cols_to_delete.append(col)
            
            # Delete columns in reverse order
            for col in sorted(cols_to_delete, reverse=True):
                ws.delete_cols(col)
                print(f"✓ Deleted empty column {col}")
            
            if cols_to_delete:
                wb.save(excel_path)
                print(f"✓ Removed {len(cols_to_delete)} empty columns")
            else:
                print("No empty columns to remove")
            
            wb.close()
            return True
            
        except Exception as e:
            print(f"Error removing empty columns: {e}")
            return False
    
    def remove_blank_columns(self, excel_path):
        """Legacy method - calls remove_empty_year_columns"""
        return self.remove_empty_year_columns(excel_path)
    
    def convert_to_template(self, screener_excel_path, output_path=None):
        """
        Convert Screener Data Sheet to EXACT target format
        
        Target format:
        - Sheet 1: "Balance Sheet" with title row, Report Date row, then data
        - Sheet 2: "Profit and Loss Account" with title row, Report Date row, then data
        """
        try:
            from openpyxl import load_workbook, Workbook
            
            # Load source file
            src_wb = load_workbook(screener_excel_path, data_only=True)
            
            if 'Data Sheet' not in src_wb.sheetnames:
                print("Error: Data Sheet not found")
                src_wb.close()
                return None
            
            src_ws = src_wb['Data Sheet']
            
            # Find sections in Data Sheet
            pl_date_row = None
            bs_date_row = None
            
            for i in range(1, 100):
                val = src_ws.cell(i, 1).value
                if val:
                    val_str = str(val).upper()
                    if ('PROFIT' in val_str or 'P&L' in val_str or 'P & L' in val_str) and pl_date_row is None:
                        # Next row is Report Date
                        if src_ws.cell(i + 1, 1).value and 'Report Date' in str(src_ws.cell(i + 1, 1).value):
                            pl_date_row = i + 1
                    elif 'BALANCE' in val_str and bs_date_row is None:
                        if src_ws.cell(i + 1, 1).value and 'Report Date' in str(src_ws.cell(i + 1, 1).value):
                            bs_date_row = i + 1
            
            if not pl_date_row or not bs_date_row:
                print(f"Error: Sections not found. PL:{pl_date_row}, BS:{bs_date_row}")
                src_wb.close()
                return None
            
            print(f"P&L date row: {pl_date_row}, BS date row: {bs_date_row}")
            
            # Find first column with data (skip empty columns at start)
            first_data_col = None
            for col in range(2, src_ws.max_column + 1):
                val = src_ws.cell(pl_date_row, col).value
                if val:
                    first_data_col = col
                    break
            
            if not first_data_col:
                print("Error: No data columns found")
                src_wb.close()
                return None
            
            # Extract all dates/years from Report Date row
            dates = []
            date_cols = []
            for col in range(first_data_col, src_ws.max_column + 1):
                val = src_ws.cell(pl_date_row, col).value
                if val:
                    dates.append(val)
                    date_cols.append(col)
            
            if not dates:
                print("Error: No dates found")
                src_wb.close()
                return None
            
            print(f"✓ Found {len(dates)} date columns starting at column {first_data_col}")
            
            # Create new workbook
            new_wb = Workbook()
            new_wb.remove(new_wb.active)  # Remove default sheet
            
            # ============== BALANCE SHEET (First Sheet) ==============
            bs_ws = new_wb.create_sheet("Balance Sheet", 0)
            
            # Row 1: Title
            bs_ws['A1'] = 'BALANCE SHEET'
            
            # Row 2: Report Date header + dates
            bs_ws['A2'] = 'Report Date'
            for idx, date_val in enumerate(dates, start=2):
                bs_ws.cell(2, idx).value = date_val
            
            # Define Balance Sheet items IN EXACT ORDER from target
            bs_items = [
                'Equity Share Capital',
                'Reserves',
                'Borrowings',
                'Other Liabilities',
                'Total',  # First Total = Total Liabilities
                'Net Block',
                'Capital Work in Progress',
                'Investments',
                'Other Assets',
                'Total',  # Second Total = Total Assets
                'Receivables',
                'Inventory',
                'Cash & Bank',
                'No. of Equity Shares',
                'New Bonus Shares',
                'Face value'
            ]
            
            # Copy Balance Sheet data
            current_target_row = 3
            total_count = 0
            
            for item_name in bs_items:
                bs_ws.cell(current_target_row, 1).value = item_name
                
                # Find this item in source
                for src_row in range(bs_date_row + 1, min(bs_date_row + 25, src_ws.max_row + 1)):
                    src_item = src_ws.cell(src_row, 1).value
                    if src_item:
                        src_item_str = str(src_item).strip()
                        
                        # Handle "Total" - need to track which one
                        if item_name == 'Total':
                            if src_item_str == 'Total':
                                total_count += 1
                                # First Total = row 7, Second Total = row 12
                                if (current_target_row == 7 and total_count == 1) or \
                                   (current_target_row == 12 and total_count == 2):
                                    # Copy data
                                    for idx, src_col in enumerate(date_cols, start=2):
                                        val = src_ws.cell(src_row, src_col).value
                                        bs_ws.cell(current_target_row, idx).value = val
                                    break
                        else:
                            # Normal item matching
                            if src_item_str == item_name:
                                # Copy data
                                for idx, src_col in enumerate(date_cols, start=2):
                                    val = src_ws.cell(src_row, src_col).value
                                    bs_ws.cell(current_target_row, idx).value = val
                                break
                
                current_target_row += 1
            
            # ============== PROFIT AND LOSS ACCOUNT (Second Sheet) ==============
            pl_ws = new_wb.create_sheet("Profit and Loss Account", 1)
            
            # Row 1: Title
            pl_ws['A1'] = 'PROFIT & LOSS'
            
            # Row 2: Report Date header + dates
            pl_ws['A2'] = 'Report Date'
            for idx, date_val in enumerate(dates, start=2):
                pl_ws.cell(2, idx).value = date_val
            
            # Define P&L items IN EXACT ORDER from target
            pl_items = [
                'Sales',
                'Raw Material Cost',
                'Change in Inventory',
                'Power and Fuel',
                'Other Mfr. Exp',
                'Employee Cost',
                'Selling and admin',
                'Other Expenses',
                'Other Income',
                'Depreciation',
                'Interest',
                'Profit before tax',
                'Tax',
                'Net profit',
                'Dividend Amount'
            ]
            
            # Copy P&L data
            current_target_row = 3
            
            for item_name in pl_items:
                pl_ws.cell(current_target_row, 1).value = item_name
                
                # Find this item in source
                for src_row in range(pl_date_row + 1, min(pl_date_row + 30, src_ws.max_row + 1)):
                    src_item = src_ws.cell(src_row, 1).value
                    if src_item and str(src_item).strip() == item_name:
                        # Copy data
                        for idx, src_col in enumerate(date_cols, start=2):
                            val = src_ws.cell(src_row, src_col).value
                            pl_ws.cell(current_target_row, idx).value = val
                        break
                
                current_target_row += 1
            
            # Save
            if output_path is None:
                base_name = os.path.splitext(os.path.basename(screener_excel_path))[0]
                output_path = f"{base_name}_template.xlsx"
            
            new_wb.save(output_path)
            new_wb.close()
            src_wb.close()
            
            print(f"✓ Template created: {output_path}")
            print(f"  - Sheet 1: Balance Sheet ({len(bs_items)} rows)")
            print(f"  - Sheet 2: Profit and Loss Account ({len(pl_items)} rows)")
            
            return output_path
            
        except Exception as e:
            print(f"Error converting: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _setup_template_sheet(self, ws, years, sheet_type):
        """Legacy method - no longer used"""
        pass
    
    def _populate_balance_sheet(self, ws, df, year_row_idx, years):
        """Legacy method - no longer used"""
        pass
    
    def _populate_pl_sheet(self, ws, df, year_row_idx, years):
        """Legacy method - no longer used"""
        pass
    
    def _find_item_values(self, df, item_name, year_row_idx, years):
        """Legacy method - no longer used"""
        pass
    
    def auto_download_and_convert(self, company_symbol, output_dir=".", keep_original=False, use_consolidated=False):
        """
        Complete workflow: download, clean, convert to ready-to-use format
        
        Args:
            company_symbol: Company symbol (e.g., 'HONASA')
            output_dir: Directory for output files
            keep_original: Keep original downloaded Excel
            use_consolidated: Use consolidated financials
            
        Returns:
            str: Path to ready-to-use Excel file or None if failed
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Download Excel
            original_path = os.path.join(output_dir, f"{company_symbol}_original.xlsx")
            downloaded_path = self.download_excel(company_symbol, original_path, use_consolidated)
            
            if not downloaded_path:
                return None
            
            # Remove empty columns
            self.remove_empty_year_columns(downloaded_path)
            
            # Convert to template format
            template_path = os.path.join(output_dir, f"{company_symbol}_template.xlsx")
            converted_path = self.convert_to_template(downloaded_path, template_path)
            
            # Clean up original if not needed
            if not keep_original and os.path.exists(downloaded_path):
                os.remove(downloaded_path)
                print(f"✓ Removed original file")
            
            return converted_path
            
        except Exception as e:
            print(f"Error in workflow: {e}")
            import traceback
            traceback.print_exc()
            return None


def download_screener_data(company_symbol, cookies_path="screener_cookies.pkl", output_dir="."):
    """
    Convenience function for quick downloads
    
    Args:
        company_symbol: Company symbol
        cookies_path: Path to cookies file
        output_dir: Output directory
        
    Returns:
        str: Path to template file
    """
    downloader = ScreenerDownloader(cookies_path)
    return downloader.auto_download_and_convert(company_symbol, output_dir)


if __name__ == "__main__":
    print("Screener Downloader Module")
    print("Usage: from screener_downloader import download_screener_data")
