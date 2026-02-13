"""
Streamlit Integration for Screener Auto Downloader
===================================================
Integrates the auto downloader into the existing Streamlit app
"""

import streamlit as st
import os
from pathlib import Path
from screener_downloader import ScreenerDownloader


def show_auto_download_section(cookies_path="screener_cookies.pkl"):
    """
    Display the auto download section in Streamlit
    
    Args:
        cookies_path: Path to cookies file
    
    Returns:
        tuple: (use_auto_download, template_file_path)
    """
    st.markdown("---")
    st.markdown("### 🤖 Auto Download from Screener.in")
    
    # Check if cookies file exists
    cookies_exists = os.path.exists(cookies_path)
    
    if not cookies_exists:
        st.warning(f"⚠️ Cookies file not found: `{cookies_path}`")
        st.info("Please place your `screener_cookies.pkl` file in the same folder as this app to enable auto download.")
        return False, None
    
    st.success(f"✅ Cookies file found: `{cookies_path}`")
    
    use_auto_download = st.checkbox(
        "🚀 Auto Download from Screener.in",
        help="Automatically download and convert Excel from Screener.in using company symbol"
    )
    
    template_file = None
    
    if use_auto_download:
        st.info("📌 **Auto Download Mode Active** - Just enter the company symbol from Screener URL")
        
        # Input for company symbol
        col1, col2 = st.columns([2, 1])
        
        with col1:
            company_symbol = st.text_input(
                "Company Symbol",
                placeholder="e.g., HONASA, NYKAA, RELIANCE",
                help="Enter the company symbol as it appears in Screener.in URL: screener.in/company/SYMBOL/"
            ).strip().upper()
        
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            download_button = st.button("📥 Download & Convert", type="primary", use_container_width=True)
        
        # Example URL display
        if company_symbol:
            st.caption(f"Will download from: `https://www.screener.in/company/{company_symbol}/`")
        
        # Download and convert
        if download_button and company_symbol:
            with st.spinner(f"🔄 Downloading data for {company_symbol}..."):
                try:
                    # Create temp directory
                    temp_dir = Path("./temp_downloads")
                    temp_dir.mkdir(exist_ok=True)
                    
                    # Download and convert
                    downloader = ScreenerDownloader(cookies_path)
                    template_path = downloader.auto_download_and_convert(
                        company_symbol,
                        output_dir=str(temp_dir),
                        keep_original=False
                    )
                    
                    if template_path and os.path.exists(template_path):
                        st.success(f"✅ Successfully downloaded and converted!")
                        
                        # Store in session state
                        st.session_state['auto_downloaded_file'] = template_path
                        st.session_state['company_symbol'] = company_symbol
                        
                        # Show download button for user
                        with open(template_path, 'rb') as f:
                            st.download_button(
                                label="💾 Download Template",
                                data=f.read(),
                                file_name=f"{company_symbol}_template.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                help="Download the converted template for your records"
                            )
                        
                        template_file = template_path
                    else:
                        st.error("❌ Failed to download and convert. Please check the company symbol and try again.")
                        
                except Exception as e:
                    st.error(f"❌ Error during auto download: {str(e)}")
                    import traceback
                    with st.expander("🐛 Debug Info"):
                        st.code(traceback.format_exc())
        
        # Use previously downloaded file if available
        elif 'auto_downloaded_file' in st.session_state:
            template_file = st.session_state['auto_downloaded_file']
            company = st.session_state.get('company_symbol', 'Unknown')
            
            if os.path.exists(template_file):
                st.info(f"📁 Using previously downloaded data for **{company}**")
                
                # Option to download again
                col1, col2 = st.columns([3, 1])
                with col2:
                    if st.button("🔄 Download Fresh Data"):
                        if 'auto_downloaded_file' in st.session_state:
                            del st.session_state['auto_downloaded_file']
                        if 'company_symbol' in st.session_state:
                            del st.session_state['company_symbol']
                        st.rerun()
            else:
                st.warning("⚠️ Previously downloaded file not found. Please download again.")
                template_file = None
    
    return use_auto_download, template_file


def integrate_with_existing_upload_section(cookies_path="screener_cookies.pkl"):
    """
    Complete integration showing both manual upload and auto download options
    
    Returns:
        tuple: (use_screener_excel, uploaded_file_or_path)
    """
    st.markdown("---")
    st.markdown("### 📊 Screener Excel Data Source")
    
    # Radio button to choose mode
    mode = st.radio(
        "Choose data input method:",
        ["🤖 Auto Download from Screener.in", "📤 Manual Upload"],
        horizontal=True
    )
    
    uploaded_file = None
    use_screener = True
    
    if mode == "🤖 Auto Download from Screener.in":
        # Check cookies
        if not os.path.exists(cookies_path):
            st.warning(f"⚠️ Cookies file not found: `{cookies_path}`")
            st.info("Place your `screener_cookies.pkl` file in the same folder as this app to enable auto download.")
            return True, None
        
        st.success(f"✅ Cookies file found")
        
        # Company symbol/ID and data type selection
        col1, col2, col3 = st.columns([3, 2, 1])
        
        with col1:
            company_symbol = st.text_input(
                "Company Symbol/ID",
                placeholder="e.g., HONASA, RELIANCE, TCS or 1285886",
                help="Enter the company symbol or ID number from Screener URL",
                key="auto_dl_symbol"
            ).strip().upper()
        
        with col2:
            data_type = st.radio(
                "Data Type:",
                ["Consolidated", "Standalone"],
                horizontal=True,
                key="auto_dl_data_type"
            )
        
        with col3:
            st.markdown("<br>", unsafe_allow_html=True)
            download_button = st.button("📥 Fetch & Analyze", type="primary", use_container_width=True)
        
        # Checkbox for ID-based URL
        use_id_url = st.checkbox(
            "Company uses ID-based URL",
            help="Check this if the company is in listing process and uses ID format: /company/id/NUMBER/",
            key="use_id_url"
        )
        
        # Show URL
        if company_symbol:
            if use_id_url:
                url_suffix = "/consolidated/" if data_type == "Consolidated" else "/"
                st.caption(f"Will download from: `https://www.screener.in/company/id/{company_symbol}{url_suffix}`")
            else:
                url_suffix = "/consolidated/" if data_type == "Consolidated" else "/"
                st.caption(f"Will download from: `https://www.screener.in/company/{company_symbol}{url_suffix}`")
        
        # Download and process
        if download_button and company_symbol:
            with st.spinner(f"🔄 Fetching {data_type.lower()} data for {company_symbol}..."):
                try:
                    temp_dir = Path("./temp_downloads")
                    temp_dir.mkdir(exist_ok=True)
                    
                    # Enable debug output
                    import sys
                    from io import StringIO
                    
                    # Capture print statements
                    old_stdout = sys.stdout
                    sys.stdout = captured_output = StringIO()
                    
                    try:
                        downloader = ScreenerDownloader(cookies_path)
                        template_path = downloader.auto_download_and_convert(
                            company_symbol,
                            output_dir=str(temp_dir),
                            keep_original=False,
                            use_consolidated=(data_type == "Consolidated"),
                            use_id_url=use_id_url
                        )
                    finally:
                        # Restore stdout and get captured output
                        sys.stdout = old_stdout
                        debug_output = captured_output.getvalue()
                    
                    # Show debug output
                    if debug_output:
                        with st.expander("📋 Download Process Log", expanded=False):
                            st.code(debug_output)
                    
                    if template_path and os.path.exists(template_path):
                        st.success(f"✅ Successfully fetched {data_type.lower()} data!")
                        
                        # Store in session state
                        st.session_state['auto_downloaded_file'] = template_path
                        st.session_state['company_symbol'] = company_symbol
                        st.session_state['data_type'] = data_type
                        
                        uploaded_file = template_path
                    else:
                        st.error("❌ Failed to fetch data. Check company symbol and try again.")
                        st.warning("⚠️ Expand 'Download Process Log' above to see what went wrong")
                        
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
        
        # Use previously downloaded file
        elif 'auto_downloaded_file' in st.session_state:
            template_file = st.session_state['auto_downloaded_file']
            company = st.session_state.get('company_symbol', 'Unknown')
            dtype = st.session_state.get('data_type', 'Unknown')
            
            if os.path.exists(template_file):
                st.info(f"📁 Using {dtype.lower()} data for **{company}**")
                uploaded_file = template_file
                
                # Option to refresh
                if st.button("🔄 Fetch Fresh Data"):
                    for key in ['auto_downloaded_file', 'company_symbol', 'data_type']:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
            else:
                st.warning("⚠️ File not found. Please fetch again.")
            
    elif mode == "📤 Manual Upload":
        st.info("📌 **Manual Upload Mode**")
        
        uploaded_file = st.file_uploader(
            "Upload Screener Excel File",
            type=['xlsx', 'xls'],
            help="Upload Excel from Screener.in",
            key="manual_upload"
        )
        
        if uploaded_file:
            st.success("✅ File uploaded!")
    
    return use_screener, uploaded_file


if __name__ == "__main__":
    st.set_page_config(page_title="Screener Auto Download Demo", layout="wide")
    
    st.title("🤖 Screener Auto Download Demo")
    
    use_screener, file = integrate_with_existing_upload_section()
    
    if use_screener and file:
        st.success(f"Ready to process: {file}")
