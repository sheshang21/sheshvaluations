"""
Stock Price vs Revenue & EPS Comparison Module
Supports both Listed (Yahoo Finance) and Screener Excel modes
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
from datetime import datetime, timedelta
import streamlit as st


def fetch_stock_prices_yahoo(ticker, num_years=4):
    """
    Fetch daily stock prices from Yahoo Finance
    
    Args:
        ticker: Stock ticker symbol (e.g., 'RELIANCE.NS')
        num_years: Number of years of data (max 4 for Yahoo Finance)
    
    Returns:
        DataFrame with date, close price, and % change
    """
    try:
        num_years = min(num_years, 4)  # Yahoo Finance limit
        end_date = datetime.now()
        start_date = end_date - timedelta(days=num_years*365)
        
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        
        if hist.empty:
            return None
        
        # Calculate daily percentage changes
        hist['pct_change'] = hist['Close'].pct_change() * 100
        
        # Reset index to get date as column
        hist = hist.reset_index()
        hist['Date'] = pd.to_datetime(hist['Date'])
        
        return hist[['Date', 'Close', 'pct_change']]
    
    except Exception as e:
        st.error(f"Error fetching stock prices: {str(e)}")
        return None


def calculate_eps_from_screener(balance_sheet_df, pnl_df, num_years=10):
    """
    Calculate EPS from Screener Excel data
    
    Args:
        balance_sheet_df: Balance sheet DataFrame
        pnl_df: Profit & Loss DataFrame
        num_years: Number of years to extract (max 10)
    
    Returns:
        DataFrame with years, net_profit, shares, EPS
    """
    try:
        num_years = min(num_years, 10)  # Screener limit
        
        # Extract Net Profit from P&L (Row with 'Net profit')
        net_profit_row = None
        for idx, row in pnl_df.iterrows():
            if row.iloc[0] and 'Net profit' in str(row.iloc[0]):
                net_profit_row = row
                break
        
        if net_profit_row is None:
            return None
        
        # Extract No. of Equity Shares from Balance Sheet
        shares_row = None
        for idx, row in balance_sheet_df.iterrows():
            if row.iloc[0] and 'No. of Equity Shares' in str(row.iloc[0]):
                shares_row = row
                break
        
        if shares_row is None:
            return None
        
        # Get year columns from P&L (skip first column which is label)
        year_row = pnl_df.iloc[1]  # Row 2 has dates
        years = []
        net_profits = []
        shares_outstanding = []
        
        for i in range(1, min(len(year_row), num_years + 1)):
            if pd.notna(year_row.iloc[i]) and pd.notna(net_profit_row.iloc[i]) and pd.notna(shares_row.iloc[i]):
                year = year_row.iloc[i]
                if isinstance(year, datetime):
                    year = year.year
                years.append(year)
                net_profits.append(float(net_profit_row.iloc[i]))
                shares_outstanding.append(float(shares_row.iloc[i]))
        
        if not years:
            return None
        
        # Calculate EPS (Net Profit is in Cr, Shares is in Cr)
        eps_values = [np / shares if shares > 0 else 0 for np, shares in zip(net_profits, shares_outstanding)]
        
        df = pd.DataFrame({
            'Year': years,
            'Net_Profit_Cr': net_profits,
            'Shares_Cr': shares_outstanding,
            'EPS': eps_values
        })
        
        return df
    
    except Exception as e:
        st.error(f"Error calculating EPS: {str(e)}")
        return None


def extract_revenue_from_screener(pnl_df, num_years=10):
    """
    Extract Revenue/Sales from Screener Excel P&L
    
    Args:
        pnl_df: Profit & Loss DataFrame
        num_years: Number of years to extract (max 10)
    
    Returns:
        DataFrame with years and revenue values
    """
    try:
        num_years = min(num_years, 10)
        
        # Extract Sales row
        sales_row = None
        for idx, row in pnl_df.iterrows():
            if row.iloc[0] and 'Sales' in str(row.iloc[0]):
                sales_row = row
                break
        
        if sales_row is None:
            return None
        
        # Get year columns
        year_row = pnl_df.iloc[1]  # Row 2 has dates
        years = []
        revenues = []
        
        for i in range(1, min(len(year_row), num_years + 1)):
            if pd.notna(year_row.iloc[i]) and pd.notna(sales_row.iloc[i]):
                year = year_row.iloc[i]
                if isinstance(year, datetime):
                    year = year.year
                years.append(year)
                revenues.append(float(sales_row.iloc[i]))
        
        if not years:
            return None
        
        df = pd.DataFrame({
            'Year': years,
            'Revenue_Cr': revenues
        })
        
        return df
    
    except Exception as e:
        st.error(f"Error extracting revenue: {str(e)}")
        return None


def identify_major_price_changes(price_df, threshold_pct=5.0):
    """
    Identify days with major price changes
    
    Args:
        price_df: DataFrame with Date, Close, pct_change
        threshold_pct: Percentage threshold for "major" change
    
    Returns:
        DataFrame with only major change dates marked
    """
    if price_df is None or price_df.empty:
        return None
    
    price_df['is_major'] = price_df['pct_change'].abs() >= threshold_pct
    return price_df


def create_stock_vs_financials_chart(stock_prices_df, revenue_df, eps_df, company_name="Company"):
    """
    Create combined chart: Stock Price (line) + Revenue (bars) + EPS (line)
    
    Args:
        stock_prices_df: Daily stock prices DataFrame
        revenue_df: Annual revenue DataFrame
        eps_df: Annual EPS DataFrame
        company_name: Name of company for title
    
    Returns:
        Plotly figure object
    """
    
    # Create figure with secondary y-axis
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]]
    )
    
    # Add Revenue as bars (primary y-axis)
    if revenue_df is not None and not revenue_df.empty:
        fig.add_trace(
            go.Bar(
                x=revenue_df['Year'],
                y=revenue_df['Revenue_Cr'],
                name='Revenue (₹ Cr)',
                marker_color='lightblue',
                opacity=0.6,
                yaxis='y'
            ),
            secondary_y=False
        )
    
    # Add Stock Price as line (secondary y-axis)
    if stock_prices_df is not None and not stock_prices_df.empty:
        # Identify major changes
        major_changes = stock_prices_df[stock_prices_df['is_major'] == True] if 'is_major' in stock_prices_df.columns else pd.DataFrame()
        
        # Main price line
        fig.add_trace(
            go.Scatter(
                x=stock_prices_df['Date'],
                y=stock_prices_df['Close'],
                name='Stock Price (₹)',
                line=dict(color='green', width=2),
                mode='lines',
                yaxis='y2'
            ),
            secondary_y=True
        )
        
        # Highlight major changes
        if not major_changes.empty:
            fig.add_trace(
                go.Scatter(
                    x=major_changes['Date'],
                    y=major_changes['Close'],
                    name='Major Price Change (≥5%)',
                    mode='markers',
                    marker=dict(
                        color='red',
                        size=8,
                        symbol='circle',
                        line=dict(color='darkred', width=1)
                    ),
                    yaxis='y2',
                    hovertemplate='<b>%{x}</b><br>Price: ₹%{y:.2f}<br>Change: %{customdata:.2f}%<extra></extra>',
                    customdata=major_changes['pct_change']
                ),
                secondary_y=True
            )
    
    # Add EPS as line (primary y-axis with revenue)
    if eps_df is not None and not eps_df.empty:
        fig.add_trace(
            go.Scatter(
                x=eps_df['Year'],
                y=eps_df['EPS'],
                name='EPS (₹)',
                line=dict(color='orange', width=2, dash='dash'),
                mode='lines+markers',
                marker=dict(size=8),
                yaxis='y'
            ),
            secondary_y=False
        )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text=f"{company_name}: Stock Price vs Revenue & EPS",
            font=dict(size=18, color='darkblue')
        ),
        xaxis_title="Time Period",
        height=600,
        width=1200,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # Update y-axes
    fig.update_yaxes(
        title_text="Revenue (₹ Cr) / EPS (₹)",
        secondary_y=False,
        gridcolor='lightgray'
    )
    
    fig.update_yaxes(
        title_text="Stock Price (₹)",
        secondary_y=True,
        gridcolor='lightgray'
    )
    
    fig.update_xaxes(gridcolor='lightgray')
    
    return fig


def get_stock_comparison_data_listed(ticker, company_name, financials, num_years=4):
    """
    Get all data needed for listed company stock comparison
    
    Args:
        ticker: Yahoo Finance ticker
        company_name: Company name
        financials: Financials dict from main analysis
        num_years: Number of years (max 4 for Yahoo)
    
    Returns:
        dict with stock_prices_df, revenue_df, eps_df, chart_fig
    """
    try:
        num_years = min(num_years, 4)
        
        # Fetch stock prices
        stock_prices_df = fetch_stock_prices_yahoo(ticker, num_years)
        if stock_prices_df is not None:
            stock_prices_df = identify_major_price_changes(stock_prices_df, threshold_pct=5.0)
        
        # Extract Revenue from financials
        revenue_df = None
        if 'years' in financials and 'revenue' in financials:
            years = financials['years'][-num_years:]
            revenues = financials['revenue'][-num_years:]
            revenue_df = pd.DataFrame({
                'Year': years,
                'Revenue_Cr': revenues
            })
        
        # Extract EPS from financials
        eps_df = None
        if 'years' in financials and 'net_income' in financials and 'shares_outstanding' in financials:
            years = financials['years'][-num_years:]
            net_incomes = financials['net_income'][-num_years:]
            shares = financials['shares_outstanding'][-num_years:]
            
            eps_values = [ni / sh if sh > 0 else 0 for ni, sh in zip(net_incomes, shares)]
            eps_df = pd.DataFrame({
                'Year': years,
                'EPS': eps_values
            })
        
        # Create chart
        chart_fig = None
        if stock_prices_df is not None or revenue_df is not None or eps_df is not None:
            chart_fig = create_stock_vs_financials_chart(
                stock_prices_df, revenue_df, eps_df, company_name
            )
        
        return {
            'stock_prices_df': stock_prices_df,
            'revenue_df': revenue_df,
            'eps_df': eps_df,
            'chart_fig': chart_fig
        }
    
    except Exception as e:
        st.error(f"Error preparing stock comparison data: {str(e)}")
        return None


def get_stock_comparison_data_screener(ticker, company_name, balance_sheet_df, pnl_df, num_years=10):
    """
    Get all data needed for screener mode stock comparison
    
    Args:
        ticker: Stock ticker for Yahoo Finance
        company_name: Company name
        balance_sheet_df: Balance sheet DataFrame from Excel
        pnl_df: P&L DataFrame from Excel
        num_years: Number of years (max 10 for Screener)
    
    Returns:
        dict with stock_prices_df, revenue_df, eps_df, chart_fig
    """
    try:
        num_years = min(num_years, 10)
        
        # Fetch stock prices (limited to available financial years)
        stock_prices_df = fetch_stock_prices_yahoo(ticker, num_years)
        if stock_prices_df is not None:
            stock_prices_df = identify_major_price_changes(stock_prices_df, threshold_pct=5.0)
        
        # Extract Revenue
        revenue_df = extract_revenue_from_screener(pnl_df, num_years)
        
        # Calculate EPS
        eps_df = calculate_eps_from_screener(balance_sheet_df, pnl_df, num_years)
        
        # Create chart
        chart_fig = None
        if stock_prices_df is not None or revenue_df is not None or eps_df is not None:
            chart_fig = create_stock_vs_financials_chart(
                stock_prices_df, revenue_df, eps_df, company_name
            )
        
        return {
            'stock_prices_df': stock_prices_df,
            'revenue_df': revenue_df,
            'eps_df': eps_df,
            'chart_fig': chart_fig
        }
    
    except Exception as e:
        st.error(f"Error preparing stock comparison data: {str(e)}")
        return None
