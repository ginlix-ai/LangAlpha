import os
import sys
import logging
import multiprocessing
from typing import Dict, Any
from langchain_core.tools import tool

# Get module-level logger
logger = logging.getLogger(__name__)

# Simplified Python code execution tool
# Now only pre-configures necessary connections, other libraries imported by user code

def _run_code_in_process(code: str, result_queue: multiprocessing.Queue, file_uuid_name: str) -> None:
    """
    Runs the provided code string in the current process with redirected stdout/stderr.
    Results are sent back via result_queue instead of shared dict.
    """
    # Suppress ALL warnings first, before any imports
    import warnings
    warnings.filterwarnings('ignore')

    # Basic library imports
    import sys
    import io
    import os
    import re
    import glob
    import pandas as pd
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')  # Set backend before pyplot import
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import matplotlib.font_manager as fm
    import datetime
    import json
    import requests

    # Financial analysis libraries
    import mplfinance as mpf

    # Machine learning libraries
    import sklearn
    from sklearn import linear_model, ensemble, metrics, preprocessing, model_selection
    import scipy
    import statsmodels.api as sm
    import statsmodels.tsa.stattools as ts_tools
    from statsmodels.tsa.arima.model import ARIMA

    # Visualization libraries
    import plotly.express as px
    import plotly.graph_objects as go
    import plotly
    from tabulate import tabulate as tab_func

    # FMP API client (async)
    from src.data_client.fmp import FMPClient
    import asyncio


    # Set logging level
    import logging
    logging.getLogger().setLevel(logging.WARNING)

    # Create event loop for this subprocess (for async FMPClient)
    _subprocess_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_subprocess_loop)

    # Initialize async FMP client
    _async_fmp_client = FMPClient()

    class SyncFMPClient:
        """Synchronous wrapper for async FMPClient in subprocess execution context.

        Provides synchronous interface to async FMPClient methods by running them
        in the subprocess event loop. This allows user code to call client methods
        synchronously (e.g., client.get_quote('AAPL')) while the underlying client
        uses async httpx with HTTP/2.
        """

        def __init__(self, async_client: FMPClient, loop: asyncio.AbstractEventLoop):
            self._async_client = async_client
            self._loop = loop

        def __getattr__(self, name):
            """Delegate attribute access to async client and wrap async methods."""
            attr = getattr(self._async_client, name)
            if asyncio.iscoroutinefunction(attr):
                def sync_wrapper(*args, **kwargs):
                    return self._loop.run_until_complete(attr(*args, **kwargs))
                return sync_wrapper
            return attr

    # Create sync wrapper for user code
    client = SyncFMPClient(_async_fmp_client, _subprocess_loop)
    
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # Execution tracker - records key information during execution
    execution_tracker = {
        "files_created": [],
        "charts_generated": 0,
        "dataframes_created": 0,
        "queries_executed": 0,
        "api_calls_made": 0,
        "calculations_done": 0,
        "partial_output": "",
        "execution_milestones": []
    }

    try:
        # Redirect stdout and stderr to buffers
        output_buffer = io.StringIO()
        error_buffer = io.StringIO()
        sys.stdout = output_buffer
        sys.stderr = error_buffer

        # Import unified data fetching function from shared module (async)
        from src.tools.market_data.data_fetcher import get_stock_data as _get_stock_data_async

        # Create execution environment - pre-configured with FMP client and convenience functions
        # Unified data fetching function (wrapper to maintain backward compatibility)
        def get_stock_data(symbol: str, interval: str = '1day', start_date: str = None, end_date: str = None):
            """
            Get stock OHLCV data for any time interval as mplfinance-ready DataFrame.

            Args:
                symbol: Stock ticker (e.g., 'AAPL', 'MSFT')
                interval: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day', 'daily'
                start_date: Start date (YYYY-MM-DD format)
                end_date: End date (YYYY-MM-DD format)

            Returns:
                pandas DataFrame with:
                - DatetimeIndex (date column as index)
                - Columns: Open, High, Low, Close, Volume (uppercase, mplfinance-compatible)
                - Ready for use with mplfinance.plot()

            Example:
                import mplfinance as mpf
                df = get_stock_data('AAPL', interval='1day', start_date='2025-01-01')
                mpf.plot(df, type='candle', volume=True)
            """
            import pandas as pd

            # Use shared get_stock_data function (async, wrapped for sync access)
            df = _subprocess_loop.run_until_complete(
                _get_stock_data_async(symbol, interval, start_date, end_date, fmp_client=_async_fmp_client)
            )

            if df.empty:
                return pd.DataFrame()

            # Ensure mplfinance-ready format: uppercase columns, DatetimeIndex
            # Rename columns to uppercase (Open, High, Low, Close, Volume)
            column_mapping = {col: col.capitalize() for col in df.columns}
            df = df.rename(columns=column_mapping)

            # Ensure date index is DatetimeIndex
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            return df

        def get_asset_data(symbol: str, asset_type: str, interval: str = 'daily', from_date: str = None, to_date: str = None):
            """
            Get OHLCV data for any asset type (commodities, crypto, forex) as mplfinance-ready DataFrame.

            Args:
                symbol: Asset symbol (e.g., 'GCUSD', 'BTCUSD', 'EURUSD')
                asset_type: Asset type - one of: 'commodity', 'crypto', 'forex'
                interval: Time interval - '1min', '5min', '1hour', 'daily', '1day'
                from_date: Start date (YYYY-MM-DD format)
                to_date: End date (YYYY-MM-DD format)

            Returns:
                pandas DataFrame with:
                - DatetimeIndex (date column as index)
                - Columns: Open, High, Low, Close, Volume (uppercase, mplfinance-compatible)
                - Ready for use with mplfinance.plot()

            Examples:
                import mplfinance as mpf

                # Commodity data
                gold_df = get_asset_data('GCUSD', asset_type='commodity', interval='daily')
                mpf.plot(gold_df, type='candle', volume=True, title='Gold Price')

                # Crypto data
                btc_df = get_asset_data('BTCUSD', asset_type='crypto', interval='1hour')
                mpf.plot(btc_df, type='candle', volume=True, title='Bitcoin Price')

                # Forex data
                eur_df = get_asset_data('EURUSD', asset_type='forex', interval='5min')
                mpf.plot(eur_df, type='candle', volume=True, title='EUR/USD')
            """
            import pandas as pd

            # Validate asset type
            valid_asset_types = ['stock', 'commodity', 'crypto', 'forex']
            if asset_type not in valid_asset_types:
                raise ValueError(f"Invalid asset_type: {asset_type}. Must be one of: {', '.join(valid_asset_types)}")

            # Normalize interval format and fetch data
            if interval in ['daily', '1day']:
                # Use daily endpoint based on asset type
                if asset_type == 'stock':
                    data = client.get_stock_price(symbol, from_date=from_date, to_date=to_date)
                elif asset_type == 'commodity':
                    data = client.get_commodity_price(symbol, from_date=from_date, to_date=to_date)
                elif asset_type == 'crypto':
                    data = client.get_crypto_price(symbol, from_date=from_date, to_date=to_date)
                elif asset_type == 'forex':
                    data = client.get_forex_price(symbol, from_date=from_date, to_date=to_date)
            elif interval in ['1min', '5min', '1hour']:
                # Use intraday endpoint based on asset type
                if asset_type == 'stock':
                    data = client.get_intraday_chart(symbol, interval=interval, from_date=from_date, to_date=to_date)
                elif asset_type == 'commodity':
                    data = client.get_commodity_intraday_chart(symbol, interval=interval, from_date=from_date, to_date=to_date)
                elif asset_type == 'crypto':
                    data = client.get_crypto_intraday_chart(symbol, interval=interval, from_date=from_date, to_date=to_date)
                elif asset_type == 'forex':
                    data = client.get_forex_intraday_chart(symbol, interval=interval, from_date=from_date, to_date=to_date)
            else:
                raise ValueError(f"Unsupported interval: {interval}. Supported: '1min', '5min', '1hour', 'daily', '1day'")

            # Convert to mplfinance-ready DataFrame
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data)

            # Rename columns to uppercase for mplfinance compatibility
            column_mapping = {
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }
            df = df.rename(columns=column_mapping)

            # Set date as DatetimeIndex
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)

            return df

        exec_globals = {
            '__builtins__': __builtins__,
            'client': client,
            'get_stock_data': get_stock_data,
            'get_asset_data': get_asset_data,
        }

        # Configure chart directory (fixed directory)
        chart_dir = 'src/tools/temp_data'
        os.makedirs(chart_dir, exist_ok=True)
        exec_globals['CHART_DIR'] = chart_dir
        exec_globals['FILE_UUID_NAME'] = file_uuid_name
        # Also set as OS environment variable for code that uses os.environ.get()
        os.environ['FILE_UUID_NAME'] = file_uuid_name

        # Inject execution tracker into execution environment
        exec_globals['_execution_tracker'] = execution_tracker

        # Add pandas to execution environment
        exec_globals['pd'] = pd

        # Hook pandas DataFrame file output methods to redirect to temp_data
        def _hook_dataframe_methods():
            """Hook DataFrame methods to ensure files are saved in temp_data."""
            original_to_csv = pd.DataFrame.to_csv
            original_to_excel = pd.DataFrame.to_excel if hasattr(pd.DataFrame, 'to_excel') else None
            original_to_json = pd.DataFrame.to_json
            original_to_pickle = pd.DataFrame.to_pickle
            original_to_parquet = pd.DataFrame.to_parquet if hasattr(pd.DataFrame, 'to_parquet') else None

            def redirect_to_csv(self, path_or_buf=None, *args, **kwargs):
                if isinstance(path_or_buf, str):
                    path_or_buf = _ensure_temp_data_path(path_or_buf, chart_dir)
                    _track_file_creation(path_or_buf, execution_tracker, recently_tracked, "CSV file")
                return original_to_csv(self, path_or_buf, *args, **kwargs)

            if original_to_excel:
                def redirect_to_excel(self, excel_writer, *args, **kwargs):
                    if isinstance(excel_writer, str):
                        excel_writer = _ensure_temp_data_path(excel_writer, chart_dir)
                        _track_file_creation(excel_writer, execution_tracker, recently_tracked, "Excel file")
                    return original_to_excel(self, excel_writer, *args, **kwargs)

            def redirect_to_json(self, path_or_buf=None, *args, **kwargs):
                if isinstance(path_or_buf, str):
                    path_or_buf = _ensure_temp_data_path(path_or_buf, chart_dir)
                    _track_file_creation(path_or_buf, execution_tracker, recently_tracked, "JSON file")
                return original_to_json(self, path_or_buf, *args, **kwargs)

            def redirect_to_pickle(self, path, *args, **kwargs):
                if isinstance(path, str):
                    path = _ensure_temp_data_path(path, chart_dir)
                    _track_file_creation(path, execution_tracker, recently_tracked, "Pickle file")
                return original_to_pickle(self, path, *args, **kwargs)

            if original_to_parquet:
                def redirect_to_parquet(self, path, *args, **kwargs):
                    if isinstance(path, str):
                        path = _ensure_temp_data_path(path, chart_dir)
                        _track_file_creation(path, execution_tracker, recently_tracked, "Parquet file")
                    return original_to_parquet(self, path, *args, **kwargs)

            # Apply the hooks
            pd.DataFrame.to_csv = redirect_to_csv
            if original_to_excel:
                pd.DataFrame.to_excel = redirect_to_excel
            pd.DataFrame.to_json = redirect_to_json
            pd.DataFrame.to_pickle = redirect_to_pickle
            if original_to_parquet:
                pd.DataFrame.to_parquet = redirect_to_parquet

        # Call the hook function
        _hook_dataframe_methods()

        # Configure Chinese font support (using Source Han Sans from project)
        import platform
        font_path = 'src/utils/fonts/SourceHanSansCN-Regular.otf'

        # Build font list based on platform
        if platform.system() == 'Darwin':  # macOS
            font_list = ['Heiti SC', 'STHeiti', 'Arial Unicode MS']
        elif platform.system() == 'Windows':
            font_list = ['Microsoft YaHei', 'SimHei', 'Arial']
        else:  # Linux
            font_list = ['WenQuanYi Micro Hei', 'DejaVu Sans']

        # Prepend bundled font if available
        if os.path.exists(font_path):
            fm.fontManager.addfont(font_path)
            font_prop = fm.FontProperties(fname=font_path)
            font_name = font_prop.get_name()
            font_list = [font_name] + font_list

        mpl.rcParams['font.sans-serif'] = font_list
        mpl.rcParams['axes.unicode_minus'] = False

        # Override rcParams to intercept font changes that don't support CJK
        original_rcParams = mpl.rcParams.__class__

        # Common non-CJK fonts that agents mistakenly use
        non_cjk_fonts = {'DejaVu Sans', 'sans-serif', 'Arial', 'Helvetica', 'Times New Roman'}
        cjk_fonts = {'Heiti SC', 'STHeiti', 'Microsoft YaHei', 'SimHei',
                     'WenQuanYi Micro Hei', 'Source Han Sans CN'}

        class FontInterceptor(original_rcParams):
            def __setitem__(self, key, value):
                # Intercept font.family settings that don't support CJK
                if key == 'font.family':
                    if isinstance(value, str) and value in non_cjk_fonts:
                        # Redirect to font.sans-serif with CJK-capable fonts
                        super().__setitem__('font.sans-serif', font_list)
                        return
                    elif isinstance(value, list):
                        # Check if any CJK font is in the list
                        if not any(f in cjk_fonts for f in value):
                            # No CJK font found, prepend our font list
                            value = font_list + value

                # Intercept font.sans-serif SimHei references
                if key == 'font.sans-serif' and isinstance(value, list):
                    # Replace SimHei with platform-appropriate fonts
                    new_value = []
                    for font in value:
                        if font == 'SimHei':
                            # Use our configured font list instead
                            new_value.extend(font_list)
                            break
                        else:
                            new_value.append(font)
                    value = new_value
                super().__setitem__(key, value)

        # Apply the interceptor
        mpl.rcParams.__class__ = FontInterceptor

        # Add matplotlib to exec_globals for hooking
        exec_globals['plt'] = plt
        exec_globals['mpf'] = mpf

        # Track recently saved files to avoid duplicates
        recently_tracked = set()

        # Helper function to ensure files are saved in temp_data
        def _ensure_temp_data_path(fname, chart_dir):
            """
            Ensure the file path is within the temp_data directory.

            Args:
                fname: The filename or path provided by user code
                chart_dir: The designated chart directory (src/tools/temp_data)

            Returns:
                str: The corrected path within chart_dir
            """
            if not isinstance(fname, str):
                return fname

            # If just a filename (no path separators), prepend CHART_DIR
            if not os.path.isabs(fname) and os.sep not in fname:
                return os.path.join(chart_dir, fname)

            # If path specified but not in temp_data, redirect it
            if not fname.startswith(chart_dir):
                basename = os.path.basename(fname)
                print(f"Warning: Redirecting file save from {fname} to {chart_dir}")
                return os.path.join(chart_dir, basename)

            # Already in correct directory
            return fname

        # Helper function to track file creation
        def _track_file_creation(fname, execution_tracker, recently_tracked, file_type="Chart"):
            """
            Track file creation in execution tracker.

            Args:
                fname: The file path
                execution_tracker: The execution tracking dictionary
                recently_tracked: Set of recently tracked files to avoid duplicates
                file_type: Type of file for logging (default: "Chart")
            """
            if fname not in recently_tracked:
                execution_tracker["files_created"].append(fname)
                # Only increment charts_generated for image files
                if file_type in ["Chart", "Candlestick Chart"] or fname.lower().endswith(('.png', '.jpg', '.jpeg', '.svg')):
                    execution_tracker["charts_generated"] += 1
                execution_tracker["execution_milestones"].append(f"{file_type} generated: {os.path.basename(fname)}")
                recently_tracked.add(fname)

        # Hook matplotlib savefig to track and redirect chart generation
        original_savefig = plt.savefig
        def tracked_savefig(fname, *args, **kwargs):
            # Ensure file is saved in CHART_DIR
            fname = _ensure_temp_data_path(fname, chart_dir)

            # Call original with corrected fname
            result = original_savefig(fname, *args, **kwargs)

            # Track the file creation
            if isinstance(fname, str):
                _track_file_creation(fname, execution_tracker, recently_tracked, "Chart")

            return result
        plt.savefig = tracked_savefig

        # Also hook Figure.savefig to catch saves on figure instances
        import matplotlib.figure
        original_fig_savefig = matplotlib.figure.Figure.savefig
        def tracked_fig_savefig(self, fname, *args, **kwargs):
            # Ensure file is saved in CHART_DIR
            fname = _ensure_temp_data_path(fname, chart_dir)

            # Call original with corrected fname
            result = original_fig_savefig(self, fname, *args, **kwargs)

            # Track the file creation
            if isinstance(fname, str):
                _track_file_creation(fname, execution_tracker, recently_tracked, "Chart")

            return result
        matplotlib.figure.Figure.savefig = tracked_fig_savefig

        # Hook mplfinance if available
        if mpf:
            original_mpf_plot = mpf.plot
            def tracked_mpf_plot(*args, **kwargs):
                # Redirect savefig parameter if present
                if 'savefig' in kwargs:
                    fname = kwargs['savefig']
                    fname = _ensure_temp_data_path(fname, chart_dir)
                    kwargs['savefig'] = fname  # Update the parameter

                    # Track the file creation
                    if isinstance(fname, str):
                        _track_file_creation(fname, execution_tracker, recently_tracked, "Candlestick Chart")

                result = original_mpf_plot(*args, **kwargs)
                return result
            mpf.plot = tracked_mpf_plot

        # Hook the built-in open function to redirect file writes to temp_data
        original_open = open
        def tracked_open(file, mode='r', *args, **kwargs):
            # Only redirect write modes
            if isinstance(file, str) and ('w' in mode or 'a' in mode):
                file = _ensure_temp_data_path(file, chart_dir)
                # Track file creation for write mode
                if 'w' in mode:
                    file_ext = os.path.splitext(file)[1].lower()
                    file_type = {
                        '.csv': 'CSV file',
                        '.txt': 'Text file',
                        '.json': 'JSON file',
                        '.xml': 'XML file',
                        '.html': 'HTML file'
                    }.get(file_ext, 'File')
                    _track_file_creation(file, execution_tracker, recently_tracked, file_type)
            return original_open(file, mode, *args, **kwargs)

        # Add hooked open to exec_globals
        exec_globals['open'] = tracked_open

        # Execute the code
        exec(code, exec_globals) # Execute in the context of exec_globals

        stdout_val = output_buffer.getvalue()
        stderr_val = error_buffer.getvalue()

        # Analyze execution status
        if stderr_val:
            # Use unified formatting function
            result = _format_execution_result(stdout_val, stderr_val, None, execution_tracker)
            result_queue.put(result)
        else:
            # Use unified formatting function (success case)
            result = _format_execution_result(stdout_val, None, None, execution_tracker)
            result_queue.put(result)


    except Exception as e:
        # Get already executed stdout content
        stdout_val = output_buffer.getvalue()

        # Use error analysis module to handle error
        from src.tools.utils.error_analysis import build_error_info
        error_info = build_error_info(e, code)

        # Check actually generated files (simplified version, avoid duplicates)
        if chart_dir and file_uuid_name:
            pattern = os.path.join(chart_dir, f"{file_uuid_name}*")
            actual_files = glob.glob(pattern)
            for f in actual_files:
                if f not in execution_tracker["files_created"]:
                    execution_tracker["files_created"].append(f)
                    if f.endswith(('.png', '.jpg', '.svg')):
                        execution_tracker["charts_generated"] += 1

        # Detect achievements in stdout (if not recorded in tracker)
        if execution_tracker["dataframes_created"] == 0 and ("Data shape:" in stdout_val or "df.shape" in stdout_val):
            execution_tracker["dataframes_created"] = 1
        if execution_tracker["charts_generated"] == 0 and ("Chart saved" in stdout_val or "chart" in stdout_val.lower()):
            execution_tracker["charts_generated"] = 1

        # Use unified formatting function
        result = _format_execution_result(stdout_val, None, error_info, execution_tracker)
        result_queue.put(result)

    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr

@tool
def execute_python_code(code: str) -> Dict[str, Any]:
    """
        Execute Python code string in an isolated and time-limited environment with FMP financial market data access.

        **Pre-configured Environment Variables:**
        • **client**: FMP API client for accessing financial data
        • **get_stock_data**: Unified stock OHLCV data fetching function
        • **CHART_DIR**: Chart save directory (fixed at src/tools/temp_data/)
        • **FILE_UUID_NAME**: 8-character unique identifier for chart filename prefix

        **Available Libraries (Pre-imported into execution environment):**
        pandas (pd), numpy (np), matplotlib (plt), mplfinance (mpf)

        **Data Access Methods:**

        1. Unified OHLCV function:
           get_stock_data(symbol, interval='1day', start_date=None, end_date=None)

           Supported intervals: '1min', '5min', '15min', '30min', '1hour', '4hour', '1day'

           Examples:
           # Get daily data
           daily = get_stock_data('AAPL', interval='1day', start_date='2024-01-01', end_date='2024-12-31')

           # Get 5-minute candlestick data
           intraday = get_stock_data('AAPL', interval='5min', start_date='2024-01-01', end_date='2024-01-31')

        2. Direct use of client for advanced queries:
           # Real-time quotes
           quote = client.get_quote('AAPL')

           # Financial statements
           income = client.get_income_statement('AAPL', period='annual', limit=5)
           balance = client.get_balance_sheet('AAPL', period='annual', limit=5)
           cashflow = client.get_cash_flow('AAPL', period='annual', limit=5)

           # Key metrics and financial ratios
           metrics = client.get_key_metrics('AAPL')
           ratios = client.get_financial_ratios('AAPL')

           # Valuation analysis
           dcf = client.get_dcf('AAPL')

           # Company information
           profile = client.get_profile('AAPL')

        **Chart Generation Example:**
        ```python
        import pandas as pd
        import mplfinance as mpf
# pyright: ignore
import os

        # Get 5-minute candlestick data
        data = get_stock_data('AAPL', interval='5min', start_date='2024-01-01', end_date='2024-01-31')
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)

        # Create chart
        mc = mpf.make_marketcolors(up='green', down='red')
        s = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)
        fig, axes = mpf.plot(df, type='candle', style=s, volume=True, figscale=1.2, returnfig=True)

        # Save chart with standardized naming
        filename = f"{FILE_UUID_NAME}_intraday.png"
        filepath = os.path.join(CHART_DIR, filename)
        fig.savefig(filepath)
        print(f'Chart saved: {filename}')
        ```

    Args:
        code (str): Python code string to execute. Must include necessary import statements.

    Returns:
        Dict: Contains 'status' ('success', 'error', or 'timeout')
              and 'output' (execution stdout, error message, or timeout message).
    """
    # Set default parameters (internal use, not exposed to AI Agent)
    timeout = 100

    # Generate 8-character UUID as file prefix
    import uuid
    file_uuid_name = str(uuid.uuid4()).replace('-', '')[:8]

    logger.debug(f"Preparing to execute Python code in isolated process (timeout: {timeout}s, file_uuid: {file_uuid_name})...")

    # Use context to create process and queue, avoiding global settings
    # macOS must use spawn to avoid NSResponder and other Objective-C runtime issues
    # Linux uses default fork for better performance
    if sys.platform == 'darwin':
        ctx = multiprocessing.get_context('spawn')
    else:
        ctx = multiprocessing.get_context('fork')  # Linux defaults to fork

    # Use Queue instead of Manager.dict
    result_queue = ctx.Queue()

    # Pass additional parameters to subprocess
    process = ctx.Process(
        target=_run_code_in_process,
        args=(code, result_queue, file_uuid_name)
    )

    try:
        process.start()

        # Improvement: Check process status more frequently, especially for faster error returns
        check_interval = 0.1  # Check every 100ms
        elapsed_time = 0

        while process.is_alive() and elapsed_time < timeout:
            process.join(check_interval)
            elapsed_time += check_interval

            # If process has completed and has error status, return immediately
            if not process.is_alive():
                break

        if process.is_alive():
            logger.warning(f"Code execution timeout ({timeout}s), terminating process...")
            process.terminate()
            process.join(1) # Wait a bit for termination
            if process.is_alive(): # Force kill if still alive
                logger.warning("Process termination timeout, force killing.")
                process.kill()
                process.join(1)
            return {
                "status": "timeout",
                "output": f"Execution timed out after {timeout} seconds.",
            }

        # Get result from queue
        try:
            final_result = result_queue.get_nowait()  # Or use get(timeout=0.5) with small timeout
        except:
            # If queue is empty, use default error result
            final_result = {"status": "error", "output": "Process completed but no result was returned."}
        
        # Regardless of success or failure, try to upload charts to OSS and cleanup local files
        try:
            from src.tools.utils.chart_uploader import upload_and_cleanup_charts_by_prefix
            chart_dir = "src/tools/temp_data"
            uploaded_urls = upload_and_cleanup_charts_by_prefix(chart_dir, file_uuid_name)
            if uploaded_urls:
                # Replace local filenames in chart_files with complete OSS URLs
                final_result["chart_files"] = uploaded_urls

                # Add markdown chart section to output with complete OSS URLs
                chart_info = "\n\n📊 **Generated Charts**:\n\n"
                for url in uploaded_urls:
                    # Extract filename from URL
                    filename = url.split('/')[-1]
                    # Use complete OSS URL in markdown output
                    chart_info += f"![{filename}]({url})\n\n"

                final_result["output"] = str(final_result.get("output", "")) + chart_info

                # Add OSS upload status to structured data
                final_result["oss_upload"] = {
                    "status": "success",
                    "uploaded_count": len(uploaded_urls),
                    "error": None
                }
                logger.debug(f"Successfully uploaded {len(uploaded_urls)} charts to OSS")
            else:
                # No charts to upload
                final_result["oss_upload"] = {
                    "status": "no_charts",
                    "uploaded_count": 0,
                    "error": None
                }
        except Exception as upload_error:
            # Add OSS upload failure status to structured data
            final_result["oss_upload"] = {
                "status": "failed",
                "uploaded_count": 0,
                "error": str(upload_error)[:200]
            }
            logger.warning(f"Chart upload failed: {upload_error}")
        
        # For backward compatibility, ensure return format includes status and output
        if "status" not in final_result:
            final_result["status"] = "error"
        if "output" not in final_result:
            final_result["output"] = "No output available"

        # Only return status and output, not including metadata to avoid duplication

        if final_result.get("status") == "success":
            logger.debug(f"Python code execution successful. Output length: {len(str(final_result.get('output')))}")
        elif final_result.get("status") == "partial_success":
            logger.debug(f"Python code partially executed successfully. Status: partial_success, Output length: {len(str(final_result.get('output')))}")
        else:
            logger.warning(f"Python code execution failed. Status: {final_result.get('status')}, Output length: {len(str(final_result.get('output')))}")

        return final_result

    except Exception as e:
        logger.error(f"Error occurred when starting or managing code execution process: {e}", exc_info=True)
        return {
            "status": "error",
            "output": f"Failed to manage execution process: {str(e)}",
        }
    finally:
        if process.is_alive(): # Ensure process is cleaned up if any exception occurred after start
            logger.warning("Cleanup: Execution process still alive, attempting to terminate.")
            process.terminate()
            process.join(1)
            if process.is_alive():
                process.kill()
                process.join(1)
        # No need to shutdown manager as we're using Queue
        
def _format_execution_result(stdout_val, stderr_val, exception, execution_tracker):
    """Format execution result with structured data.

    Args:
        stdout_val: Standard output from execution
        stderr_val: Standard error output (if any)
        exception: Exception information (if any)
        execution_tracker: Dictionary tracking execution metrics

    Returns:
        dict: Result dictionary with output, status, metrics, and files
    """
    # Determine if we have meaningful output
    has_meaningful = _has_meaningful_output(stdout_val, execution_tracker)

    # Determine status
    if stderr_val or exception:
        if has_meaningful:
            status = "partial_success"
        else:
            status = "error"
    else:
        status = "success"

    # Build clean output text
    output_parts = []

    # 1. Execution output (different label based on error presence)
    if stdout_val and stdout_val.strip():
        if stderr_val or exception:
            output_parts.append("=== OUTPUT_BEFORE_ERROR ===")
        output_parts.append(stdout_val)

    # 2. Error information (if any)
    if stderr_val:
        output_parts.append("\n=== ERROR_INFO ===")
        output_parts.append(stderr_val)
    elif exception:
        output_parts.append("\n=== ERROR_INFO ===")
        output_parts.append(str(exception))

    output_text = "\n".join(output_parts)

    # Extract chart file names
    chart_files = []
    if execution_tracker.get("files_created"):
        for f in execution_tracker["files_created"]:
            basename = f.split('/')[-1] if '/' in f else f
            chart_files.append(basename)

    # Build structured result
    result = {
        "status": status,
        "output": output_text,
        "execution_metrics": {
            "charts_generated": execution_tracker['charts_generated'],
            "queries_executed": execution_tracker['queries_executed'],
            "dataframes_created": execution_tracker['dataframes_created']
        },
        "chart_files": chart_files
    }

    return result


def _has_meaningful_output(stdout_val, tracker):
    """Single source of truth for meaningful output detection.

    Args:
        stdout_val: Standard output text
        tracker: Execution tracker dictionary

    Returns:
        bool: True if output is meaningful, False otherwise
    """
    if not stdout_val:
        return False

    # Quick wins - if we generated artifacts
    if tracker.get("charts_generated", 0) > 0:
        return True
    if tracker.get("dataframes_created", 0) >= 2:
        return True
    if tracker.get("queries_executed", 0) >= 3 and len(stdout_val) > 200:
        return True

    # Check for financial/analysis patterns
    stdout_lower = stdout_val.lower()

    # Data structure indicators
    if any(pattern in stdout_lower for pattern in [
        "data shape:", "df.shape",
        "rows", "columns"
    ]):
        return True

    # Analysis results
    if "Calculation result" in stdout_val or "Analysis result" in stdout_val:
        return True

    # Financial analysis patterns
    financial_terms = [
        "P/E Ratio", "PE:", "P/E", "pe:",
        "P/B Ratio", "PB:", "P/B", "pb:",
        "ROE:", "roe:", "Return on Equity",
        "Gross Margin", "Net Margin", "Profit Margin",
        "Market Cap", "market cap", "Total Market Cap", "Float Market Cap"
    ]
    if any(term in stdout_val or term in stdout_lower for term in financial_terms):
        return True

    # Structured output patterns
    import re
    if re.search(r'\d+\.?\d*%', stdout_val):  # Percentage values
        return True
    if re.search(r'\|.*\|.*\|', stdout_val):  # Table format
        return True
    if "###" in stdout_val and any(term in stdout_val for term in ["Analysis", "analysis", "Financial", "Valuation", "Core"]):
        return True

    # Patterns for data (both English and Chinese rows/columns indicators)
    if re.search(r'\d+\s*(rows?|columns?|行|列)', stdout_val):  # rows/columns
        return True
    if re.search(r'(Result|结果)[:：]\s*\d+', stdout_val):  # Result: number pattern
        return True
    if re.search(r'\d+\.?\d*\s*(万元|USD|CNY)', stdout_val):  # Currency
        return True

    # Check for substantial output
    return len(stdout_val.strip()) > 500