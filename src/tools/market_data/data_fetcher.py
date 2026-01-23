# pyright: ignore
"""
FMP Intraday Data Fetcher (Async)

Fetches intraday OHLCV data from Financial Modeling Prep API
with support for multiple intervals and date ranges.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, cast
import logging

from src.data_client.fmp import FMPClient

logger = logging.getLogger(__name__)


class IntradayDataFetcher:
    """Fetches intraday stock data from FMP API (Async)"""

    VALID_INTERVALS = ["1min", "5min", "15min", "30min", "1hour", "4hour"]

    def __init__(self, fmp_client: Optional[FMPClient] = None):
        """
        Initialize data fetcher

        Args:
            fmp_client: FMP client instance (creates new if not provided)
        """
        self.client = fmp_client or FMPClient()
        self._owns_client = fmp_client is None

    async def fetch_intraday_data(
        self,
        ticker: str,
        interval: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Fetch intraday OHLCV data from FMP

        Args:
            ticker: Stock ticker symbol (e.g., "AAPL")
            interval: Time interval - "1min", "5min", "15min", "30min", "1hour", "4hour"
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with columns: date, open, high, low, close, volume
            Index is datetime

        Raises:
            ValueError: If interval is invalid or date range is invalid
        """
        # Validate interval
        if interval not in self.VALID_INTERVALS:
            raise ValueError(
                f"Invalid interval '{interval}'. "
                f"Must be one of: {', '.join(self.VALID_INTERVALS)}"
            )

        # Parse and validate dates
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

        if start_dt > end_dt:
            raise ValueError("start_date must be before end_date")

        # Check if date range is too large (FMP limits)
        days_diff = (end_dt - start_dt).days
        max_days = self._get_max_days_for_interval(interval)

        if days_diff > max_days:
            logger.warning(
                f"Date range ({days_diff} days) exceeds recommended limit "
                f"({max_days} days) for {interval} interval. Fetching in chunks..."
            )
            return await self._fetch_in_chunks(ticker, interval, start_dt, end_dt, max_days)

        # Fetch data from FMP
        try:
            data = await self._fetch_from_fmp(ticker, interval, start_date, end_date)

            if not data:
                logger.warning(f"No data returned for {ticker} {interval} {start_date} to {end_date}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Process and clean data
            df = self._process_dataframe(df)

            logger.info(
                f"Fetched {len(df)} {interval} data points for {ticker} "
                f"from {start_date} to {end_date}"
            )

            return df

        except Exception as e:
            logger.error(f"Error fetching intraday data for {ticker}: {e}")
            raise

    async def _fetch_from_fmp(
        self,
        ticker: str,
        interval: str,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Fetch data from FMP API"""
        # Use the intraday chart method
        data = await self.client.get_intraday_chart(
            ticker,
            interval,
            from_date=start_date,
            to_date=end_date
        )
        return data if data else []

    async def _fetch_in_chunks(
        self,
        ticker: str,
        interval: str,
        start_dt: datetime,
        end_dt: datetime,
        chunk_days: int
    ) -> pd.DataFrame:
        """Fetch data in chunks when date range is too large"""
        all_data = []
        current_start = start_dt

        while current_start < end_dt:
            current_end = min(current_start + timedelta(days=chunk_days), end_dt)

            try:
                chunk_data = await self._fetch_from_fmp(
                    ticker,
                    interval,
                    current_start.strftime("%Y-%m-%d"),
                    current_end.strftime("%Y-%m-%d")
                )

                if chunk_data:
                    all_data.extend(chunk_data)

                logger.info(
                    f"Fetched chunk: {current_start.date()} to {current_end.date()} "
                    f"({len(chunk_data)} records)"
                )

            except Exception as e:
                logger.error(f"Error fetching chunk {current_start.date()}: {e}")

            current_start = current_end + timedelta(days=1)

        if not all_data:
            return pd.DataFrame()

        # Convert to DataFrame and process
        df = pd.DataFrame(all_data)
        df = self._process_dataframe(df)

        # Remove duplicates that might occur at chunk boundaries
        df = cast(pd.DataFrame, df.loc[~df.index.duplicated(keep='first'), :])

        return df

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean the DataFrame"""
        if df.empty:
            return df

        # Ensure required columns exist
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Convert date to datetime and set as index
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)

        # Ensure numeric types
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Remove rows with NaN values in OHLC
        df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)

        # Capitalize column names for consistency with mplfinance
        df.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }, inplace=True)

        return df

    def _get_max_days_for_interval(self, interval: str) -> int:
        """
        Get recommended maximum days for each interval

        FMP has rate limits and data size limits. These are conservative estimates.
        """
        max_days_map = {
            "1min": 5,      # ~390 bars per day * 5 = ~1950 bars
            "5min": 15,     # ~78 bars per day * 15 = ~1170 bars
            "15min": 30,    # ~26 bars per day * 30 = ~780 bars
            "30min": 60,    # ~13 bars per day * 60 = ~780 bars
            "1hour": 90,    # ~6.5 bars per day * 90 = ~585 bars
            "4hour": 180,   # ~1.6 bars per day * 180 = ~288 bars
        }

        return max_days_map.get(interval, 30)

    async def close(self):
        """Close the client if we own it"""
        if self._owns_client:
            await self.client.close()


# Unified data fetching function (async)
async def get_stock_data(
    symbol: str,
    interval: str = '1day',
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fmp_client: Optional[FMPClient] = None
) -> pd.DataFrame:
    """
    Unified function to get stock OHLCV data for any time interval.

    Supports both daily and intraday data with a consistent interface.

    Args:
        symbol: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        interval: Time interval - supports:
            - Daily: '1day', 'daily'
            - Intraday: '1min', '5min', '15min', '30min', '1hour', '4hour'
            Default: '1day'
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        fmp_client: Optional FMP client instance

    Returns:
        DataFrame with columns: Open, High, Low, Close, Volume
        Index is DatetimeIndex

    Examples:
        # Get daily data
        daily_df = await get_stock_data('AAPL', interval='1day',
                                   start_date='2024-01-01', end_date='2024-12-31')

        # Get hourly data
        hourly_df = await get_stock_data('AAPL', interval='1hour',
                                    start_date='2024-12-01', end_date='2024-12-31')

        # Get 5-minute data
        intraday_df = await get_stock_data('AAPL', interval='5min',
                                      start_date='2024-12-01', end_date='2024-12-15')
    """
    # Use provided client or create new one
    owns_client = fmp_client is None
    client = fmp_client or FMPClient()

    try:
        # Normalize interval
        interval_lower = interval.lower()

        # Handle daily data
        if interval_lower in ['1day', 'daily', '1d', 'day']:
            # Fetch daily data
            data = await client.get_stock_price(
                symbol,
                from_date=start_date,
                to_date=end_date
            )

            if not data:
                logger.warning(f"No daily data returned for {symbol}")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Process DataFrame similar to intraday processing
            if df.empty:
                return df

            # Ensure required columns exist
            required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                logger.error(f"Missing required columns for daily data: {missing_cols}")
                return pd.DataFrame()

            # Convert date to datetime and set as index
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)

            # Ensure numeric types
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Remove rows with NaN values in OHLC
            df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)

            # Capitalize column names for consistency
            df.rename(columns={
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }, inplace=True)

            logger.info(f"Fetched {len(df)} daily data points for {symbol}")
            return df

        # Handle intraday data
        else:
            # FMP intraday endpoints require concrete date strings
            if start_date is None or end_date is None:
                end_dt = datetime.utcnow().date()
                start_dt = end_dt - timedelta(days=7)
                start_date = start_date or start_dt.strftime("%Y-%m-%d")
                end_date = end_date or end_dt.strftime("%Y-%m-%d")

            assert start_date is not None and end_date is not None

            fetcher = IntradayDataFetcher(client)
            return await fetcher.fetch_intraday_data(symbol, interval, start_date, end_date)

    finally:
        # Close client if we created it
        if owns_client:
            await client.close()


# Convenience function (async version)
async def fetch_intraday_data(
    ticker: str,
    interval: str,
    start_date: str,
    end_date: str,
    fmp_client: Optional[FMPClient] = None
) -> pd.DataFrame:
    """
    Convenience function to fetch intraday data

    Args:
        ticker: Stock ticker symbol
        interval: Time interval (1min, 5min, 15min, 30min, 1hour, 4hour)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        fmp_client: Optional FMP client instance

    Returns:
        DataFrame with OHLCV data indexed by datetime

    Example:
        df = await fetch_intraday_data("AAPL", "15min", "2024-01-01", "2024-01-31")
    """
    owns_client = fmp_client is None
    client = fmp_client or FMPClient()

    try:
        fetcher = IntradayDataFetcher(client)
        return await fetcher.fetch_intraday_data(ticker, interval, start_date, end_date)
    finally:
        if owns_client:
            await client.close()
