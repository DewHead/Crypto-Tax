import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from app.services.ingestion import IngestionService
from app.models.exchange_key import ExchangeKey
from app.models.transaction import Transaction
from datetime import datetime

@pytest.mark.asyncio
async def test_sync_all_initializes_krakenfutures():
    """
    Verifies that sync_all correctly identifies and initializes krakenfutures.
    """
    service = IngestionService()
    
    # Mock DB session and keys
    mock_key = ExchangeKey(
        id=1,
        exchange_name='krakenfutures',
        api_key='test_key',
        api_secret='test_secret'
    )
    
    with patch('app.services.ingestion.AsyncSessionLocal') as mock_session_factory:
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        
        # Mocking select(ExchangeKey) results
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_key]
        mock_result.scalars.return_value.first.return_value = mock_key
        mock_session.execute.return_value = mock_result
        
        # Mock ccxt.krakenfutures
        with patch('ccxt.async_support.krakenfutures') as mock_kraken_futures:
            mock_instance = AsyncMock()
            mock_kraken_futures.return_value = mock_instance
            
            # Mock sync_exchange to avoid actual network calls
            service.sync_exchange = AsyncMock()
            service.sync_env_keys = AsyncMock()
            
            await service.sync_all()
            
            # Verify krakenfutures was instantiated with correct keys
            mock_kraken_futures.assert_called_once_with({
                'apiKey': 'test_key',
                'secret': 'test_secret',
                'enableRateLimit': True,
            })
            
            # Verify sync_exchange was called for krakenfutures
            service.sync_exchange.assert_called_once_with('krakenfutures', mock_instance, 1, db=mock_session)
            
            # Verify close was called
            mock_instance.close.assert_called_once()

@pytest.mark.asyncio
async def test_sync_exchange_kraken_global_fetch():
    """
    Verifies that sync_exchange uses global fetch for Kraken.
    """
    service = IngestionService()
    mock_exchange = AsyncMock()
    mock_exchange.has = {'fetchDeposits': False, 'fetchWithdrawals': False, 'fetchMyTrades': True}
    
    # Mock fetch_my_trades to return a dummy trade
    mock_trade = {
        'id': 'trade1',
        'symbol': 'BTC/USD',
        'timestamp': int(datetime(2025, 1, 1).timestamp() * 1000),
        'side': 'buy',
        'amount': 1.0,
        'cost': 50000.0,
        'fee': {'currency': 'USD', 'cost': 10.0}
    }
    mock_exchange.fetch_my_trades.return_value = [mock_trade]
    
    # Mock _process_trade_to_tx
    service._process_trade_to_tx = AsyncMock()
    
    await service.sync_exchange('kraken', mock_exchange, 1, db=None)
    
    # Verify load_markets was called
    mock_exchange.load_markets.assert_called_once()
    
    # Verify fetch_my_trades was called (via _fetch_all_my_trades)
    mock_exchange.fetch_my_trades.assert_called()
    
    # Verify _process_trade_to_tx was called
    service._process_trade_to_tx.assert_called()

@pytest.mark.asyncio
async def test_process_trade_creates_transaction():
    """
    Verifies that _process_trade_to_tx correctly maps CCXT trade to Transaction.
    """
    service = IngestionService()
    mock_exchange = MagicMock()
    mock_exchange.market.return_value = {'base': 'BTC', 'quote': 'USD'}
    
    mock_trade = {
        'id': 'trade1',
        'symbol': 'BTC/USD',
        'timestamp': int(datetime(2025, 1, 1).timestamp() * 1000),
        'side': 'buy',
        'amount': 1.0,
        'cost': 50000.0,
        'fee': {'currency': 'USD', 'cost': 10.0}
    }
    
    service._save_transaction = AsyncMock()
    
    tx = await service._process_trade_to_tx('kraken', mock_exchange, mock_trade)
    
    # Check that it returns a valid transaction
    assert isinstance(tx, Transaction)
    assert tx.exchange == 'kraken'
    assert tx.tx_hash == 'trade1'
    assert tx.asset_to == 'BTC'
    assert tx.amount_to == 1.0
    assert tx.asset_from == 'USD'
    assert tx.amount_from == 50000.0
    assert tx.fee_amount == 10.0
