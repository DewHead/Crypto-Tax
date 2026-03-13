
import asyncio
from app.db.session import AsyncSessionLocal
from app.models.transaction import Transaction, TransactionType
from sqlalchemy import select
from app.services.boi import boi_service
from app.services.price import price_service

async def get_fee_ils(tx: Transaction) -> float:
    if not tx.fee_amount: return 0.0
    rate = tx.ils_exchange_rate or 3.65
    if tx.fee_asset in ['USD', 'USDT', 'USDC', 'BUSD', 'DAI']:
        return tx.fee_amount * rate
    if tx.fee_asset == 'ILS':
        return tx.fee_amount
    usd_price = await price_service.get_historical_price(tx.fee_asset, tx.timestamp.date())
    if usd_price:
        return tx.fee_amount * usd_price * rate
    return 0.0

async def check_yearly_net_gain():
    async with AsyncSessionLocal() as db:
        stmt = select(Transaction).order_by(Transaction.timestamp.asc())
        result = await db.execute(stmt)
        all_txs = result.scalars().all()
        
        txs_by_year = {}
        for tx in all_txs:
            y = tx.timestamp.year
            if y not in txs_by_year:
                txs_by_year[y] = []
            txs_by_year[y].append(tx)
        
        sorted_years = sorted(txs_by_year.keys())
        accumulated_loss = 0.0
        
        for y in sorted_years:
            y_fees = 0.0
            for tx in txs_by_year[y]:
                if tx.type not in [TransactionType.buy, TransactionType.sell, TransactionType.dust, TransactionType.convert]:
                    y_fees += await get_fee_ils(tx)
            
            y_taxable_txs = [t for t in txs_by_year[y] if t.is_taxable_event]
            y_gain = sum(t.capital_gain_ils or 0.0 for t in y_taxable_txs)
            y_net_gain = y_gain - y_fees
            
            print(f"Year {y}: Gain={y_gain:.2f}, Fees={y_fees:.2f}, Net={y_net_gain:.2f}, Prev Acc Loss={accumulated_loss:.2f}")
            
            accumulated_loss += y_net_gain
            if accumulated_loss > 0:
                accumulated_loss = 0.0
            
            print(f"Year {y}: New Acc Loss={accumulated_loss:.2f}")
            print("-" * 20)

if __name__ == "__main__":
    asyncio.run(check_yearly_net_gain())
