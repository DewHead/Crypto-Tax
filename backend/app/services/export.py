import csv
import io
from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.transaction import Transaction
from app.models.tax_lot_consumption import TaxLotConsumption

class ExportService:
    async def generate_form_8659_csv(self, db: AsyncSession, year: Optional[int] = None) -> str:
        """
        Generates a CSV formatted for Israeli Tax Authority Form 8659 (Appendix D).
        Columns: Asset, Date of Purchase, Date of Sale, Amount, Original Cost Basis (ILS), 
                 Adjusted (Madad) Cost Basis (ILS), Proceeds (ILS), Real Gain (ILS), Inflationary Gain (ILS)
        """
        # Join TaxLotConsumption with Sell and Buy transactions
        stmt = select(
            TaxLotConsumption, 
            Transaction.timestamp.label('sell_date'),
            Transaction.asset_from.label('asset')
        ).join(
            Transaction, TaxLotConsumption.sell_tx_id == Transaction.id
        ).order_by(Transaction.timestamp.asc())
        
        result = await db.execute(stmt)
        consumptions = result.all() # List of (TaxLotConsumption, sell_date, asset)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Asset", "Date of Purchase", "Date of Sale", "Amount", 
            "Original Cost Basis (ILS)", "Adjusted (Madad) Cost Basis (ILS)", 
            "Proceeds (ILS)", "Real Gain (ILS)", "Inflationary Gain (ILS)"
        ])

        for consumption_row in consumptions:
            c: TaxLotConsumption = consumption_row[0]
            sell_date = consumption_row[1]
            asset = consumption_row[2]
            
            if year and sell_date.year != year:
                continue
            
            # Fetch buy transaction for purchase date
            buy_stmt = select(Transaction.timestamp).where(Transaction.id == c.buy_tx_id)
            buy_res = await db.execute(buy_stmt)
            buy_date = buy_res.scalar_one()

            # Proceeds for this lot consumption:
            # We need to fetch the sell transaction's total proceeds to apportion it
            # Or we can store proceeds in TaxLotConsumption. 
            # Since we didn't store it, we'll calculate it here.
            sell_stmt = select(Transaction).where(Transaction.id == c.sell_tx_id)
            sell_res = await db.execute(sell_stmt)
            sell_tx: Transaction = sell_res.scalar_one()
            
            # We use the same logic as in _process_transaction to get proceeds
            # Note: This is a bit redundant, ideally we'd store proceeds_ils in TaxLotConsumption
            # But we can reconstruct it.
            
            # For the export, real_gain_ils and inflationary_gain_ils are already in 'c'
            # We can find proceeds by: proceeds = real_gain + adjusted_cost_basis
            proceeds = (c.real_gain_ils or 0.0) + (c.adjusted_cost_basis_ils or 0.0)

            writer.writerow([
                asset,
                buy_date.strftime("%d/%m/%Y"),
                sell_date.strftime("%d/%m/%Y"),
                f"{c.amount_consumed:.8f}",
                f"{c.ils_value_consumed:.2f}",
                f"{c.adjusted_cost_basis_ils:.2f}",
                f"{proceeds:.2f}",
                f"{c.real_gain_ils:.2f}",
                f"{c.inflationary_gain_ils:.2f}"
            ])

        return output.getvalue()

export_service = ExportService()
