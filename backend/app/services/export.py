import csv
import io
from typing import List, Optional
from datetime import timezone
from zoneinfo import ZoneInfo
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from app.models.transaction import Transaction
from app.models.tax_lot_consumption import TaxLotConsumption

class ExportService:
    async def generate_form_8659_csv(self, db: AsyncSession, year: Optional[int] = None) -> str:
        """
        Generates a CSV formatted for Israeli Tax Authority Form 8659 (Appendix D).
        Columns: Asset, Date of Purchase, Date of Sale, Amount, Original Cost Basis (ILS), 
                 Adjusted (Madad) Cost Basis (ILS), Proceeds (ILS), Real Gain (ILS), Inflationary Gain (ILS)
        """
        BuyTx = aliased(Transaction)
        SellTx = aliased(Transaction)

        # Join TaxLotConsumption with Sell and Buy transactions in one query
        stmt = select(
            TaxLotConsumption, 
            SellTx.timestamp.label('sell_date'),
            SellTx.asset_from.label('asset'),
            BuyTx.timestamp.label('buy_date')
        ).join(
            SellTx, TaxLotConsumption.sell_tx_id == SellTx.id
        ).join(
            BuyTx, TaxLotConsumption.buy_tx_id == BuyTx.id
        ).order_by(SellTx.timestamp.asc())
        
        result = await db.execute(stmt)
        consumptions = result.all()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Asset", "Date of Purchase", "Date of Sale", "Amount", 
            "Original Cost Basis (ILS)", "Adjusted (Madad) Cost Basis (ILS)", 
            "Proceeds (ILS)", "Real Gain (ILS)", "Inflationary Gain (ILS)"
        ])

        for row in consumptions:
            c: TaxLotConsumption = row[0]
            
            # Localize the timestamps to Israel Time
            sell_dt = row[1]
            if sell_dt.tzinfo is None:
                sell_dt = sell_dt.replace(tzinfo=timezone.utc)
            sell_date_local = sell_dt.astimezone(ZoneInfo("Asia/Jerusalem"))
            
            buy_dt = row[3]
            if buy_dt.tzinfo is None:
                buy_dt = buy_dt.replace(tzinfo=timezone.utc)
            buy_date_local = buy_dt.astimezone(ZoneInfo("Asia/Jerusalem"))

            asset = row[2]
            
            # Check the local year, not the UTC year
            if year and sell_date_local.year != year:
                continue
            
            # Proceeds = real_gain + adjusted_cost_basis
            proceeds = (c.real_gain_ils or 0.0) + (c.adjusted_cost_basis_ils or 0.0)

            writer.writerow([
                asset,
                buy_date_local.strftime("%d/%m/%Y"),
                sell_date_local.strftime("%d/%m/%Y"),
                f"{c.amount_consumed:.8f}",
                f"{c.ils_value_consumed:.2f}",
                f"{c.adjusted_cost_basis_ils:.2f}",
                f"{proceeds:.2f}",
                f"{c.real_gain_ils:.2f}",
                f"{c.inflationary_gain_ils:.2f}"
            ])

        return output.getvalue()

export_service = ExportService()
