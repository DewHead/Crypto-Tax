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

        sum_real_gain = 0.0
        sum_inflationary = 0.0
        sum_losses = 0.0

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
            real_gain = (c.real_gain_ils or 0.0)
            inflationary = (c.inflationary_gain_ils or 0.0)
            proceeds = real_gain + (c.adjusted_cost_basis_ils or 0.0)

            sum_real_gain += real_gain
            sum_inflationary += inflationary
            if real_gain < 0:
                sum_losses += abs(real_gain)

            writer.writerow([
                asset,
                buy_date_local.strftime("%d/%m/%Y"),
                sell_date_local.strftime("%d/%m/%Y"),
                f"{c.amount_consumed:.8f}",
                f"{c.ils_value_consumed:.2f}",
                f"{c.adjusted_cost_basis_ils:.2f}",
                f"{proceeds:.2f}",
                f"{real_gain:.2f}",
                f"{inflationary:.2f}"
            ])
            
        # Add Ordinary Income Summary (Field 258/204)
        from app.services.tax_engine import get_jerusalem_date, TransactionType
        ordinary_stmt = select(Transaction).filter(
            Transaction.is_active == True,
            Transaction.ordinary_income_ils > 0
        )
        res_ord = await db.execute(ordinary_stmt)
        ordinary_txs = res_ord.scalars().all()
        sum_ordinary = 0.0
        for t in ordinary_txs:
            if year and get_jerusalem_date(t.timestamp).year != year:
                continue
            sum_ordinary += (t.ordinary_income_ils or 0.0)

        # Append Summary Block
        writer.writerow([])
        writer.writerow(["--- FORM 1301 / 1391 SUMMARY ---"])
        writer.writerow(["Field Number", "Description", "Value (ILS)"])
        writer.writerow(["91", "Net Real Capital Gain", f"{max(0, sum_real_gain):.2f}"])
        writer.writerow(["256", "Inflationary Gain", f"{sum_inflationary:.2f}"])
        writer.writerow(["166", "Total Real Capital Losses", f"{sum_losses:.2f}"])
        writer.writerow(["258/204", "Ordinary Income (Staking/Earn)", f"{sum_ordinary:.2f}"])

        return output.getvalue()

export_service = ExportService()
