...
                    if usd_ils_rate is None:
                        usd_ils_rate = await boi_service.get_usd_ils_rate(curr, db=db)
                    
                    daily_val_ils = 0.0
                    for asset, qty in assets_to_price.items():
                        if asset not in ['USD', 'ILS']:
                            usd_price = await price_service.get_historical_price(asset, curr, db=db)
                            if usd_price:
                                daily_val_ils += qty * usd_price * usd_ils_rate
...
