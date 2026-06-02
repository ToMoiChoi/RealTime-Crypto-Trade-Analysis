import numpy as np
import pandas as pd
import random

def run_evaluation():
    np.random.seed(42)
    random.seed(42)

    # We will generate 45,000 transactions in total.
    # Group trades into batches of 300 to simulate micro-batches where symbol count > 30.
    batch_size = 300
    n_batches = 150
    n_total = batch_size * n_batches
    
    records = []
    
    # Base prices
    base_prices = {1: 60000.0, 2: 3000.0, 3: 600.0, 4: 150.0, 5: 0.6}
    
    # Let's keep track of ground truth labels:
    # 0 = normal, 1 = Z-score outlier, 2 = Wash trade, 3 = Price slippage
    ground_truth = []
    
    # Generate base normal trades
    trade_id_counter = 1000000
    
    z_score_indices = set(random.sample(range(n_batches), 50))
    wash_trade_batches = set(random.sample([b for b in range(n_batches) if b not in z_score_indices], 10))
    slippage_batches = set(random.sample([b for b in range(n_batches) if b not in z_score_indices and b not in wash_trade_batches], 50))
    
    trade_time_sec = 1714500000 # Start epoch time
    
    for b in range(n_batches):
        batch_trades = []
        batch_gt = []
        
        current_batch_size = batch_size
        if b in wash_trade_batches:
            current_batch_size = batch_size - 5 # we will add a wash trade cluster of 5 trades to make it 300
            
        for i in range(current_batch_size):
            crypto_pair_key = random.randint(1, 5)
            bp = base_prices[crypto_pair_key]
            
            # Normal price: standard deviation 0.1%
            price = bp * (1.0 + np.random.normal(0, 0.001))
            # Normal quantity
            if crypto_pair_key == 1:
                qty = random.uniform(0.01, 0.5) # normal amount around $600 to $30k
            elif crypto_pair_key == 2:
                qty = random.uniform(0.1, 5.0)  # normal amount around $300 to $15k
            else:
                qty = random.uniform(1.0, 50.0) # normal amount around $150 to $30k
                
            amount_usd = price * qty
            
            trade_time_sec += random.randint(1, 2)
            
            batch_trades.append({
                "trade_id": trade_id_counter,
                "crypto_pair_key": crypto_pair_key,
                "price": price,
                "quantity": qty,
                "amount_usd": amount_usd,
                "trade_time": pd.to_datetime(trade_time_sec, unit='s'),
            })
            batch_gt.append(0) # Normal
            trade_id_counter += 1
            
        # Add anomalies
        if b in z_score_indices:
            # Add a Z-score outlier: amount_usd is mean + 5 * std
            crypto_pair_key = random.randint(1, 5)
            bp = base_prices[crypto_pair_key]
            price = bp * (1.0 + np.random.normal(0, 0.001))
            
            # Calculate standard deviation and mean of normal trades of this symbol in the batch
            peer_amounts = [t["amount_usd"] for t in batch_trades if t["crypto_pair_key"] == crypto_pair_key]
            if len(peer_amounts) > 2:
                mean_amt = np.mean(peer_amounts)
                std_amt = np.std(peer_amounts)
                if std_amt == 0:
                    std_amt = 1000.0
            else:
                mean_amt = 10000.0
                std_amt = 5000.0
                
            # Set target amount to mean + 5 * std to guarantee Z-score > 3.0
            # Make sure it's below 1,000,000 USD to not trigger Whale Alert, but high enough
            amount_usd = min(950000.0, mean_amt + 5.0 * std_amt)
            if amount_usd <= mean_amt + 3.0 * std_amt:
                # If constrained by 950k, ensure we still satisfy z-score if we adjust peer amounts or just override std
                amount_usd = mean_amt + 5.0 * std_amt
                # If it exceeds 1M, let's keep it above 1M, the rule also counts it as anomaly (Whale Alert)
                # But to specifically test Z-Score, let's keep it under 1M by scaling down other peers if needed,
                # or just let it be. If it is > 1M, is_anomaly is still True.
            
            qty = amount_usd / price
            
            # Replace a trade of this symbol with the anomaly
            indices_of_pair = [idx for idx, t in enumerate(batch_trades) if t["crypto_pair_key"] == crypto_pair_key]
            if indices_of_pair:
                idx_to_replace = random.choice(indices_of_pair)
                batch_trades[idx_to_replace] = {
                    "trade_id": trade_id_counter,
                    "crypto_pair_key": crypto_pair_key,
                    "price": price,
                    "quantity": qty,
                    "amount_usd": amount_usd,
                    "trade_time": pd.to_datetime(trade_time_sec, unit='s'),
                }
                batch_gt[idx_to_replace] = 1 # Z-Score Outlier
            else:
                # Fallback if no matching pair
                batch_trades.append({
                    "trade_id": trade_id_counter,
                    "crypto_pair_key": crypto_pair_key,
                    "price": price,
                    "quantity": qty,
                    "amount_usd": amount_usd,
                    "trade_time": pd.to_datetime(trade_time_sec, unit='s'),
                })
                batch_gt.append(1)
            trade_id_counter += 1
            
        elif b in wash_trade_batches:
            # Add a wash trade cluster: 5 trades at the exact same second for the same symbol
            crypto_pair_key = random.randint(1, 5)
            bp = base_prices[crypto_pair_key]
            same_sec = trade_time_sec + 2
            
            for _ in range(5):
                price = bp * (1.0 + np.random.normal(0, 0.0005))
                qty = random.uniform(1.0, 5.0)
                amount_usd = price * qty
                
                batch_trades.append({
                    "trade_id": trade_id_counter,
                    "crypto_pair_key": crypto_pair_key,
                    "price": price,
                    "quantity": qty,
                    "amount_usd": amount_usd,
                    "trade_time": pd.to_datetime(same_sec, unit='s'),
                })
                batch_gt.append(2) # Wash Trade
                trade_id_counter += 1
                
        elif b in slippage_batches:
            # Add a price slippage anomaly: price dev > 1% and amount > batch mean
            crypto_pair_key = random.randint(1, 5)
            bp = base_prices[crypto_pair_key]
            
            peer_prices = [t["price"] for t in batch_trades if t["crypto_pair_key"] == crypto_pair_key]
            peer_amounts = [t["amount_usd"] for t in batch_trades if t["crypto_pair_key"] == crypto_pair_key]
            
            avg_price = np.mean(peer_prices) if peer_prices else bp
            mean_amt = np.mean(peer_amounts) if peer_amounts else 10000.0
            
            # Deviate price by +2%
            price = avg_price * 1.02
            amount_usd = mean_amt * 1.5
            qty = amount_usd / price
            
            indices_of_pair = [idx for idx, t in enumerate(batch_trades) if t["crypto_pair_key"] == crypto_pair_key]
            if indices_of_pair:
                idx_to_replace = random.choice(indices_of_pair)
                batch_trades[idx_to_replace] = {
                    "trade_id": trade_id_counter,
                    "crypto_pair_key": crypto_pair_key,
                    "price": price,
                    "quantity": qty,
                    "amount_usd": amount_usd,
                    "trade_time": pd.to_datetime(trade_time_sec, unit='s'),
                }
                batch_gt[idx_to_replace] = 3 # Price Slippage
            else:
                batch_trades.append({
                    "trade_id": trade_id_counter,
                    "crypto_pair_key": crypto_pair_key,
                    "price": price,
                    "quantity": qty,
                    "amount_usd": amount_usd,
                    "trade_time": pd.to_datetime(trade_time_sec, unit='s'),
                })
                batch_gt.append(3)
            trade_id_counter += 1
            
        records.extend(batch_trades)
        ground_truth.extend(batch_gt)
        
    df = pd.DataFrame(records)
    df["gt"] = ground_truth
    df["batch_id"] = np.repeat(np.arange(n_batches), batch_size)
    
    # Run anomaly detection simulation
    detected_anomalies = []
    
    for b_id, group in df.groupby("batch_id"):
        group = group.copy()
        
        # Calculate statistics per symbol
        for pair_key, symbol_group in group.groupby("crypto_pair_key"):
            idx = symbol_group.index
            batch_count = len(symbol_group)
            
            # Z-Score statistics
            mean_usd = symbol_group["amount_usd"].mean()
            std_usd = symbol_group["amount_usd"].std()
            if pd.isna(std_usd) or std_usd == 0:
                std_usd = 1.0
                
            z_scores = (symbol_group["amount_usd"] - mean_usd) / std_usd if batch_count > 10 else pd.Series(0.0, index=idx)
            
            # Price Slippage statistics
            avg_price = symbol_group["price"].mean()
            price_dev_pct = (symbol_group["price"] - avg_price).abs() / avg_price
            
            # Wash Trade statistics
            wash_counts = symbol_group.groupby("trade_time")["trade_id"].transform("count")
            
            for i in idx:
                z_sc = z_scores.loc[i]
                dev_pct = price_dev_pct.loc[i]
                w_size = wash_counts.loc[i]
                amt = symbol_group.loc[i, "amount_usd"]
                
                is_z_anomaly = (batch_count > 30) and (z_sc > 3.0)
                is_wash_anomaly = (w_size >= 4)
                is_slip_anomaly = (batch_count > 30) and (dev_pct > 0.01) and (amt > mean_usd)
                is_whale = (amt >= 1000000)
                is_dust = (amt < 0.01)
                
                is_anomaly = is_z_anomaly or is_wash_anomaly or is_slip_anomaly or is_whale or is_dust
                
                detected_anomalies.append({
                    "index": i,
                    "is_anomaly": is_anomaly,
                    "is_z_anomaly": is_z_anomaly,
                    "is_wash_anomaly": is_wash_anomaly,
                    "is_slip_anomaly": is_slip_anomaly,
                    "z_score": z_sc,
                    "price_dev_pct": dev_pct,
                    "wash_cluster_size": w_size
                })
                
    det_df = pd.DataFrame(detected_anomalies).sort_values("index").set_index("index")
    df = df.join(det_df)
    
    df["gt_anomaly"] = df["gt"] > 0
    
    tp = ((df["gt_anomaly"] == True) & (df["is_anomaly"] == True)).sum()
    fp = ((df["gt_anomaly"] == False) & (df["is_anomaly"] == True)).sum()
    fn = ((df["gt_anomaly"] == True) & (df["is_anomaly"] == False)).sum()
    tn = ((df["gt_anomaly"] == False) & (df["is_anomaly"] == False)).sum()
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    
    # Detailed rules performance
    z_tp = ((df["gt"] == 1) & (df["is_z_anomaly"] == True)).sum()
    z_total = (df["gt"] == 1).sum()
    
    w_tp = ((df["gt"] == 2) & (df["is_wash_anomaly"] == True)).sum()
    w_total = (df["gt"] == 2).sum()
    
    s_tp = ((df["gt"] == 3) & (df["is_slip_anomaly"] == True)).sum()
    s_total = (df["gt"] == 3).sum()
    
    # Check for False Positives details
    # A FP can occur if a normal trade got flagged by one of the rules
    fp_z = ((df["gt"] == 0) & (df["is_z_anomaly"] == True)).sum()
    fp_w = ((df["gt"] == 0) & (df["is_wash_anomaly"] == True)).sum()
    fp_s = ((df["gt"] == 0) & (df["is_slip_anomaly"] == True)).sum()
    
    print(f"Total Transactions: {len(df):,}")
    print(f"Total Ground Truth Anomalies: {df['gt_anomaly'].sum():,}")
    print(f"  - Z-Score Outliers: {z_total}")
    print(f"  - Wash Trade: {w_total}")
    print(f"  - Price Slippage: {s_total}")
    print("-" * 50)
    print(f"System Detected Anomalies: {df['is_anomaly'].sum():,}")
    print(f"  - Z-Score rule detected: {df['is_z_anomaly'].sum()} (FP: {fp_z})")
    print(f"  - Wash Trade rule detected: {df['is_wash_anomaly'].sum()} (FP: {fp_w})")
    print(f"  - Price Slippage rule detected: {df['is_slip_anomaly'].sum()} (FP: {fp_s})")
    print("-" * 50)
    print(f"TP: {tp}, FP: {fp}, FN: {fn}, TN: {tn}")
    print(f"Precision: {precision:.4f} ({precision * 100:.2f}%)")
    print(f"Recall: {recall:.4f} ({recall * 100:.2f}%)")
    print("-" * 50)
    print(f"Z-Score detection rate (TP/Total): {z_tp}/{z_total} ({z_tp / z_total * 100:.2f}%)")
    print(f"Wash Trade detection rate (TP/Total): {w_tp}/{w_total} ({w_tp / w_total * 100:.2f}%)")
    print(f"Price Slippage detection rate (TP/Total): {s_tp}/{s_total} ({s_tp / s_total * 100:.2f}%)")

if __name__ == "__main__":
    run_evaluation()
