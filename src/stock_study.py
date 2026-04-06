import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
import shutil
import time

# --- 設定 ---
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
TIMEFRAMES = ["1m", "5m", "60m", "1d"]

# --- 環境判定とディレクトリ設定 ---
def setup_directories():
    """実行環境に応じて作業用(Local SSD)と保存用(Google Drive)のディレクトリを設定する"""
    is_colab = False
    try:
        from google.colab import drive
        is_colab = True
    except ImportError:
        pass
        
    is_kaggle = os.environ.get('KAGGLE_KERNEL_RUN_TYPE') is not None
    
    # スクリプトの場所からプロジェクトルートを取得
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    
    if is_colab:
        drive_path = "/content/drive/MyDrive/stock_data_hub"
        if not os.path.exists("/content/drive/MyDrive"):
            if os.path.exists("/content/drive/My Drive"):
                drive_path = "/content/drive/My Drive/stock_data_hub"
            else:
                print("Error: Google Drive is not mounted. Please mount it in the Colab cell first.")
        
        work_path = "/content/stock_data_work"
        print(f"Environment: Colab. Project Root: {project_root}")
        print(f"Drive Path: {drive_path}")
    elif is_kaggle:
        drive_path = "/kaggle/working/drive/MyDrive/stock_data_hub" 
        work_path = "/kaggle/working/stock_data_work"
        print(f"Environment: Kaggle. Work: {work_path}")
    else:
        drive_path = os.path.join(project_root, "data_drive")
        work_path = os.path.join(project_root, "data_work")
        print(f"Environment: Local. Project Root: {project_root}")
        print(f"Drive (Simulated): {drive_path}")

    os.makedirs(drive_path, exist_ok=True)
    os.makedirs(work_path, exist_ok=True)
    
    return drive_path, work_path

DRIVE_DIR, WORK_DIR = setup_directories()

# --- データベース操作 ---

def load_price_db(interval):
    """Google Driveの統合ファイルをローカル作業領域にコピーして読み込む"""
    drive_file = os.path.join(DRIVE_DIR, f"price_{interval}.parquet")
    work_file = os.path.join(WORK_DIR, f"price_{interval}.parquet")
    
    if os.path.exists(drive_file):
        print(f"Loading {interval} DB from Drive...")
        shutil.copy2(drive_file, work_file)
        df = pd.read_parquet(work_file)
        # タイムゾーンの正規化（念のため）
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        return df
    
    print(f"No existing DB found for {interval}. Creating new.")
    return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

def save_price_db(df, interval):
    """ローカルの作業ファイルをGoogle Driveに上書き保存する"""
    if df.empty: return
    
    work_file = os.path.join(WORK_DIR, f"price_{interval}.parquet")
    drive_file = os.path.join(DRIVE_DIR, f"price_{interval}.parquet")
    
    df.to_parquet(work_file, index=False)
    shutil.copy2(work_file, drive_file)
    print(f"Saved updated price_{interval}.parquet to Drive. Row count: {len(df)}")

# --- 銘柄ユニバース管理 ---

def update_universe(csv_filename="学習.csv"):
    """
    1. JPXから最新リストを単に保存（バックアップ）
    2. Google Drive上の指定のCSV（デフォルトは学習.csv）から銘柄コードを取得
    """
    # 1. JPXリストのバックアップ保存
    print("Backing up latest JPX list...")
    try:
        resp = requests.get(JPX_URL, timeout=10)
        jpx_save_path = os.path.join(DRIVE_DIR, "jpx_stock_list_raw.xls")
        with open(jpx_save_path, "wb") as f:
            f.write(resp.content)
        print(f"  JPX list saved to: {jpx_save_path}")
    except Exception as e:
        print(f"  Warning: Failed to backup JPX list: {e}")

    # 2. 指定された CSV の読み込み
    csv_path = os.path.join(DRIVE_DIR, csv_filename)
    if not os.path.exists(csv_path):
        # ローカルデバッグ用 fallback
        local_csv = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_drive", csv_filename)
        if os.path.exists(local_csv):
            csv_path = local_csv
        else:
            print(f"Error: {csv_path} not found. Please place it in Google Drive.")
            return []

    print(f"Reading target codes from: {os.path.basename(csv_path)}")
    try:
        df_uni = pd.read_csv(csv_path, encoding="utf-8")
        if "コード" not in df_uni.columns:
            codes = df_uni.iloc[:, 0].astype(str).tolist()
        else:
            codes = df_uni["コード"].astype(str).tolist()
        
        # クリーンアップ
        codes = [c.split('.')[0] for c in codes if c.strip()]
        print(f"  Successfully loaded {len(codes)} codes.")
        return codes
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []

# --- 株価取得・加工 ---

def merge_price_data(old_df, new_df):
    """結合・重複削除・ソート"""
    if new_df is None or new_df.empty:
        return old_df
        
    combined = pd.concat([old_df, new_df])
    combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    combined = combined.sort_values(["ticker", "date"])
    return combined

def parse_yfinance_batch(df_raw, chunk_tickers):
    """yf.downloadの一括結果をLong形式かつ日本時間に変換する"""
    if df_raw.empty:
        return pd.DataFrame()
    
    all_rows = []
    
    for ticker in chunk_tickers:
        symbol = f"{ticker}.T"
        try:
            if symbol in df_raw.columns.get_level_values(1):
                t_df = df_raw.xs(symbol, axis=1, level=1).copy()
            elif symbol in df_raw.columns.get_level_values(0):
                t_df = df_raw[symbol].copy()
            else:
                continue

            t_df = t_df.dropna(how="all")
            if t_df.empty: continue
            
            t_df = t_df.reset_index()
            t_df.columns = [str(c).lower() for c in t_df.columns]
            t_df = t_df.rename(columns={"datetime": "date", "index": "date"}) 
            
            # タイムゾーン情報の有無に応じて処理（1dなどはnaiveな場合があるため）
            dt_col = pd.to_datetime(t_df["date"])
            if dt_col.dt.tz is not None:
                t_df["date"] = dt_col.dt.tz_convert("Asia/Tokyo").dt.tz_localize(None)
            else:
                t_df["date"] = dt_col
            
            t_df["ticker"] = str(ticker)
            
            valid_cols = [c for c in ["date", "ticker", "open", "high", "low", "close", "volume"] if c in t_df.columns]
            all_rows.append(t_df[valid_cols])
        except Exception as e:
            print(f"  Warning: Failed to parse data for {ticker}: {e}")
            
    if not all_rows: return pd.DataFrame()
    return pd.concat(all_rows, ignore_index=True)

# --- データベース統合更新 (ハイブリッド型) ---

def update_price_database(csv_filename="学習.csv"):
    """銘柄の同期状況を判定して、最適なバッチ・期間で一括更新する"""
    tickers = update_universe(csv_filename)
    if not tickers: return

    # 制限用の時刻
    now = datetime.now()

    for interval in TIMEFRAMES:
        print(f"\n--- Checking Interval DB: {interval} ---")
        db_df = load_price_db(interval)
        
        # 各銘柄の最新日を把握
        last_updates_map = {}
        if not db_df.empty:
            # 銘柄ごとの最新日を抽出
            last_updates_map = db_df.groupby("ticker")["date"].max().to_dict()

        # ターゲット銘柄を「最新組」と「追っかけ組」に仕分ける
        # データベース全体の中での「最大公約数的な最新日」を基準にする
        global_max_date = max(last_updates_map.values()) if last_updates_map else None
        
        group_up_to_date = [] # 昨日のデータまである優等生
        group_catchup = []    # 新人、または同期が遅れている銘柄

        # 判定基準：最新日より1日以上遅れていたら「追っかけ組」
        # (※休日などを考慮して2日程度の余裕、または global_max そのものと比較)
        for t in tickers:
            t_last = last_updates_map.get(t)
            if t_last and global_max_date and t_last >= global_max_date:
                group_up_to_date.append(t)
            else:
                group_catchup.append(t)

        print(f"  Group Summary: {len(group_up_to_date)} up-to-date, {len(group_catchup)} need catch-up/new.")

        # --- 実行セクション：優等生グループ (Group A) ---
        # 差分のみなのでバッチを大きくして高速に回す
        all_downloaded = []
        
        if group_up_to_date:
            print(f"  ⚡ Synchronizing Group-A (Up-To-Date) ...")
            BATCH_A = 100
            start_date_a = (global_max_date + timedelta(days=1)).strftime("%Y-%m-%d")
            
            for i in range(0, len(group_up_to_date), BATCH_A):
                chunk = group_up_to_date[i:i+BATCH_A]
                print(f"    Batch {i//BATCH_A + 1}/{(len(group_up_to_date)-1)//BATCH_A + 1} ({len(chunk)} tickers)")
                
                symbols = [f"{t}.T" for t in chunk]
                try:
                    df_raw = yf.download(symbols, start=start_date_a, interval=interval, auto_adjust=False, actions=True, progress=False, threads=True)
                    chunk_processed = parse_yfinance_batch(df_raw, chunk)
                    if not chunk_processed.empty:
                        all_downloaded.append(chunk_processed)
                    
                    time.sleep(1.5) # 優等生は比較的軽いので少し短めのスリープ
                except Exception as e:
                    print(f"    Error A: {e}")

        # --- 実行セクション：遅延・新人グループ (Group B) ---
        # データ量が多くなる可能性があるのでバッチを小さくして確実に
        if group_catchup:
            print(f"  🚀 Catching up Group-B (Laggards/New) ...")
            BATCH_B = 25
            
            for i in range(0, len(group_catchup), BATCH_B):
                chunk = group_catchup[i:i+BATCH_B]
                print(f"    Batch {i//BATCH_B + 1}/{(len(group_catchup)-1)//BATCH_B + 1} ({len(chunk)} tickers)")
                
                # このバッチの中での「最も進んでいる人（もし居れば）」を確認し、最も古い開始日を決める
                batch_existing_df = db_df[db_df["ticker"].isin(chunk)] if not db_df.empty else pd.DataFrame()
                
                start_date_b = "2023-01-01" # デフォルト
                if not batch_existing_df.empty:
                    # バッチ内の現存銘柄の「最新日」の最小値から始める
                    batch_lasts = batch_existing_df.groupby("ticker")["date"].max()
                    if len(batch_lasts) == len(chunk):
                        # 全員DBには居るが遅れている場合
                        start_date_b = (batch_lasts.min() + timedelta(days=1)).strftime("%Y-%m-%d")
                
                # 時間軸ごとの制限を適用
                if interval == "1m":
                    limit = now - timedelta(days=5)
                elif interval == "5m":
                    limit = now - timedelta(days=58)
                elif interval == "60m":
                    limit = now - timedelta(days=720)
                else:
                    limit = None
                
                if limit and pd.to_datetime(start_date_b) < limit:
                    start_date_b = limit.strftime("%Y-%m-%d")

                symbols = [f"{t}.T" for t in chunk]
                try:
                    df_raw = yf.download(symbols, start=start_date_b, interval=interval, auto_adjust=False, actions=True, progress=False, threads=True)
                    chunk_processed = parse_yfinance_batch(df_raw, chunk)
                    if not chunk_processed.empty:
                        all_downloaded.append(chunk_processed)
                    
                    time.sleep(2.5) # 追っかけ組は重いので長めのスリープ
                except Exception as e:
                    print(f"    Error B: {e}")

        # --- 統合と保存 ---
        if all_downloaded:
            new_combined = pd.concat(all_downloaded, ignore_index=True)
            db_df = merge_price_data(db_df, new_combined)
            print(f"  Summary for {interval}: Processed {len(new_combined)} new rows.")
            save_price_db(db_df, interval)
        else:
            print(f"  No new data fetched for {interval}.")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Stock Data Pipeline")
    parser.add_argument("--csv", type=str, default="学習.csv", help="Target CSV filename in DRIVE_DIR")
    args = parser.parse_args()

    start_time = datetime.now()
    print(f"Pipeline started at {start_time}")
    
    import sys
    print(f"Current Working Directory: {os.getcwd()}")
    print(f"Script Location: {os.path.abspath(__file__)}")
    print(f"Using CSV Source: {args.csv}")
    
    update_price_database(args.csv)
    
    end_time = datetime.now()
    print(f"\nPipeline finished. Duration: {end_time - start_time}")

if __name__ == "__main__":
    main()
