import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
import shutil

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
        from google.colab import drive
        drive.mount('/content/drive')
        drive_path = "/content/drive/MyDrive/stock_data_hub"
        work_path = "/content/stock_data_work"
        print(f"Environment: Colab. Drive: {drive_path}")
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
        # タイムゾーンの正規化
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

def update_universe():
    """
    1. JPXから最新リストを単に保存（バックアップ）
    2. Google Drive上の『学習.csv』から銘柄コードを取得
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

    # 2. 学習.csv の読み込み
    csv_path = os.path.join(DRIVE_DIR, "学習.csv")
    if not os.path.exists(csv_path):
        # ローカルデバッグ用
        local_csv = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_drive", "学習.csv")
        if os.path.exists(local_csv):
            csv_path = local_csv
        else:
            print(f"Error: {csv_path} not found. Please place it in Google Drive.")
            return []

    print(f"Reading target codes from: {os.path.basename(csv_path)}")
    try:
        # 1列目が「コード」であることを前提に読み込み
        df_uni = pd.read_csv(csv_path, encoding="utf-8")
        if "コード" not in df_uni.columns:
            # カラム名が違う場合でも1列目を取得
            codes = df_uni.iloc[:, 0].astype(str).tolist()
        else:
            codes = df_uni["コード"].astype(str).tolist()
        
        # 不要な文字（ハイフン等）が含まれる場合のクリーンアップ
        codes = [c.split('.')[0] for c in codes if c.strip()]
        print(f"  Successfully loaded {len(codes)} codes.")
        return codes
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []

# --- 株価取得・加工 ---

def check_stock_splits(ticker, existing_ticker_df):
    """保存済み期間内に分割があるか、actionsデータで確認する"""
    if existing_ticker_df.empty:
        return True # 新規銘柄はフル取得
    
    symbol = f"{ticker}.T"
    y_ticker = yf.Ticker(symbol)
    try:
        actions = y_ticker.actions
        if actions.empty or "Stock Splits" not in actions.columns:
            return False
            
        splits = actions[actions["Stock Splits"] != 0].copy()
        if splits.empty:
            return False
            
        # タイムゾーンを消す
        splits.index = splits.index.tz_localize(None)
        
        last_saved_date = pd.to_datetime(existing_ticker_df["date"]).max()
        first_saved_date = pd.to_datetime(existing_ticker_df["date"]).min()
        
        # 保存範囲内で分割が発生しているか
        split_in_range = splits[(splits.index >= first_saved_date) & (splits.index <= last_saved_date)]
        if not split_in_range.empty:
            print(f"  Split detected for {ticker}: {split_in_range['Stock Splits'].tolist()}")
            return True
    except:
        pass
    return False

def download_missing_prices(ticker, interval, existing_ticker_df):
    """差分またはフル(分割時)でデータを取得する"""
    symbol = f"{ticker}.T"
    needs_full = check_stock_splits(ticker, existing_ticker_df)
    
    start_date = None
    if not needs_full:
        last_date = pd.to_datetime(existing_ticker_df["date"]).max()
        if pd.notnull(last_date):
            start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            if interval == "1m":
                limit = datetime.now() - timedelta(days=6)
                if last_date < limit: start_date = limit.strftime("%Y-%m-%d")

    try:
        if start_date:
            df = yf.download(symbol, start=start_date, interval=interval, auto_adjust=False, actions=True, progress=False)
        else:
            df = yf.download(symbol, period="max", interval=interval, auto_adjust=False, actions=True, progress=False)
            
        if df.empty: return None, False
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        df = df.rename(columns={"datetime": "date"})
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        df["ticker"] = str(ticker)
        
        cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
        df = df[[c for c in cols if c in df.columns]]
        
        return df, needs_full
    except Exception as e:
        print(f"Error downloading {ticker}: {e}")
        return None, False

def merge_price_data(old_df, new_df):
    """結合・重複削除・ソート"""
    if new_df is None or new_df.empty:
        return old_df
        
    combined = pd.concat([old_df, new_df])
    combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    combined = combined.sort_values(["ticker", "date"])
    return combined

# --- データベース統合更新 ---

def update_price_database():
    """全時間軸、全指定銘柄のDBをインクリメンタル更新する"""
    tickers = update_universe()
    
    if not tickers:
        print("No tickers found. Skipping update.")
        return

    for interval in TIMEFRAMES:
        print(f"\n--- Updating Interval DB: {interval} ---")
        db_df = load_price_db(interval)
        
        newly_fetched_total = 0
        split_fixes = 0
        
        print(f"Processing {len(tickers)} tickers for {interval}...")
        
        for ticker in tickers:
            existing_ticker_data = db_df[db_df["ticker"] == str(ticker)]
            new_data, is_full = download_missing_prices(ticker, interval, existing_ticker_data)
            
            if new_data is not None and not new_data.empty:
                if is_full:
                    db_df = db_df[db_df["ticker"] != str(ticker)]
                    split_fixes += 1
                
                db_df = merge_price_data(db_df, new_data)
                newly_fetched_total += len(new_data)
        
        if newly_fetched_total > 0 or split_fixes > 0:
            print(f"Summary for {interval}: Added {newly_fetched_total} rows, Corrected {split_fixes} splits.")
            save_price_db(db_df, interval)
        else:
            print(f"No new data for {interval}.")

def main():
    start_time = datetime.now()
    print(f"Pipeline started at {start_time}")
    
    import sys
    print(f"Current Working Directory: {os.getcwd()}")
    print(f"Script Location: {os.path.abspath(__file__)}")
    
    update_price_database()
    
    end_time = datetime.now()
    print(f"\nPipeline finished. Duration: {end_time - start_time}")

if __name__ == "__main__":
    main()
