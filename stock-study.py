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
    
    if is_colab:
        from google.colab import drive
        drive.mount('/content/drive')
        drive_path = "/content/drive/MyDrive/stock_data_hub"
        work_path = "/content/stock_data_work"
        print(f"Environment: Colab. Drive: {drive_path}")
    elif is_kaggle:
        # KaggleではDrive認証はユーザーが行う必要があるが、パスとしては以下を想定
        drive_path = "/kaggle/working/drive/MyDrive/stock_data_hub" 
        work_path = "/kaggle/working/stock_data_work"
        print(f"Environment: Kaggle. Work: {work_path}")
    else:
        drive_path = "./data_drive"
        work_path = "./data_work"
        print(f"Environment: Local. Drive (Simulated): {drive_path}")

    os.makedirs(drive_path, exist_ok=True)
    os.makedirs(work_path, exist_ok=True)
    
    return drive_path, work_path

DRIVE_DIR, WORK_DIR = setup_directories()
UNIVERSE_PATH_DRIVE = os.path.join(DRIVE_DIR, "universe_latest.parquet")
UNIVERSE_PATH_WORK = os.path.join(WORK_DIR, "universe_latest.parquet")

# --- データベース操作 ---

def load_price_db(interval):
    """Google Driveの統合ファイルをローカル作業領域にコピーして読み込む"""
    drive_file = os.path.join(DRIVE_DIR, f"price_{interval}.parquet")
    work_file = os.path.join(WORK_DIR, f"price_{interval}.parquet")
    
    if os.path.exists(drive_file):
        print(f"Loading {interval} DB from Drive...")
        shutil.copy2(drive_file, work_file)
        return pd.read_parquet(work_file)
    
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
    """JPXから最新リストを取得し、前回との差分（新規追加銘柄）を返す"""
    print("Fetching latest JPX list...")
    resp = requests.get(JPX_URL)
    temp_xls = "temp_jpx.xls"
    with open(temp_xls, "wb") as f:
        f.write(resp.content)
    df_raw = pd.read_excel(temp_xls)
    os.remove(temp_xls)
    
    # プライム・スタンダード限定
    df_new = df_raw[df_raw["市場・商品区分"].str.contains("プライム|スタンダード", na=False)].copy()
    new_tickers = df_new["コード"].astype(str).tolist()
    
    old_tickers = []
    if os.path.exists(UNIVERSE_PATH_DRIVE):
        old_df = pd.read_parquet(UNIVERSE_PATH_DRIVE)
        old_tickers = old_df["ticker"].astype(str).tolist()
    
    added = list(set(new_tickers) - set(old_tickers))
    removed = list(set(old_tickers) - set(new_tickers))
    
    # 最新を保存
    pd.DataFrame({"ticker": new_tickers}).to_parquet(UNIVERSE_PATH_DRIVE, index=False)
    
    print(f"Universe Sync: Total={len(new_tickers)}, Added={len(added)}, Removed={len(removed)}")
    return new_tickers, added

# --- 株価取得・加工 ---

def check_stock_splits(ticker, existing_ticker_df):
    """保存済み期間内に分割があるか、actionsデータで確認する"""
    if existing_ticker_df.empty:
        return True # 新規銘柄はフル取得
    
    symbol = f"{ticker}.T"
    y_ticker = yf.Ticker(symbol)
    actions = y_ticker.actions
    
    if actions.empty or "Stock Splits" not in actions.columns:
        return False
        
    splits = actions[actions["Stock Splits"] != 0]
    if splits.empty:
        return False
        
    last_saved_date = pd.to_datetime(existing_ticker_df["date"]).max()
    first_saved_date = pd.to_datetime(existing_ticker_df["date"]).min()
    
    # 保存範囲内で分割が発生しているか
    split_in_range = splits[(splits.index >= first_saved_date) & (splits.index <= last_saved_date)]
    if not split_in_range.empty:
        return True
        
    return False

def download_missing_prices(ticker, interval, existing_ticker_df):
    """差分またはフル(分割時)でデータを取得する"""
    symbol = f"{ticker}.T"
    needs_full = check_stock_splits(ticker, existing_ticker_df)
    
    start_date = None
    if not needs_full:
        # 最新の日付の翌日から取得
        last_date = pd.to_datetime(existing_ticker_df["date"]).max()
        if pd.notnull(last_date):
            start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            # 1分足等の取得期間制限に注意
            if interval == "1m":
                limit = datetime.now() - timedelta(days=6)
                if last_date < limit: start_date = limit.strftime("%Y-%m-%d")

    try:
        if start_date:
            df = yf.download(symbol, start=start_date, interval=interval, auto_adjust=False, actions=True, progress=False)
        else:
            df = yf.download(symbol, period="max", interval=interval, auto_adjust=False, actions=True, progress=False)
            
        if df.empty: return None, False
        
        # 整形
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        df = df.rename(columns={"datetime": "date"})
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        df["ticker"] = str(ticker)
        
        # 必要なカラムのみ
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
    """全時間軸、全銘柄のDBをインクリメンタル更新する"""
    tickers, added_tickers = update_universe()
    
    # 全銘柄を回る
    for interval in TIMEFRAMES:
        print(f"\n--- Updating Interval DB: {interval} ---")
        db_df = load_price_db(interval)
        
        # date型の整形
        if not db_df.empty:
            db_df["date"] = pd.to_datetime(db_df["date"]).dt.tz_localize(None)
        
        newly_fetched_total = 0
        split_fixes = 0
        
        # テスト・デモ用に最初の50銘柄のみに制限（本番は tickers を使用）
        process_list = tickers[:50] 
        print(f"Processing {len(process_list)} tickers...")
        
        results = []
        for ticker in process_list:
            existing_ticker_data = db_df[db_df["ticker"] == str(ticker)]
            
            new_data, is_full = download_missing_prices(ticker, interval, existing_ticker_data)
            
            if new_data is not None and not new_data.empty:
                if is_full:
                    # 分割修正のため、一旦その銘柄をDBから削除
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
    
    update_price_database()
    
    end_time = datetime.now()
    print(f"\nPipeline finished. Duration: {end_time - start_time}")

if __name__ == "__main__":
    main()
