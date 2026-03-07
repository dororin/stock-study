import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
import shutil

# --- 設定 ---
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
TIMEFRAMES = ["1m", "5m", "60m", "1d"]

# --- ユニバース条件 ---
# 学習用
L_MIN_CAP = 100 * 10**8      # 100億円
L_MAX_CAP = 1 * 10**12       # 1兆円
L_MIN_TURNOVER = 1 * 10**8   # 1億円

# 運用用
O_MIN_CAP = 300 * 10**8      # 300億円
O_MAX_CAP = 5000 * 10**8     # 5000億円
O_MIN_TURNOVER = 5 * 10**8   # 5億円

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
    
    # プロジェクトルートの特定 (srcの親ディレクトリ)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    
    if is_colab:
        drive_mount_point = "/content/drive"
        
        # 複数の候補を検索
        possible_drive_roots = [
            os.path.join(drive_mount_point, "My Drive"),
            os.path.join(drive_mount_point, "MyDrive"),
            "/gdrive/My Drive"
        ]
        
        drive_path = None
        for root in possible_drive_roots:
            if os.path.exists(root):
                drive_path = os.path.join(root, "stock_data_hub")
                break
        
        if drive_path is None:
            # マウントされていないかパスが特殊な場合
            print("\n" + "!"*50)
            print("WARNING: Google Drive is NOT detected at standard paths.")
            print("Defaulting to local VM storage (NOT persistent).")
            print("Please run: from google.colab import drive; drive.mount('/content/drive')")
            print("!"*50 + "\n")
            drive_path = "/content/stock_data_hub_local"
                
        work_path = "/content/stock_data_work"
        print(f"Environment: Colab. Project Root: {project_root}")
        print(f"Drive Path: {drive_path}")
    elif is_kaggle:
        drive_path = "/kaggle/working/stock_data_hub" 
        work_path = "/kaggle/working/stock_data_work"
        print(f"Environment: Kaggle. Work: {work_path}")
    else:
        drive_path = os.path.join(project_root, "data_drive")
        work_path = os.path.join(project_root, "data_work")
        print(f"Environment: Local.")
        print(f"Project Root: {project_root}")
        print(f"Drive Path: {drive_path}")

    os.makedirs(drive_path, exist_ok=True)
    os.makedirs(work_path, exist_ok=True)
    
    return drive_path, work_path

DRIVE_DIR, WORK_DIR = setup_directories()
UN_L_DRIVE = os.path.join(DRIVE_DIR, "universe_learning.parquet")
UN_O_DRIVE = os.path.join(DRIVE_DIR, "universe_operational.parquet")

# --- データベース操作 ---

def load_price_db(interval):
    """Google Driveの統合ファイルをローカル作業領域にコピーして読み込む"""
    drive_file = os.path.join(DRIVE_DIR, f"price_{interval}.parquet")
    work_file = os.path.join(WORK_DIR, f"price_{interval}.parquet")
    
    if os.path.exists(drive_file):
        print(f"Loading {interval} DB from Drive...")
        shutil.copy2(drive_file, work_file)
        df = pd.read_parquet(work_file)
        # 強制的にタイムゾーンを消去 (TypeError対策を強化)
        if not df.empty and "date" in df.columns:
            db_date_type = str(df["date"].dtype)
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
            print(f"Loaded {interval} DB. Original date type: {db_date_type}, New: {df['date'].dtype}")
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
    """JPXから取得した全銘柄を時価総額と流動性でフィルタリングし、学習用と運用用のリストを保存する"""
    print("Fetching latest JPX list...")
    resp = requests.get(JPX_URL)
    temp_xls = "temp_jpx.xls"
    with open(temp_xls, "wb") as f:
        f.write(resp.content)
    df_raw = pd.read_excel(temp_xls)
    os.remove(temp_xls)
    
    # 東証プライム・スタンダード限定
    df_base = df_raw[df_raw["市場・商品区分"].str.contains("プライム|スタンダード", na=False)].copy()
    all_tickers = df_base["コード"].astype(str).tolist()
    print(f"Base Universe (Prime/Standard): {len(all_tickers)} tickers")

    # 1. 売買代金（流動性）の取得とフィルタリング
    # yf.downloadで一括取得（1ヶ月分の1dデータ）
    print("Downloading 1-month daily data for turnover analysis...")
    symbols = [f"{t}.T" for t in all_tickers]
    
    # チャンク分けして取得（APIエラー回避）
    chunk_size = 200
    all_turnovers = []
    
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        print(f"  Downloading turnover data: chunk {i//chunk_size + 1}/{len(symbols)//chunk_size + 1}...")
        data = yf.download(chunk, period="1mo", interval="1d", progress=False, group_by='ticker')
        
        for sym in chunk:
            if sym in data.columns.get_level_values(0):
                ticker_data = data[sym].dropna()
                if not ticker_data.empty and "Close" in ticker_data.columns and "Volume" in ticker_data.columns:
                    # 売買代金 = 終値 * 出来高
                    turnover = (ticker_data["Close"] * ticker_data["Volume"]).mean()
                    ticker_code = sym.replace(".T", "")
                    all_turnovers.append({"ticker": ticker_code, "turnover": turnover})
    
    if not all_turnovers:
        print("Failed to fetch any turnover data.")
        return []
        
    df_prices = pd.DataFrame(all_turnovers)
    
    # 流動性フィルタ（学習用と運用用の低い方を基準に残す）
    min_needed_turnover = min(L_MIN_TURNOVER, O_MIN_TURNOVER)
    df_filtered = df_prices[df_prices["turnover"] >= min_needed_turnover].copy()
    print(f"Tickers passing liquidity filter: {len(df_filtered)}")

    # 2. 時価総額の取得と最終フィルタ
    print("Fetching market capitalization for candidate tickers...")
    symbols_to_check = [f"{t}.T" for t in df_filtered["ticker"].tolist()]
    
    learning_list = []
    operational_list = []
    
    # yf.Tickersを使ってfast_infoを一括取得
    for i in range(0, len(symbols_to_check), chunk_size):
        chunk = symbols_to_check[i:i+chunk_size]
        print(f"  Fetching market cap: chunk {i//chunk_size + 1}/{len(symbols_to_check)//chunk_size + 1}...")
        tickers_obj = yf.Tickers(" ".join(chunk))
        for sym in chunk:
            try:
                mkt_cap = tickers_obj.tickers[sym].fast_info['marketCap']
                if mkt_cap is None: continue
                
                ticker_base = sym.replace(".T", "")
                turnover = df_filtered[df_filtered["ticker"] == ticker_base]["turnover"].values[0]
                
                # 学習用
                if L_MIN_CAP <= mkt_cap <= L_MAX_CAP and turnover >= L_MIN_TURNOVER:
                    learning_list.append(ticker_base)
                
                # 運用用
                if O_MIN_CAP <= mkt_cap <= O_MAX_CAP and turnover >= O_MIN_TURNOVER:
                    operational_list.append(ticker_base)
            except:
                continue

    # 保存
    pd.DataFrame({"ticker": learning_list}).to_parquet(UN_L_DRIVE, index=False)
    pd.DataFrame({"ticker": operational_list}).to_parquet(UN_O_DRIVE, index=False)
    
    print(f"Final Universe Counts: Learning={len(learning_list)}, Operational={len(operational_list)}")
    
    # 統合したリスト（データ取得対象）を返す
    update_targets = list(set(learning_list) | set(operational_list))
    return update_targets

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
    # タイムゾーンを消して比較 (TypeError回避)
    splits_no_tz = splits.index.tz_localize(None)
    
    # 型不整合のデバッグ出力を追加 (問題発生時のみ)
    try:
        split_in_range = splits[(splits_no_tz >= first_saved_date) & (splits_no_tz <= last_saved_date)]
    except TypeError as e:
        print(f"DEBUG - Comparison Error for {ticker}:")
        print(f"  splits.index: {splits.index.dtype}")
        print(f"  splits_no_tz: {splits_no_tz.dtype}")
        print(f"  first_saved_date: {type(first_saved_date)} / {first_saved_date}")
        raise e
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
    tickers = update_universe()
    
    if not tickers:
        print("No tickers in universe. Skipping update.")
        return

    # 全銘柄を回る
    for interval in TIMEFRAMES:
        print(f"\n--- Updating Interval DB: {interval} ---")
        db_df = load_price_db(interval)
        
        # date型の整形
        if not db_df.empty:
            db_df["date"] = pd.to_datetime(db_df["date"]).dt.tz_localize(None)
        
        newly_fetched_total = 0
        split_fixes = 0
        
        print(f"Processing {len(tickers)} tickers for {interval}...")
        
        for ticker in tickers:
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
    print(f"Current Working Directory: {os.getcwd()}")
    print(f"Script Location: {os.path.abspath(__file__)}")
    
    update_price_database()
    
    end_time = datetime.now()
    print(f"\nPipeline finished. Duration: {end_time - start_time}")

if __name__ == "__main__":
    main()
