import pandas as pd
import yfinance as yf
import os
from datetime import datetime
import io
import requests

# --- 設定 ---
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
# Google Drive上の保存先パス (Colab環境を想定)
# --- 環境設定 ---
def get_environment():
    """実行環境を判定する (colab, kaggle, local)"""
    if os.environ.get('KAGGLE_KERNEL_RUN_TYPE'):
        return 'kaggle'
    try:
        import google.colab
        return 'colab'
    except ImportError:
        return 'local'

def setup_storage():
    """環境に応じたストレージ設定を行い、ベースディレクトリを返す"""
    env = get_environment()
    
    if env == 'colab':
        print("Detected environment: Google Colab")
        from google.colab import drive
        drive.mount('/content/drive')
        return "/content/drive/MyDrive/stock_data_hub"
        
    elif env == 'kaggle':
        print("Detected environment: Kaggle")
        # Kaggleの場合、GDriveから /kaggle/working (SSD) へコピーして使う想定
        # ※認証や実際のコピー処理は、Kaggle Notebook側のセルで行うか、
        # ここに実装を追加することも可能。
        base_ssd = "/kaggle/working/stock_data_hub"
        os.makedirs(base_ssd, exist_ok=True)
        return base_ssd
        
    else:
        print("Detected environment: Local")
        return "./data"

BASE_DIR = setup_storage()
UNIVERSE_DIR = os.path.join(BASE_DIR, "universe")
PRICE_DIR = os.path.join(BASE_DIR, "price")

os.makedirs(UNIVERSE_DIR, exist_ok=True)
os.makedirs(PRICE_DIR, exist_ok=True)

# 取得する時間軸と最大期間の定義
# 5mから15m, 30m、60mから90mをそれぞれリサンプリングで生成
TIMEFRAMES = {
    "1m": "max",
    "5m": "max",
    "60m": "max",
    "1d": "max" 
}

RESAMPLE_MAP = {
    # (元となる時間軸, 生成する時間軸, リサンプリングルール)
    "5m": [("15m", "15T"), ("30m", "30T")],
    "60m": [("90m", "90T")]
}

def download_jpx_list():
    """JPXから銘柄一覧(Excel)をダウンロードしてDataFrameで返す"""
    print(f"Downloading JPX list from {JPX_URL}...")
    response = requests.get(JPX_URL)
    response.raise_for_status()
    # Excelファイルを読み込み
    df = pd.read_excel(io.BytesIO(response.content))
    return df

def filter_by_market(df):
    """市場区分でフィルタリング (プライム・スタンダードのみ)"""
    # カラム名などは構造に合わせる必要がある (通常: '市場・商品区分')
    target_markets = ["プライム（内国株式）", "スタンダード（内国株式）"]
    df_filtered = df[df["市場・商品区分"].isin(target_markets)].copy()
    return df_filtered

def get_dynamic_metrics(tickers):
    """yfinanceを使用して時価総額と売買代金を取得する"""
    results = []
    # 負荷軽減のためバッチ処理を検討すべきだが、まずは簡易実装
    for ticker in tickers:
        symbol = f"{ticker}.T"
        try:
            t = yf.Ticker(symbol)
            info = t.info
            # 直近の売買代金 (ボリューム * 価格) の簡易計算、または履歴から計算
            # ここでは info の averageVolume * currentPrice で概算
            market_cap = info.get("marketCap", 0)
            price = info.get("currentPrice") or info.get("regularMarketPrice", 0)
            avg_vol = info.get("averageVolume", 0)
            liquidity = avg_vol * price
            
            results.append({
                "Ticker": ticker,
                "MarketCap": market_cap,
                "Liquidity": liquidity
            })
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            continue
    return pd.DataFrame(results)

def adjust_splits(df):
    """
    actions=Trueで取得したdfに対し、Stock Splitsを用いて過去に遡って価格を修正する。
    配当落ち(Dividends)は無視する。
    """
    if 'Stock Splits' not in df.columns or df['Stock Splits'].sum() == 0:
        return df
    
    # 分割比率の累積値を計算 (現在から過去へ)
    splits = df['Stock Splits'].copy()
    splits = splits.replace(0, 1)
    
    # 累積分割係数: 現在を1.0として、過去に遡るほど (1/分割数) を掛けていく
    # cumulative_factor[i] = PRODUCT(1/splits[j] for j from i+1 to end)
    cumulative_factor = (1 / splits).iloc[::-1].cumprod().iloc[::-1]
    cumulative_factor = cumulative_factor.shift(-1).fillna(1.0)
    
    for col in ['Open', 'High', 'Low', 'Close']:
        if col in df.columns:
            df[col] = df[col] * cumulative_factor
            
    return df

def download_price_data(ticker, interval, period, start=None):
    """
    指定した銘柄・時間軸のデータを取得して分割調整を行い、保存する。
    startが指定されている場合は、その日付以降の差分のみを取得して追記する。
    """
    symbol = f"{ticker}.T"
    try:
        if start:
            # 差分更新
            df_new = yf.download(symbol, start=start, interval=interval, auto_adjust=False, actions=True, progress=False)
        else:
            # 新規取得
            df_new = yf.download(symbol, period=period, interval=interval, auto_adjust=False, actions=True, progress=False)
            
        if df_new.empty:
            return None
        
        # MultiIndexの解消
        if isinstance(df_new.columns, pd.MultiIndex):
            df_new.columns = df_new.columns.get_level_values(0)
        df_new.columns = [str(c).lower() for c in df_new.columns]
        
        save_path = os.path.join(PRICE_DIR, interval, f"{ticker}.parquet")
        
        # 既存データの読み込み (差分更新の場合)
        if start and os.path.exists(save_path):
            df_old = pd.read_parquet(save_path)
            # 重複を排除して結合
            df_combined = pd.concat([df_old, df_new]).sort_index()
            df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
        else:
            df_combined = df_new

        # 分割調整 (ロバスト版)
        # 差分に分割が含まれているか、または新規取得の場合は全体に適用
        split_col = 'stock splits' if 'stock splits' in df_combined.columns else 'Stock Splits'
        if split_col in df_combined.columns and df_combined[split_col].sum() > 0:
            # 分割を検知した場合
            if start:
                print(f" !!! Split detected in increment for {ticker}. Forcing full re-calculation...")
                return download_price_data(ticker, interval, period="max") # 再帰的にフル取得
            
            # フル調整ロジック
            split_events = df_combined[df_combined[split_col] != 0].copy().sort_index()
            adjustment_factor = pd.Series(1.0, index=df_combined.index)
            
            for date in split_events.index:
                split_ratio = split_events.loc[date, split_col]
                if isinstance(split_ratio, pd.Series): split_ratio = split_ratio.iloc[0]
                if split_ratio <= 0 or split_ratio == 1.0: continue
                
                try:
                    idx = df_combined.index.get_loc(date)
                    if idx > 0:
                        price_before = df_combined['close'].iloc[idx-1]
                        price_after = df_combined['close'].iloc[idx]
                        if price_after > 0:
                            actual_ratio = price_before / price_after
                            if abs(actual_ratio - split_ratio) < abs(actual_ratio - 1.0):
                                adjustment_factor.iloc[:idx] *= (1 / split_ratio)
                except:
                    continue
            
            for col in ['open', 'high', 'low', 'close']:
                if col in df_combined.columns:
                    df_combined[col] = df_combined[col] * adjustment_factor
        
        # 保存
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df_combined.to_parquet(save_path)
        return df_combined
    except Exception as e:
        print(f"Error processing {symbol} ({interval}): {e}")
        return None

def process_universe():
    """ユニバースの更新と全銘柄の初期ダウンロード/更新を行う"""
    # 1. JPXリスト取得
    df_jpx = download_jpx_list()
    
    # 2. 市場区分フィルタ
    df_universe = filter_by_market(df_jpx)
    tickers = df_universe["コード"].astype(str).tolist()
    
    print(f"Total candidates after market filtering: {len(tickers)}")
    
    # 3. 動的監視 (時価総額・流動性)
    print("Fetching metrics for all candidates (this may take time)...")
    # 本番運用ではここで並列処理(ThreadPoolExecutor等)を推奨
    metrics_df = get_dynamic_metrics(tickers)
    
    # フィルタ条件
    train_mask = (metrics_df["MarketCap"] >= 1e10) & (metrics_df["MarketCap"] <= 1e12) & (metrics_df["Liquidity"] >= 1e8)
    ops_mask = (metrics_df["MarketCap"] >= 3e10) & (metrics_df["MarketCap"] <= 5e11) & (metrics_df["Liquidity"] >= 5e8)
    
    train_list = metrics_df[train_mask]
    ops_list = metrics_df[ops_mask]
    
    # ユニバース保存
    timestamp = datetime.now().strftime("%Y%m%d")
    train_list.to_parquet(f"{UNIVERSE_DIR}/universe_train_{timestamp}.parquet")
    ops_list.to_parquet(f"{UNIVERSE_DIR}/universe_ops_{timestamp}.parquet")
    
    # 最新版へのシンボリックリンク的保存 (更新用)
    train_list.to_parquet(f"{UNIVERSE_DIR}/universe_train_latest.parquet")
    ops_list.to_parquet(f"{UNIVERSE_DIR}/universe_ops_latest.parquet")
    
    print(f"Universe updated. Training: {len(train_list)}, Operational: {len(ops_list)}")
    return train_list, ops_list

def update_all_prices(ticker_list):
    """リストにある全銘柄の価格データを更新する"""
    for ticker in ticker_list:
        # 代表して '1d' の既存データから開始日を決定
        check_path = os.path.join(PRICE_DIR, "1d", f"{ticker}.parquet")
        start_date = None
        if os.path.exists(check_path):
            existing_df = pd.read_parquet(check_path)
            start_date = (existing_df.index.max() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"Updating {ticker}...")
        downloaded_dfs = {}
        for interval, period in TIMEFRAMES.items():
            df = download_price_data(ticker, interval, period, start=start_date)
            if df is not None:
                downloaded_dfs[interval] = df
        
        # リサンプリング生成
        for base_interval, targets in RESAMPLE_MAP.items():
            if base_interval in downloaded_dfs:
                base_df = downloaded_dfs[base_interval]
                for target_interval, rule in targets:
                    resampled = base_df.resample(rule).agg({
                        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
                    }).dropna()
                    save_path = os.path.join(PRICE_DIR, target_interval, f"{ticker}.parquet")
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    resampled.to_parquet(save_path)

def main():
    # 本番運用の流れ: 
    # 1. 銘柄ユニバースを更新 (月1回程度)
    # train_list, ops_list = process_universe()
    
    # 2. 全銘柄の価格を更新 (毎日)
    # tickers = pd.read_parquet(f"{UNIVERSE_DIR}/universe_train_latest.parquet")["Ticker"].tolist()
    # update_all_prices(tickers)
    
    # --- デモ/検証用コード ---
    print("--- Running Demo/Verification ---")
    
    # テスト対象を選択 (1301.T)
    target_ticker = "1301"
    
    # 分割調整の検証 (トヨタ: 7203.T)
    print("\nVerifying split adjustment for 7203.T...")
    download_price_data("7203", "1d", "5y")
    final_toyota = pd.read_parquet(os.path.join(PRICE_DIR, "1d", "7203.parquet"))
    final_toyota.index = pd.to_datetime(final_toyota.index).date
    d_b, d_a = datetime(2021, 9, 28).date(), datetime(2021, 9, 29).date()
    if d_b in final_toyota.index and d_a in final_toyota.index:
        p_b, p_a = final_toyota.loc[d_b, "close"], final_toyota.loc[d_a, "close"]
        if hasattr(p_b, "__iter__"): p_b = p_b.iloc[0]
        if hasattr(p_a, "__iter__"): p_a = p_a.iloc[0]
        print(f" - Ratio 2021-09-28/29: {p_a/p_b:.4f} (Target: ~1.0)")

    print(f"\nPipeline demonstration complete. Results in {BASE_DIR}")

if __name__ == "__main__":
    main()
