#!/usr/bin/env python3
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ========== 获取数据 ==========
ticker = yf.Ticker("^GSPC")
df_long = ticker.history(period="5y", interval="1d")
df_mid = ticker.history(period="1y", interval="1d")
df_short = ticker.history(period="3mo", interval="1d")

print(f"长期数据: {df_long.index[0].strftime('%Y-%m-%d')} ~ {df_long.index[-1].strftime('%Y-%m-%d')}, {len(df_long)} 天")
print(f"中期数据: {df_mid.index[0].strftime('%Y-%m-%d')} ~ {df_mid.index[-1].strftime('%Y-%m-%d')}, {len(df_mid)} 天")
print(f"短期数据: {df_short.index[0].strftime('%Y-%m-%d')} ~ {df_short.index[-1].strftime('%Y-%m-%d')}, {len(df_short)} 天")

# ========== 计算均线 ==========
for ma in [5, 20, 60, 120, 250]:
    df_long[f'MA{ma}'] = df_long['Close'].rolling(window=ma).mean()
for ma in [5, 20, 60, 120]:
    df_mid[f'MA{ma}'] = df_mid['Close'].rolling(window=ma).mean()
for ma in [5, 10, 20, 60]:
    df_short[f'MA{ma}'] = df_short['Close'].rolling(window=ma).mean()

# ========== 关键数值 ==========
latest = df_long['Close'].iloc[-1]
ma250 = df_long['MA250'].iloc[-1]
ma120 = df_long['MA120'].iloc[-1]
ma60_long = df_long['MA60'].iloc[-1]
ma60_mid = df_mid['MA60'].iloc[-1]
ma20_mid = df_mid['MA20'].iloc[-1]
ma5_s = df_short['MA5'].iloc[-1]
ma20_s = df_short['MA20'].iloc[-1]

deviation_250 = (latest - ma250) / ma250 * 100
dev_60 = (latest - ma60_mid) / ma60_mid * 100
dev_20 = (latest - ma20_mid) / ma20_mid * 100

ma250_slope = (df_long['MA250'].iloc[-1] - df_long['MA250'].iloc[-60]) / 60
ma120_slope = (df_long['MA120'].iloc[-1] - df_long['MA120'].iloc[-60]) / 60
ma60_slope_mid = (df_mid['MA60'].iloc[-1] - df_mid['MA60'].iloc[-20]) / 20
ma20_slope_mid = (df_mid['MA20'].iloc[-1] - df_mid['MA20'].iloc[-10]) / 10

recent_high = df_short['Close'].max()
recent_low = df_short['Close'].min()
high_date = df_short['Close'].idxmax()
low_date = df_short['Close'].idxmin()
drawdown = (latest - recent_high) / recent_high * 100
recovery = (latest - recent_low) / recent_low * 100

# 趋势判断
from scipy.signal import argrelextrema
prices = df_short['Close'].values
try:
    local_max_idx = argrelextrema(prices, np.greater, order=5)[0]
    local_min_idx = argrelextrema(prices, np.less, order=5)[0]
    if len(local_max_idx) >= 2 and len(local_min_idx) >= 2:
        highs_rising = prices[local_max_idx[-1]] > prices[local_max_idx[-2]]
        lows_rising = prices[local_min_idx[-1]] > prices[local_min_idx[-2]]
        if highs_rising and lows_rising:
            trend_short = "UPTREND (higher highs + higher lows)"
        elif not highs_rising and not lows_rising:
            trend_short = "DOWNTREND (lower highs + lower lows)"
        else:
            trend_short = "CONSOLIDATION (mixed signals)"
    else:
        trend_short = "Insufficient swings"
except:
    trend_short = "N/A"

# ========== 画图 ==========
fig, axes = plt.subplots(3, 1, figsize=(18, 24))
fig.suptitle('S&P 500 Trend Analysis\n(MrBrain Framework: Price + Moving Averages = Pendulum Model)',
             fontsize=18, fontweight='bold', y=0.98)

# --- 长期 ---
ax1 = axes[0]
ax1.set_title('LONG-TERM (5Y) | MA250 = Annual Line = Value Center (the "Owner" in Graham\'s Dog Metaphor)',
              fontsize=13, fontweight='bold', color='darkred')
ax1.plot(df_long.index, df_long['Close'], color='black', linewidth=0.8, alpha=0.7, label='Price')
ax1.plot(df_long.index, df_long['MA60'], color='blue', linewidth=1.5, label='MA60 (Q)')
ax1.plot(df_long.index, df_long['MA120'], color='orange', linewidth=2, label='MA120 (H)')
ax1.plot(df_long.index, df_long['MA250'], color='red', linewidth=2.5, label='MA250 (Y) = Value Center')

# 填充偏离区域
ax1.fill_between(df_long.index, df_long['Close'], df_long['MA250'],
                 where=df_long['Close'] > df_long['MA250'],
                 alpha=0.1, color='red', label='Overvalued zone')
ax1.fill_between(df_long.index, df_long['Close'], df_long['MA250'],
                 where=df_long['Close'] < df_long['MA250'],
                 alpha=0.1, color='green', label='Undervalued zone')

ax1.annotate(f'Current: {latest:.0f}\nMA250: {ma250:.0f} ({deviation_250:+.1f}%)\nMA120: {ma120:.0f}',
            xy=(df_long.index[-1], latest), fontsize=11, fontweight='bold',
            xytext=(-250, 30), textcoords='offset points',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.8),
            arrowprops=dict(arrowstyle='->', color='black'))

dir250 = "UP" if ma250_slope > 0 else "DOWN"
dir120 = "UP" if ma120_slope > 0 else "DOWN"
ax1.text(0.02, 0.95,
         f'MA250 Direction: {dir250} ({ma250_slope:+.1f}/day)\n'
         f'MA120 Direction: {dir120} ({ma120_slope:+.1f}/day)\n'
         f'Price vs MA250: {deviation_250:+.1f}%\n'
         f'5Y High: {df_long["Close"].max():.0f} | 5Y Low: {df_long["Close"].min():.0f}',
         transform=ax1.transAxes, fontsize=11, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.9))

ax1.legend(loc='lower right', fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.set_ylabel('Price', fontsize=12)

# --- 中期 ---
ax2 = axes[1]
ax2.set_title('MID-TERM (1Y) | MA60 = Quarterly Line = Medium Pendulum',
              fontsize=13, fontweight='bold', color='darkblue')
ax2.plot(df_mid.index, df_mid['Close'], color='black', linewidth=1, alpha=0.7, label='Price')
ax2.plot(df_mid.index, df_mid['MA5'], color='green', linewidth=1, alpha=0.6, label='MA5')
ax2.plot(df_mid.index, df_mid['MA20'], color='purple', linewidth=1.5, label='MA20 (M)')
ax2.plot(df_mid.index, df_mid['MA60'], color='blue', linewidth=2, label='MA60 (Q)')
ax2.plot(df_mid.index, df_mid['MA120'], color='orange', linewidth=2.5, label='MA120 (H)')

ax2.fill_between(df_mid.index, df_mid['Close'], df_mid['MA60'],
                 where=df_mid['Close'] > df_mid['MA60'],
                 alpha=0.1, color='red')
ax2.fill_between(df_mid.index, df_mid['Close'], df_mid['MA60'],
                 where=df_mid['Close'] < df_mid['MA60'],
                 alpha=0.1, color='green')

dir60 = "UP" if ma60_slope_mid > 0 else "DOWN"
dir20 = "UP" if ma20_slope_mid > 0 else "DOWN"
ax2.text(0.02, 0.95,
         f'MA60 Direction: {dir60}\nMA20 Direction: {dir20}\n'
         f'Price vs MA60: {dev_60:+.1f}%\nPrice vs MA20: {dev_20:+.1f}%',
         transform=ax2.transAxes, fontsize=11, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))

ax2.legend(loc='lower right', fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.set_ylabel('Price', fontsize=12)

# --- 短期 ---
ax3 = axes[2]
ax3.set_title('SHORT-TERM (3M) | MA5/MA10/MA20 = Small Pendulums',
              fontsize=13, fontweight='bold', color='darkgreen')
ax3.plot(df_short.index, df_short['Close'], color='black', linewidth=1.5, label='Price')
ax3.plot(df_short.index, df_short['MA5'], color='green', linewidth=1.2, label='MA5 (W)')
ax3.plot(df_short.index, df_short['MA10'], color='purple', linewidth=1.5, label='MA10')
ax3.plot(df_short.index, df_short['MA20'], color='blue', linewidth=2, label='MA20 (M)')
ma60_valid = df_short['MA60'].dropna()
if len(ma60_valid) > 0:
    ax3.plot(ma60_valid.index, ma60_valid, color='red', linewidth=2, label='MA60 (Q)')

ax3.annotate(f'High: {recent_high:.0f}', xy=(high_date, recent_high),
            fontsize=10, fontweight='bold', color='red',
            xytext=(0, 15), textcoords='offset points', ha='center')
ax3.annotate(f'Low: {recent_low:.0f}', xy=(low_date, recent_low),
            fontsize=10, fontweight='bold', color='darkgreen',
            xytext=(0, -20), textcoords='offset points', ha='center')

ax3.text(0.02, 0.95,
         f'Short trend: {trend_short}\n'
         f'From high: {drawdown:+.1f}% | From low: {recovery:+.1f}%\n'
         f'Price vs MA20: {((latest-ma20_s)/ma20_s*100):+.1f}%',
         transform=ax3.transAxes, fontsize=11, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='lavender', alpha=0.9))

ax3.legend(loc='lower right', fontsize=10)
ax3.grid(True, alpha=0.3)
ax3.set_ylabel('Price', fontsize=12)
ax3.set_xlabel('Date', fontsize=12)

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig('MrBrain-invest-principle/sp500_trend_analysis.png', dpi=150, bbox_inches='tight')
print("\nChart saved: MrBrain-invest-principle/sp500_trend_analysis.png")

# ========== 数据摘要 ==========
print("\n" + "="*60)
print("S&P 500 TREND DATA SUMMARY")
print("="*60)
print(f"\nCurrent Price: {latest:.2f}")
print(f"\n[LONG-TERM - Annual Line (MA250)]")
print(f"  MA250 = {ma250:.2f}, Direction: {'UP' if ma250_slope > 0 else 'DOWN'} (slope: {ma250_slope:+.2f}/day)")
print(f"  MA120 = {ma120:.2f}, Direction: {'UP' if ma120_slope > 0 else 'DOWN'}")
print(f"  Price deviation from MA250: {deviation_250:+.1f}%")
print(f"  5Y High: {df_long['Close'].max():.2f} | 5Y Low: {df_long['Close'].min():.2f}")

print(f"\n[MID-TERM - Quarterly Line (MA60)]")
print(f"  MA60 = {ma60_mid:.2f}, Direction: {'UP' if ma60_slope_mid > 0 else 'DOWN'} (slope: {ma60_slope_mid:+.2f}/day)")
print(f"  MA20 = {ma20_mid:.2f}, Direction: {'UP' if ma20_slope_mid > 0 else 'DOWN'}")
print(f"  Price deviation from MA60: {dev_60:+.1f}%")
print(f"  Price deviation from MA20: {dev_20:+.1f}%")

print(f"\n[SHORT-TERM - Weekly Line (MA5)]")
print(f"  MA5 = {ma5_s:.2f} | MA20 = {ma20_s:.2f}")
print(f"  3M High: {recent_high:.2f} ({high_date.strftime('%Y-%m-%d')})")
print(f"  3M Low: {recent_low:.2f} ({low_date.strftime('%Y-%m-%d')})")
print(f"  From high: {drawdown:+.1f}% | From low: {recovery:+.1f}%")
print(f"  Trend: {trend_short}")

print(f"\n[MA ALIGNMENT]")
if ma5_s > ma20_s > ma60_mid:
    print("  Bullish alignment: MA5 > MA20 > MA60")
elif ma5_s < ma20_s < ma60_mid:
    print("  Bearish alignment: MA5 < MA20 < MA60")
else:
    print(f"  Tangled: MA5={ma5_s:.0f}, MA20={ma20_s:.0f}, MA60={ma60_mid:.0f}")

if latest > ma250:
    print(f"  Price ABOVE annual line -> Bullish structure")
else:
    print(f"  Price BELOW annual line -> Bearish structure")
