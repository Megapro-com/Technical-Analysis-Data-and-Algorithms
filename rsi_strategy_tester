#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pandas as pd
from datetime import datetime

# https://www.tradingview.com/support/solutions/43000502338-relative-strength-index-rsi/


#pd.set_option('display.max_rows', None)
pd.set_option('display.width', 150)
symbol = 'AAPL'
start_date = '2011-01-01'
end_date = '2020-12-31'
risk_free_rate = 1.5 # 10 Year Treasury Rate Percentage
RSI_High = 70
RSI_Low = 30


# import kline data
def get_kline(symbol, start='', stop=''):
    kline = pd.read_csv('%s.csv' % symbol)
    if not kline.empty:
        kline.set_index('date', inplace=True, drop=True)
    if start:
        kline = kline[kline.index >= start]
    if stop:
        kline = kline[kline.index <= stop]
    return kline

kline = get_kline(symbol, start_date, end_date)
print(kline)


# calculate RSI
def rsi(kline, period=14):
    delta = kline['Adj Close'].diff()
    dUp, dDown = delta.copy(), delta.copy()
    dUp[dUp < 0] = 0
    dDown[dDown > 0] = 0
    RolUp = dUp.iloc[:].ewm(span=2*period-1,adjust=False).mean()
    RolDown = dDown.iloc[:].ewm(span=2*period-1,adjust=False).mean().abs()
    RS = RolUp / RolDown
    rsi= 100.0 - (100.0 / (1.0 + RS))
    return rsi

kline['rsi'] = rsi(kline, 14)
print(kline)


long_trades = pd.DataFrame()
initial_balance = 100000
cash = initial_balance
long_holding = 0
last_long_price = 0
for i in range(len(kline)):
    index = kline.index[i]
    line = kline.iloc[i]
    pre_rsi = kline.iloc[i-1]['rsi'] if i > 1 else kline.iloc[i]['rsi']
    record = dict()
    tmp_cash = cash
    record['date'] = index
    record['price'] = line['Adj Close']
    # Buy Long when RSI <= RSI_Low and current RSI is larger than previous RSI
    if (long_holding <= 0) and (line['rsi'] <= RSI_Low) and (line['rsi'] > pre_rsi):
        record['quantity'] = int(cash/line['Adj Close'])
        record['side'] = 'buy'
        if record['quantity'] >= 1:
            long_holding += record['quantity']
            tmp_cash -= record['quantity'] * line['Adj Close']
            last_long_price = line['Adj Close']
            long_trades = long_trades.append(record, ignore_index=True)
    # Sell Long when RSI >= RSI_High and current RSI is less than previous RSI
    if (long_holding > 0) and (line['rsi'] >= RSI_High) and (line['rsi'] < pre_rsi):
        tmp_cash += line['Adj Close'] * long_holding
        record['quantity'] = long_holding
        record['side'] = 'sell'
        long_holding = 0
        long_trades = long_trades.append(record, ignore_index=True)
    cash = tmp_cash

if not long_trades.empty:
    long_trades.set_index('date', inplace=True, drop=True)
    if long_trades['side'].iat[-1] == 'buy':
        long_trades.drop(long_trades.tail(1).index, inplace=True)


def timediff2str(timediff):
    timediff = str(timediff).replace(' days, ', ':').replace(' day, ', ':').replace(' days ', ':')
    timediff_list = [float(i) for i in timediff.split(':')]
    return '%.0fD%.0fH%.0fM' % tuple(timediff_list[:3])


def aggregate_trades(df, position):
    trades_df = pd.DataFrame()
    holded = False
    record = dict()
    for i in range(len(df)):
        line = df.iloc[i]
        index = datetime.strptime(df.index[i], '%Y-%m-%d')
        if not holded and ((line['side'] == 'buy' and position == 'long') or                (line['side'] == 'sell' and position == 'short')):
            holded = True
            record['OpenTime'] = index
            record['OpenPrice'] = round(line['price'], 2)
            record['OpenAmt'] = round(line['price'] * line['quantity'], 2)
            record['Qty'] = round(line['quantity'], 0)
        if holded and ((line['side'] == 'sell' and position == 'long') or                (line['side'] == 'buy' and position == 'short')):
            holded = False
            record['CloseTime'] = index
            record['ClosePrice'] = round(line['price'], 2)
            record['CloseAmt'] = round(line['price'] * record['Qty'], 2)
            record['TimeDiffValue'] = record['CloseTime'] - record['OpenTime']
            if position == 'long':
                record['Gain/Loss'] = round(record['CloseAmt'] - record['OpenAmt'], 2)
                record['GLRValue'] = (record['ClosePrice']/record['OpenPrice'] - 1) * 100
            else:
                record['Gain/Loss'] = round(record['OpenAmt'] - record['CloseAmt'], 2)
                record['GLRValue'] = (record['OpenPrice']/record['ClosePrice'] - 1) * 100
            record['G/L Rate'] = '%.2f%%' % record['GLRValue']
            record['TimeDiff'] = timediff2str(record['TimeDiffValue'])
            record['OpenTime'] = record['OpenTime'].strftime('%Y%m%dT%H:%M')
            record['CloseTime'] = record['CloseTime'].strftime('%Y%m%dT%H:%M')
            record['Position'] = position
            trades_df = trades_df.append(record, ignore_index=True)
            record = dict()
    return(trades_df[['OpenTime', 'OpenPrice', 'Qty', 'OpenAmt', 'CloseTime', 'ClosePrice', 'CloseAmt',
                'Gain/Loss', 'G/L Rate', 'TimeDiff', 'GLRValue', 'TimeDiffValue', 'Position']])


all_trades = aggregate_trades(long_trades, 'long').sort_values(by=['OpenTime'], inplace=False)
all_trades.reset_index(drop=True, inplace=True)
df_to_print = all_trades[['OpenTime', 'OpenPrice', 'Qty', 'OpenAmt', 'CloseTime',
        'ClosePrice', 'CloseAmt', 'Gain/Loss', 'G/L Rate', 'TimeDiff']]
df_to_print.columns = ['OpenTime', 'OpenP', 'Qty', 'OpenAmt', 'CloseTime', 'CloseP', 'CloseAmt', 'G/L', 'GLRate', 'TimeDiff']
#print(df_to_print)
#print()
gl_rate = (all_trades['Gain/Loss'].sum() / initial_balance) * 100
sharpe_ratio = (gl_rate - risk_free_rate) / (all_trades['GLRValue'].std())
summary = ['Total Number of Trades: %.0f' % len(all_trades),
           'G/L Count: %.0f/%.0f (%.2f%%)' % (all_trades['Gain/Loss'].gt(0).sum(),
            all_trades['Gain/Loss'].lt(0).sum(), all_trades['Gain/Loss'].gt(0).mean()*100),
        'Initial Balance: %.2f' % initial_balance,
        'Gain/Loss: $%.2f' % all_trades['Gain/Loss'].sum(), 
        'G/L Rate: %.2f%%' % gl_rate,
        'Max Gain: %.2f' % all_trades['Gain/Loss'].max(),
        'Max Loss: %.2f' % all_trades['Gain/Loss'].min(), 
        'Average Gain/Loss: $%.2f' % all_trades['Gain/Loss'].mean(),
        'Average Gain Rate (Winning Trades): %.2f%%' % all_trades[all_trades['GLRValue'] > 0]['GLRValue'].mean(),
        'Average Loss Rate (Losing Trades): %.2f%%' % all_trades[all_trades['GLRValue'] < 0]['GLRValue'].mean(),
        'Average G/L Rate: %.2f%%' % all_trades['GLRValue'].mean(), 
        'Average Time Range: ' + timediff2str(all_trades['TimeDiffValue'].mean()),
        'Median Gain/Loss: $%.2f' % all_trades['Gain/Loss'].median(), 
        'Median G/L Rate: %.2f%%' % all_trades['GLRValue'].median(),
        'Median Time Range: ' + timediff2str(all_trades['TimeDiffValue'].median()),
        'Sharpe Ratio: %.2f%%' % sharpe_ratio]
print('\n'.join(summary))
df_summary = pd.DataFrame(summary, columns=['Summary'])

# Save results as .xslx file
filename = 'RSIStrategyResult%s.xlsx' % symbol
with pd.ExcelWriter(filename) as writer:  
    df_to_print.to_excel(writer, sheet_name='Trades')
    df_summary.to_excel(writer, sheet_name='Summary')

filename2 = 'RSIStrategyResult%s.html' % symbol
with open(filename2, 'w') as fp:
    fp.write(df_to_print.to_html() + "\n\n" + df_summary.to_html())


# send trading signals to discord channel
from webcord import Webhook

WEBHOOK = 'https://discord.com/api/webhooks/webhookExample123456'
messages = ''
discord_client = Webhook(WEBHOOK)
for i in range(len(long_trades)):
    dt = long_trades.index[i]
    line = long_trades.iloc[i]
    message = '%s %s %.0f @ %.2f %s' % (line['side'], symbol, line['quantity'], line['price'], dt)
    #discord_client.send_message(message, 'RSI Strategy Tester')
    messages += message + '\n'
print(messages)
