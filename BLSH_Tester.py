#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from MongoDB.client import SyncDB
import numpy as np

#pd.set_option('display.max_rows', None)
pd.set_option('display.width', 150)
stock_collection = 'stock_daily'
stock_column = 'Close'
stock_match = {'symbol': 'SPY'}
stock_label = 'SPY'
start_date = '2020-01-01'
end_date = '2022-01-01'
risk_free_rate = 1.5 # 10 Year Treasury Rate Percentage
Sell_High = 70
Buy_Low = 30
data_collection = 'TA_Daily'
data_column = 'RSI'
data_label = 'RSI'
data_match = {'symbol': 'SPY'}

# import kline data
def get_kline(collection, match, start='', stop=''):
    date_query = {}
    if start:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        date_query['$gte'] = start_date
    if stop:
        end_date = datetime.strptime(stop, '%Y-%m-%d')
        date_query['$lte'] = end_date
    query = match
    query['date'] = date_query
    kline = pd.DataFrame(list(SyncDB.find(collection, query))).drop(['_id', 'UpdateTime'], 
                                                                         axis=1).sort_values(['date'])
    if not kline.empty:
        kline.set_index('date', inplace=True, drop=True)
    if start:
        kline = kline[kline.index >= start]
    if stop:
        kline = kline[kline.index <= stop]
    return kline

modified_start = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
kline = get_kline(stock_collection, stock_match, modified_start, end_date)
print(kline)


# get indicator from database
def get_indicator(collection, column, match, start='', stop=''):
    date_query = {}
    if start:
        start_date = datetime.strptime(start, '%Y-%m-%d') - timedelta(days=30)
        date_query['$gte'] = start_date
    if stop:
        end_date = datetime.strptime(stop, '%Y-%m-%d')
        date_query['$lte'] = end_date
    query = match
    query['date'] = date_query
    data_df = pd.DataFrame(list(SyncDB.find(collection, query)))[['date', column]].sort_values(['date'])
    if not data_df.empty:
        data_df.set_index('date', inplace=True, drop=True)
    return data_df

kline['data'] = get_indicator(data_collection, data_column, data_match,
                                    start_date, end_date)[data_column]

kline = kline[(kline.index >= start_date)&(kline.index <= end_date)]
print(kline)


long_trades = pd.DataFrame()
initial_balance = 100000
cash = initial_balance
long_holding = 0
last_long_price = 0
for i in range(len(kline)):
    index = kline.index[i]
    line = kline.iloc[i]
    record = dict()
    tmp_cash = cash
    record['date'] = index
    record['price'] = line[stock_column]
    # Buy Long when data <= Buy_Low
    if (long_holding <= 0) and (line['data'] <= Buy_Low):
        record['quantity'] = int(cash/line[stock_column])
        record['side'] = 'buy'
        if record['quantity'] >= 1:
            long_holding += record['quantity']
            tmp_cash -= record['quantity'] * line[stock_column]
            last_long_price = line[stock_column]
            long_trades = long_trades.append(record, ignore_index=True)
    # Sell Long when data >= Sell_High
    if (long_holding > 0) and (line['data'] >= Sell_High):
        tmp_cash += line[stock_column] * long_holding
        record['quantity'] = long_holding
        record['side'] = 'sell'
        long_holding = 0
        long_trades = long_trades.append(record, ignore_index=True)
    cash = tmp_cash

if long_holding > 0:
    line = kline.iloc[-1]
    index = kline.index[-1]
    record = dict()
    record['date'] = index
    record['price'] = line[stock_column]
    cash += line[stock_column] * long_holding
    record['quantity'] = long_holding
    record['side'] = 'sell'
    long_holding = 0
    long_trades = long_trades.append(record, ignore_index=True)

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
        index = df.index[i]
        if not holded and ((line['side'] == 'buy' and position == 'long') or (line['side'] == 'sell' and position == 'short')):
            holded = True
            record['OpenTime'] = index
            record['OpenPrice'] = round(line['price'], 2)
            record['OpenAmt'] = round(line['price'] * line['quantity'], 2)
            record['Qty'] = round(line['quantity'], 0)
        if holded and ((line['side'] == 'sell' and position == 'long') or (line['side'] == 'buy' and position == 'short')):
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
    if trades_df.empty:
        return None
    return(trades_df[['OpenTime', 'OpenPrice', 'Qty', 'OpenAmt', 'CloseTime', 'ClosePrice', 'CloseAmt',
                'Gain/Loss', 'G/L Rate', 'TimeDiff', 'GLRValue', 'TimeDiffValue', 'Position']])

all_trades = aggregate_trades(long_trades, 'long')
if all_trades is not None:
    all_trades.sort_values(by=['OpenTime'], inplace=True)
    all_trades.reset_index(drop=True, inplace=True)

if all_trades is None:
    print('No Trades Found')
else:
    df_to_print = all_trades[['OpenTime', 'OpenPrice', 'Qty', 'OpenAmt', 'CloseTime',
            'ClosePrice', 'CloseAmt', 'Gain/Loss', 'G/L Rate', 'TimeDiff']]
    df_to_print.columns = ['OpenTime', 'OpenP', 'Qty', 'OpenAmt', 'CloseTime', 'CloseP', 'CloseAmt', 'G/L', 'GLRate', 'TimeDiff']
    #print(df_to_print)
    #print()
    gl_rate = (all_trades['Gain/Loss'].sum() / initial_balance) * 100

    tradeSD = all_trades['GLRValue'].std()
    if tradeSD > 0:
        sharpe_ratio = (gl_rate - risk_free_rate) / tradeSD
    else:
        sharpe_ratio = np.inf
    avgGainRate = all_trades[all_trades['GLRValue'] > 0]['GLRValue'].mean()
    avgGainRate = avgGainRate if avgGainRate > 0 else 0
    avgLossRate = all_trades[all_trades['GLRValue'] < 0]['GLRValue'].mean()
    avgLossRate = avgLossRate if avgLossRate < 0 else 0
    
    summary = ['Total Number of Trades: %.0f' % len(all_trades),
               'G/L Count: %.0f/%.0f (%.2f%%)' % (all_trades['Gain/Loss'].gt(0).sum(),
                all_trades['Gain/Loss'].lt(0).sum(), all_trades['Gain/Loss'].gt(0).mean()*100),
            'Initial Balance: %.2f' % initial_balance,
            'Gain/Loss: $%.2f' % all_trades['Gain/Loss'].sum(), 
            'G/L Rate: %.2f%%' % gl_rate,
            'Max Gain: %.2f' % all_trades['Gain/Loss'].max(),
            'Max Loss: %.2f' % all_trades['Gain/Loss'].min(), 
            'Average Gain/Loss: $%.2f' % all_trades['Gain/Loss'].mean(),
            'Average Gain Rate (Winning Trades): %.2f%%' % avgGainRate,
            'Average Loss Rate (Losing Trades): %.2f%%' % avgLossRate,
            'Average G/L Rate: %.2f%%' % all_trades['GLRValue'].mean(), 
            'Average Time Range: ' + timediff2str(all_trades['TimeDiffValue'].mean()),
            'Median Gain/Loss: $%.2f' % all_trades['Gain/Loss'].median(), 
            'Median G/L Rate: %.2f%%' % all_trades['GLRValue'].median(),
            'Median Time Range: ' + timediff2str(all_trades['TimeDiffValue'].median()),
            'Sharpe Ratio: %.2f%%' % sharpe_ratio]
    print('\n'.join(summary))
    df_summary = pd.DataFrame(summary, columns=['Summary'])

    # Save results as .xslx file
    filename = 'BLSHStrategyResult%s.xlsx' % list(stock_match.values())[0]
    with pd.ExcelWriter(filename) as writer:  
        df_to_print.to_excel(writer, sheet_name='Trades')
        df_summary.to_excel(writer, sheet_name='Summary')

    filename2 = 'BLSHStrategyResult%s.html' % list(stock_match.values())[0]
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
    message = '%s %s %.0f @ %.2f %s' % (line['side'], list(stock_match.values())[0], line['quantity'], line['price'], dt)
    #discord_client.send_message(message, 'BLSH Strategy Tester')
    messages += message + '\n'
print(messages)


