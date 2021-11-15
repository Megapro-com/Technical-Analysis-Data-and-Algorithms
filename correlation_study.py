#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from MongoDB.client import SyncDB


# ### Set Configs


# Please change the parameters here
# For corr_method you can set the following:
# 'pearson' : standard correlation coefficient
# 'kendall' : Kendall Tau correlation coefficient
# 'spearman' : Spearman rank correlation
# 'all': All of 3 methods

startDate = datetime(2020, 1, 1)
endDate = datetime(2021, 1, 1)
collection_1 = 'stock_daily'
column_1 = 'Close'
label_1 = 'SPY'
match_1 = {'symbol': 'SPY'}
collection_2 = 'TA_Daily'
column_2 = 'RSI'
label_2 = 'RSI'
match_2 = {'symbol': 'SPY'}

corr_method = 'pearson'

SmoothPeriod = 5


# ### Prepare data from collection_1


dateRange = (endDate - startDate).days

query_1 = {**match_1, **{'date': {'$gte': startDate, '$lte': endDate}}}

data_list_1 = list(SyncDB.find(collection_1, query_1))
if not data_list_1:
    query_1 = {**match_1, **{'date': {'$gte': startDate.strftime('%Y-%m-%d'), '$lte': endDate.strftime('%Y-%m-%d')}}}
    data_list_1 = list(SyncDB.find(collection_1, query_1))
if not data_list_1:
    query_1 = {**match_1, **{'date': {'$gte': startDate, '$lte': endDate}}}
    data_list_1 = list(SyncDB.find(collection_1, query_1))
if not data_list_1:
    query_1 = {**match_1, **{'Date': {'$gte': startDate.strftime('%Y-%m-%d'), '$lte': endDate.strftime('%Y-%m-%d')}}}
    data_list_1 = list(SyncDB.find(collection_1, query_1))
if not data_list_1:
    query_1 = {**match_1, **{'TradeTime': {'$gte': startDate, '$lte': endDate}}}
    pipeline = [{'$match': query_1},
               {'$group' : {
                   '_id': {
                       "year": {"$year": "$TradeTime" },
                       "month": {"$month": "$TradeTime"}, 
                       "day": {"$dayOfMonth": "$TradeTime"}
                   },
                   ('total%s' % column_1): { '$sum': '$%s' % column_1}                   
                }},
               ]
    data_list_1 = list(SyncDB.aggregate(collection_1, pipeline))
    new_data_list = list()
    for record in data_list_1:
        record['date'] = datetime(record['_id']['year'], record['_id']['month'], record['_id']['day'])
        record[column_1] = record['total%s' % column_1]
        new_data_list.append({'date': record['date'], column_1: record[column_1]})
    data_list_1 = new_data_list

index_1 = 'date' if 'date' in data_list_1[0] else 'Date'
cols = [index_1, column_1]
df = pd.DataFrame(data_list_1)[cols]
if type(df[index_1]) != pd.core.indexes.datetimes.DatetimeIndex:
    df[index_1] = pd.to_datetime(df[index_1], infer_datetime_format=True)
df.set_index(index_1, drop=True, inplace=True)
df.sort_values(index_1, inplace=True)


# ### Prepare data from collection_2


def add_collection_to_df(collection, query, column, label, df):
    query['date'] = {'$gte': startDate, '$lte': endDate}
    data_list = list(SyncDB.find(collection, query))
    if not data_list:
        query['date'] = {'$gte': startDate.strftime('%Y-%m-%d'), '$lte': endDate.strftime('%Y-%m-%d')}
        data_list = list(SyncDB.find(collection, query))
        if not data_list:
            query['Date'] = {'$gte': startDate, '$lte': endDate}
            data_list = list(SyncDB.find(collection, query))
            if not data_list:
                query['Date'] = {'$gte': startDate.strftime('%Y-%m-%d'), '$lte': endDate.strftime('%Y-%m-%d')}
                data_list = list(SyncDB.find(collection, query))
    index = 'date' if 'date' in data_list[0] else 'Date'
    cols = [index, column]
    df_n = pd.DataFrame(data_list)[cols]
    if type(df_n[index]) != pd.core.indexes.datetimes.DatetimeIndex:
        df_n[index] = pd.to_datetime(df_n[index], infer_datetime_format=True)
    df_n.set_index(index, drop=True, inplace=True)
    df_n.sort_values(index, inplace=True)
    if column in list(df.columns) or ((column+'SMA') in list(df.columns)):
        col_new = label
    else:
        col_new = column
    df_n.columns = [col_new]
    tmp_col_s = df_n[col_new].rolling(SmoothPeriod).mean().to_frame(name=col_new+'SMA')
    df = df.join(df_n[col_new], how='left')
    df = df.join(tmp_col_s, how='left')
    return col_new, df
    
column_2, df = add_collection_to_df(collection_2, match_2, column_2, label_2, df)
df_to_plot = df.dropna(how='any')


# Calculate correlation value
corr_values = [(df_to_plot[[column_1, column_2]].dropna(how='any')).corr(method=corr_method).iat[0,1],
            (df_to_plot[[column_1, column_2+'SMA']].dropna(how='any')).corr(method=corr_method).iat[0,1]]

### Plot results
# create a fig of width=10 and length=200
fig, ax = plt.subplots(2, 1, figsize=(26, 15), sharex='col')
fig.subplots_adjust(hspace=0)

# set the label of x axis and y axis

ax[0].set_ylabel(column_1)
ax[0].text(0.02, 0.78, 'megapro.com', horizontalalignment='left', color='gray', alpha=0.4,
        verticalalignment='center', rotation=0, fontsize=25, transform=ax[0].transAxes, zorder=0)

ax[0].text(0.02, 0.62, 'Join Discord: mCmMjSRuBn', horizontalalignment='left', color='gray', alpha=0.4,
        verticalalignment='center', rotation=0, fontsize=25, transform=ax[0].transAxes, zorder=0)

if not label_1:
    label_1 = column_1

ax[0].plot(df_to_plot.index, df_to_plot[column_1], color='blue', label=label_1)
ax[0].text(0.08, 0.9, '%s: %.4f' % (label_1, df_to_plot[column_1].iat[-1]), 
        horizontalalignment='left', color='blue', verticalalignment='center', fontsize=10, transform=ax[0].transAxes)

        
def plot_data_panel(inx, label, col, corr_value):
    label_1 = '[DATA]' + label if label else '[DATA]' + col
    ax[inx].set_ylabel(label_1[6:])
    ax[inx].plot(df_to_plot.index, df_to_plot[col], color='orange', label=label_1)
    ax[inx].text(0.2, 0.9, '%s: %.4f' % (label_1, df_to_plot[col].iat[-1]), 
            horizontalalignment='left', color='black', verticalalignment='center', fontsize=10, transform=ax[inx].transAxes)
    ax[inx].text(0.9, 0.9, 'Correlation: %.4f' % corr_values[0], 
            horizontalalignment='right', color='orange', verticalalignment='center', fontsize=15, transform=ax[inx].transAxes)           
    if SmoothPeriod > 1:
        label_2 = label_1 + ' SMA %d' % SmoothPeriod
        ax[inx].plot(df_to_plot.index, df_to_plot[col+'SMA'], color='magenta', label=label_2)
        ax[inx].text(0.4, 0.9, '%s: %.4f' % (label_2, df_to_plot[col+'SMA'].iat[-1]), 
                horizontalalignment='left', color='black', verticalalignment='center', fontsize=10, transform=ax[inx].transAxes)
        ax[inx].text(0.9, 0.8, '%.4f' % corr_values[1], 
                horizontalalignment='right', color='magenta', verticalalignment='center', fontsize=15, transform=ax[inx].transAxes)       
    return
      
inx = 1
plot_data_panel(inx, label_2, column_2, corr_value)

# set the legend at upper left corner
#ax[0].legend(loc=[0.002, 0.88],prop={'size': 18})
#ax[1].legend(loc=[0.002, 0.94],prop={'size': 18})
# set date xaxis format
for axi in ax:
    axi.grid(axis="x", color='grey',linestyle=':',linewidth=0.75)
    axi.set_xmargin(0.005)
    axi.legend(loc=[0.002, 0.85],prop={'size': 10})
    axi.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    axi.xaxis.set_major_locator(mdates.DayLocator(interval=int(dateRange/80)))

fig.autofmt_xdate(rotation=90)
# set title

pic_title = 'Megapro Chart %s Correlation Study\n%s-%s' % (label_2, startDate.strftime('%Y%m%d'), endDate.strftime('%Y%m%d'))
fig.suptitle(pic_title, fontsize=30, y=0.98)
#fig.tight_layout()
fig.subplots_adjust(top=0.88)

# save plot to file
filename = ('%s_Corr_%s-%s.png' % (label_2, startDate.strftime('%y%m'), endDate.strftime('%y%m'))).replace(' ', '')

plt.savefig(filename)
