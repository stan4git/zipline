import numpy as np
import pandas as pd
from algorithm.TradingAlgorithm import attach_pipeline, pipeline_output
from pipeline import Pipeline
from pipeline.data import Fundamentals
from pipeline.data.builtin import USEquityPricing
from pipeline.factors import Returns, AverageDollarVolume
from pipeline.filters import QTradableStocksUS
from pipeline.factors.eventvestor import BusinessDaysUntilNextEarnings, BusinessDaysSincePreviousEarnings
import optimize as opt
from pipeline.data.zacks import EarningsSurprises

    
def initialize(context):
    context.MAX_LEV = 0.5
    context.MAX_IN_ONE = .1
    
    context.longs = [[]] * 5
    context.shorts = [[]] * 5
    
    attach_pipeline(pre_earnings_pipeline(context), 'pre_pipeline')
    attach_pipeline(post_earnings_pipeline(context), 'post_pipeline')
    
    # < No Slippage Assumed >
    set_slippage(slippage.FixedBasisPointsSlippage(0))
    schedule_function(rebalance, date_rules.every_day(), time_rules.market_open())
    schedule_function(record_vars, date_rules.every_day(), time_rules.market_close())
    
    
def pre_earnings_pipeline(context):
    universe = QTradableStocksUS()
    point_in_time = BusinessDaysUntilNextEarnings().eq(1)
    
    factor = (EarningsSurprises.eps_pct_diff_surp.latest.rank() + Returns(window_length=5).rank())
    filter_factor = EarningsSurprises.eps_std_dev_est.latest
    
    universe = universe & factor.notnan() & filter_factor.notnan()
    universe = filter_factor.rank(mask=universe).percentile_between(70, 100)
    
    universe = universe & point_in_time
    
    longs = factor.zscore(mask=universe).percentile_between(0, 25)
    shorts = factor.zscore(mask=universe).percentile_between(75, 100)

    return Pipeline(
        columns={
            'longs': longs,
            'shorts': shorts,
        },

        screen=universe
    )


def post_earnings_pipeline(context):
    universe = QTradableStocksUS()
    point_in_time = BusinessDaysSincePreviousEarnings().eq(1)
        
    factor = Fundamentals.pe_ratio.latest
    filter_factor = (EarningsSurprises.eps_amt_diff_surp.latest**2)**0.5
    
    universe = universe & factor.notnan() & filter_factor.notnan()
    universe = filter_factor.rank(mask=universe).percentile_between(0, 30)
    
    universe = universe & point_in_time
    
    longs = factor.zscore(mask=universe).percentile_between(0, 25)
    shorts = factor.zscore(mask=universe).percentile_between(75, 100)
    
    return Pipeline(
        columns={
            'longs': longs,
            'shorts': shorts,
        },
        screen=universe
)


def before_trading_start(context, data):
    context.pre_pipeline = pipeline_output('pre_pipeline')
    context.post_pipeline = pipeline_output('post_pipeline')

    def update_record(record, new_item, days_to_hold):
        record.insert(0, new_item)
        while len(record) > days_to_hold and len(record[-1]) == 0:
            del(record[-1])
        if sum(map(lambda l: 0 if len(l) == 0 else 1, record)) > days_to_hold:
            del(record[-1])
    
    update_record(context.longs, context.pre_pipeline.index[
        (context.pre_pipeline['longs'] == True)
    ], 2)
    update_record(context.shorts, context.pre_pipeline.index[
        (context.pre_pipeline['shorts'] == True)
    ], 2)
    update_record(context.longs, context.post_pipeline.index[
        (context.post_pipeline['longs'] == True)
    ], 5)
    update_record(context.shorts, context.post_pipeline.index[
        (context.post_pipeline['shorts'] == True)
    ], 5)
        
        
def rebalance(context, data):
    long_list = [equity for sublist in context.longs for equity in sublist]
    short_list = [equity for sublist in context.shorts for equity in sublist]
    
    for equity in long_list:
        if data.can_trade(equity):
            order_target_percent(equity, min(context.MAX_LEV / len(long_list), context.MAX_IN_ONE))
    for equity in short_list:
        if data.can_trade(equity):
            order_target_percent(equity, -min(context.MAX_LEV / len(short_list), context.MAX_IN_ONE))
    
    for position in context.portfolio.positions:
        if position not in long_list and position not in short_list:
            order_target_percent(position, 0)
            
            
def record_vars(context, data):
    record(leverage=context.account.leverage, pos=len(context.portfolio.positions))