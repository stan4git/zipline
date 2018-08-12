import numpy as np
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline import CustomFactor, Pipeline
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data import morningstar

"""
    Creating an algorithm based off the Piotroski Score index which is based off of a score (0-9)
    Each of the following points in Profitablity, Leverage & Operating Effificieny means one point. 
    We are going to select 

    Profitability 
    - Positive ROA
    - Positive Operating Cash Flow
    - Higher ROA in current year versus last year
    - Cash flow from operations > ROA of current year

    Leverage
    - Current ratio of long term debt < last year's ratio of long term debt
    - Current year's current_ratio > last year's current_ratio
    - No new shares issued this year

    Operating Efficiency
    - Higher gross margin compared to previous year
    - Higher asset turnover ratio compared to previous year

"""


class Piotroski(CustomFactor):
    inputs = [
        morningstar.operation_ratios.roa,
        morningstar.cash_flow_statement.operating_cash_flow,
        morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities,
        
        morningstar.operation_ratios.long_term_debt_equity_ratio,
        morningstar.operation_ratios.current_ratio,
        morningstar.valuation.shares_outstanding,
        
        morningstar.operation_ratios.gross_margin,
        morningstar.operation_ratios.assets_turnover,
    ]
    window_length = 100
    
    def compute(self, today, assets, out,
                roa, cash_flow, cash_flow_from_ops,
                long_term_debt_ratio, current_ratio, shares_outstanding,
                gross_margin, assets_turnover):
        profit = (
            (roa[-1] > 0).astype(int) +
            (cash_flow[-1] > 0).astype(int) +
            (roa[-1] > roa[0]).astype(int) +
            (cash_flow_from_ops[-1] > roa[-1]).astype(int)
        )
        
        leverage = (
            (long_term_debt_ratio[-1] < long_term_debt_ratio[0]).astype(int) +
            (current_ratio[-1] > current_ratio[0]).astype(int) + 
            (shares_outstanding[-1] <= shares_outstanding[0]).astype(int)
        )
        
        operating = (
            (gross_margin[-1] > gross_margin[0]).astype(int) +
            (assets_turnover[-1] > assets_turnover[0]).astype(int)
        )
        
        out[:] = profit + leverage + operating

class ROA(CustomFactor):
    window_length = 1
    inputs = [morningstar.operation_ratios.roa]
    
    def compute(self, today, assets, out, roa):
        out[:] = (roa[-1] > 0).astype(int)
        
class ROAChange(CustomFactor):
    window_length = 100
    inputs = [morningstar.operation_ratios.roa]
    
    def compute(self, today, assets, out, roa):
        out[:] = (roa[-1] > roa[0]).astype(int)
        
class CashFlow(CustomFactor):
    window_length = 1
    inputs = [morningstar.cash_flow_statement.operating_cash_flow]
    
    def compute(self, today, assets, out, cash_flow):
        out[:] = (cash_flow[-1] > 0).astype(int)
        
class CashFlowFromOps(CustomFactor):
    window_length = 1
    inputs = [morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities, morningstar.operation_ratios.roa]
    
    def compute(self, today, assets, out, cash_flow_from_ops, roa):
        out[:] = (cash_flow_from_ops[-1] > roa[-1]).astype(int)
        
class LongTermDebtRatioChange(CustomFactor):
    window_length = 100
    inputs = [morningstar.operation_ratios.long_term_debt_equity_ratio]
    
    def compute(self, today, assets, out, long_term_debt_ratio):
        out[:] = (long_term_debt_ratio[-1] < long_term_debt_ratio[0]).astype(int)
        
class CurrentDebtRatioChange(CustomFactor):
    window_length = 100
    inputs = [morningstar.operation_ratios.current_ratio]
    
    def compute(self, today, assets, out, current_ratio):
        out[:] = (current_ratio[-1] > current_ratio[0]).astype(int)
        
class SharesOutstandingChange(CustomFactor):
    window_length = 100
    inputs = [morningstar.valuation.shares_outstanding]
    
    def compute(self, today, assets, out, shares_outstanding):
        out[:] = (shares_outstanding[-1] <= shares_outstanding[0]).astype(int)
        
class GrossMarginChange(CustomFactor):
    window_length = 100
    inputs = [morningstar.operation_ratios.gross_margin]
    
    def compute(self, today, assets, out, gross_margin):
        out[:] = (gross_margin[-1] > gross_margin[0]).astype(int)
        
class AssetsTurnoverChange(CustomFactor):
    window_length = 100
    inputs = [morningstar.operation_ratios.assets_turnover]
    
    def compute(self, today, assets, out, assets_turnover):
        out[:] = (assets_turnover[-1] > assets_turnover[0]).astype(int) 
        
def initialize(context):

    pipe = Pipeline()
    pipe = attach_pipeline(pipe, name='piotroski')
    
    profit = ROA() + ROAChange() + CashFlow() + CashFlowFromOps()
    leverage = LongTermDebtRatioChange() + CurrentDebtRatioChange() + SharesOutstandingChange()
    operating = GrossMarginChange() + AssetsTurnoverChange()
    piotroski = profit + leverage + operating
    
    pipe.add(piotroski, 'piotroski')
    pipe.set_screen(piotroski >= 7)
    context.is_month_end = False
    schedule_function(set_month_end, date_rules.month_end(1)) 
    schedule_function(trade, date_rules.month_end(), time_rules.market_close())

def set_month_end(context, data):
    print "---- Set Month End -----"
    context.is_month_end = True
    
def before_trading_start(context, data):
    if context.is_month_end:
        context.results = pipeline_output('piotroski')
        context.long_stocks = context.results.sort_values('piotroski', ascending=False).head(10)
        context.total_piotroski = context.long_stocks.piotroski.sum()
        context.piotroski_weight = context.long_stocks.piotroski/context.long_stocks.piotroski.sum()
        update_universe(context.long_stocks.index)
   

def trade(context, data):
    valid_stocks = set(data.keys()).intersection(set(context.long_stocks.index))
    print data.keys()
    print "-------- New Set ----------"
    print valid_stocks
    
    for stock in valid_stocks:
        order_target_percent(stock, context.piotroski_weight[stock])
    
    for stock in context.portfolio.positions:
        if stock not in valid_stocks:
            order_target_percent(stock, 0)
    context.is_month_end = False

def track_orders(context, data):
    '''
    Show orders when made and filled.
    Info: https://www.quantopian.com/posts/track-orders
    '''
    c = context
    if 'trac' not in c:
        c.t_options = {           # __________    O P T I O N S    __________
            'log_neg_cash': 1,    # Show cash only when negative.
            'log_cash'    : 0,    # Show cash values in logging window or not.
            'log_ids'     : 1,    # Include order id's in logging window or not.
            'log_unfilled': 1,    # When orders are unfilled. (stop & limit excluded).
        }    # Move these to initialize() for better efficiency.
        c.trac = {}
        c.t_dates  = {  # To not overwhelm the log window, start/stop dates can be entered.
            'active': 0,
            'start' : [],   # Start dates, option like ['2007-05-07', '2010-04-26']
            'stop'  : []    # Stop  dates, option like ['2008-02-13', '2010-11-15']
        }
    from pytz import timezone     # Python only does once, makes this portable.
                                  #   Move to top of algo for better efficiency.
    # If 'start' or 'stop' lists have something in them, triggers ...
    if c.t_dates['start'] or c.t_dates['stop']:
        date = str(get_datetime().date())
        if   date in c.t_dates['start']:    # See if there's a match to start
            c.t_dates['active'] = 1
        elif date in c.t_dates['stop']:     #   ... or to stop
            c.t_dates['active'] = 0
    else: c.t_dates['active'] = 1           # Set to active b/c no conditions.
    if c.t_dates['active'] == 0: return     # Skip if not active.
    def _minute():   # To preface each line with the minute of the day.
        bar_dt = get_datetime().astimezone(timezone('US/Eastern'))
        return str((bar_dt.hour * 60) + bar_dt.minute - 570).rjust(3) # (-570 = 9:31a)
    def _trac(to_log):      # So all logging comes from the same line number,
        log.info(to_log)    #   for vertical alignment in the logging window.

    for oid in c.trac.copy():               # Existing known orders
      o = get_order(oid)
      if o.dt == o.created: continue        # No chance of fill yet.
      cash = ''
      prc  = data.current(o.sid, 'price') if data.can_trade(o.sid) else c.portfolio.positions[o.sid].last_sale_price
      if (c.t_options['log_neg_cash'] and c.portfolio.cash < 0) or c.t_options['log_cash']:
        cash = 'cash {}'.format(int(c.portfolio.cash))
      if o.status == 2:                      # Canceled
        do = 'Buy' if o.amount > 0 else 'Sell' ; style = ''
        if o.stop:
          style = ' stop {}'.format(o.stop)
          if o.limit: style = ' stop {} limit {}'.format(o.stop, o.limit)
        elif o.limit: style = ' limit {}'.format(o.limit)
        _trac(' {}     Canceled {} {} {}{} at {}   {}  {}'.format(_minute(), do, o.amount,
           o.sid.symbol, style, prc, cash, o.id[-4:] if c.t_options['log_ids'] else ''))
        del c.trac[o.id]
      elif o.filled:                             # Filled at least some.
        filled = '{}'.format(o.amount)
        filled_amt = 0
        if o.status == 1:            # Complete
          if 0 < c.trac[o.id] < o.amount:
            filled   = 'all {}/{}'.format(o.filled - c.trac[o.id], o.amount)
          filled_amt = o.filled
        else:                                    # c.trac[o.id] value is previously filled total
          filled_amt = o.filled - c.trac[o.id]   # filled this time, can be 0
          c.trac[o.id] = o.filled                # save fill value for increments math
          filled = '{}/{}'.format(filled_amt, o.amount)
        if filled_amt:
          now = ' ({})'.format(c.portfolio.positions[o.sid].amount) if c.portfolio.positions[o.sid].amount else ' _'
          pnl = ''  # for the trade only
          amt = c.portfolio.positions[o.sid].amount ; style = ''
          if (amt - o.filled) * o.filled < 0:    # Profit-taking scenario including short-buyback
            cb = c.portfolio.positions[o.sid].cost_basis
            if cb:
              pnl  = -filled_amt * (prc - cb)
              sign = '+' if pnl > 0 else '-'
              pnl  = '  ({}{})'.format(sign, '%.0f' % abs(pnl))
          if o.stop:
            style = ' stop {}'.format(o.stop)
            if o.limit: style = ' stop () limit {}'.format(o.stop, o.limit)
          elif o.limit: style = ' limit {}'.format(o.limit)
          if o.filled == o.amount: del c.trac[o.id]
          _trac(' {}      {} {} {}{} at {}{}{}'.format(_minute(),
            'Bot' if o.amount > 0 else 'Sold', filled, o.sid.symbol, now,
            '%.2f' % prc, pnl, style).ljust(52) + '  {}  {}'.format(cash, o.id[-4:] if c.t_options['log_ids'] else ''))
      elif c.t_options['log_unfilled'] and not (o.stop or o.limit):
        _trac(' {}         {} {}{} unfilled  {}'.format(_minute(), o.sid.symbol, o.amount,
         ' limit' if o.limit else '', o.id[-4:] if c.t_options['log_ids'] else ''))

    oo = get_open_orders().values()
    if not oo: return                       # Handle new orders
    cash = ''
    if (c.t_options['log_neg_cash'] and c.portfolio.cash < 0) or c.t_options['log_cash']:
      cash = 'cash {}'.format(int(c.portfolio.cash))
    for oo_list in oo:
      for o in oo_list:
        if o.id in c.trac: continue         # Only new orders beyond this point
        prc = data.current(o.sid, 'price') if data.can_trade(o.sid) else c.portfolio.positions[o.sid].last_sale_price
        c.trac[o.id] = 0 ; style = ''
        now  = ' ({})'.format(c.portfolio.positions[o.sid].amount) if c.portfolio.positions[o.sid].amount else ' _'
        if o.stop:
          style = ' stop {}'.format(o.stop)
          if o.limit: style = ' stop {} limit {}'.format(o.stop, o.limit)
        elif o.limit: style = ' limit {}'.format(o.limit)
        _trac(' {}   {} {} {}{} at {}{}'.format(_minute(), 'Buy' if o.amount > 0 else 'Sell',
          o.amount, o.sid.symbol, now, '%.2f' % prc, style).ljust(52) + '  {}  {}'.format(cash, o.id[-4:] if c.t_options['log_ids'] else ''))


            
# Will be called on every trade event for the securities you specify. 
def handle_data(context, data):
    track_orders(context, data)