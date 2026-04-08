import os,time,json,json5
import dolphindb as ddb
import pandas as pd
from src.entity.Table import Statistics, OrderDetails, TradeDetails
from src.entity.Simulator import Simulator
from src.entity.Result import Result
from src.entity.Eva import Eva
pd.set_option("display.max_columns", None)

if __name__ == "__main__":
    session = ddb.session("localhost", 8848, "admin", "123456")
    with open(r".\src\cons\format.json5", "r", encoding="utf-8") as f:
        cfg = json5.load(f)
    statistics = pd.read_excel(r"E:\Quant\PyBackTest\test\classVoteStrategy\Statistics(Future).xlsx",index_col=None,header=0)
    orderDetails = pd.read_excel(r"E:\Quant\PyBackTest\test\classVoteStrategy\OrderDetails(Future).xlsx",index_col=None,header=0)
    tradeDetails = pd.read_excel(r"E:\Quant\PyBackTest\test\classVoteStrategy\TradeDetails(Future).xlsx",index_col=None,header=0)
    tradeDetails["symbol"] = tradeDetails["symbol"].apply(lambda x:str(x).split(".")[0].replace(str(x).split(".")[0][-4:], ""))
    t0 = time.time()
    EvaObj = Eva(session=session, statsCfg=cfg["statistics"], orderCfg=cfg["orderDetails"], tradeCfg=cfg["tradeDetails"],
                 statistics=statistics, tradeDetails=tradeDetails, orderDetails=orderDetails)
    EvaObj.upload()
    EvaObj.restore()
    EvaObj.indicatorStats()
    EvaObj.summaryPnlStats()
    EvaObj.pnlStatsBySymbol(symbol="AO")
    EvaObj.pnlStatsByPeriod(startDate="20150101", endDate="20270101")
    EvaObj.summaryTradeStats()
    EvaObj.tradeStatsByPeriod(startDate="20150101", endDate="20270101")
    t1 = time.time()
    print("StrategyEva总耗时(s)", t1-t0)
