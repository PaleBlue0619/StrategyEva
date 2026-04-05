import os,json,json5
import dolphindb as ddb
import pandas as pd
from src.entity.Table import Statistics, OrderDetails, TradeDetails
from src.entity.Simulator import Simulator
pd.set_option("display.max_columns", None)

if __name__ == "__main__":
    with open(r".\src\cons\format.json5", "r", encoding="utf-8") as f:
        cfg = json5.load(f)
    tradeDetails = pd.read_excel(r"E:\Quant\PyBackTest\test\classVoteStrategy\TradeDetails(Future).xlsx",index_col=None,header=0)
    tradeDetails.columns = ["tradeNum","tradeTime","state","direction","symbol","price","vol","reason"]
    session = ddb.session("localhost", 8848, "admin", "123456")
    TradeTable = TradeDetails(session, tradeDetails)
    TradeTable.fromDict(cfg=cfg["tradeDetails"])
    TradeTable.upload()
    S = Simulator(session)
    S.getData()
    S.restore()
    # print(S.tradeDetails)