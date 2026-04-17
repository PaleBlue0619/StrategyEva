import pandas as pd
import dolphindb as ddb
from src.entity.Result import Result
from typing import List, Dict

class Plot(Result):
    def __init__(self, session: ddb.session, statistics: pd.DataFrame,
                 tradeDetails: pd.DataFrame, orderDetails: pd.DataFrame,
                 statsCfg: Dict[str, any], orderCfg: Dict[str, any], tradeCfg: Dict[str, any]):
        Result.__init__(self, session, statistics, tradeDetails, orderDetails, statsCfg, orderCfg, tradeCfg)