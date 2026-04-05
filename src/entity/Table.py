import dolphindb as ddb
import pandas as pd
from typing import Dict, List

class Table:
    def __init__(self, session: ddb.session, tableName: str, data: pd.DataFrame):
        self.tableName: str = tableName
        self.data: pd.DataFrame = data
        self.session: ddb.session = session

    def upload(self):
        self.session.upload({self.tableName: self.data})

class Statistics(Table):
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        super().__init__(session, data)
        self.tableName: str = "statistics"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    @override
    def upload(self):   # 上传 + 改名
        colDict: Dict[str, str] = {self.cfg[i]: str(i).replace("Col") for i in self.cfg.keys() if "Col" in i}
        self.session.upload({self.tableName: self.data})
        self.session.upload({"colNames": list(colDict.keys())})
        self.session.upload({"targetNames": list(colDict.values())})
        self.session.run(f"""
            {self.tableName} = <select _$$colNames as _$$targetNames from {self.tableName}>.eval();
        """)

class OrderDetails(Table):
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        super().__init__(session, data)
        self.tableName: str = "orderDetails"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    @override
    def upload(self):   # 规范状态名称 +上传 + 改名
        colDict: Dict[str, str] = {i: str(i).replace("Col") for i in self.cfg.keys() if "Col" in i}
        self.data[self.cfg["reasonCol"]] = self.data[self.cfg["reasonCol"]].map(
            {j: i for i, j in self.cfg["stateDict"].items()})
        self.session.upload({"colNames": list(colDict.keys())})
        self.session.upload({"targetNames": list(colDict.values())})
        self.session.run(f"""
            {self.tableName} = <select _$$colNames as _$$targetNames from {self.tableName}>.eval();
        """)

class TradeDetails(Table):
    def __init__(self, session: ddb.session, tableName: str, data: pd.DataFrame):
        super().__init__(session, tableName, data)
        self.tableName: str = "tradeDetails"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    @override
    def upload(self): # 规范状态名称 +上传 + 改名
        colDict: Dict[str, str] = {i: str(i).replace("Col") for i in self.cfg.keys() if "Col" in i}
        self.data[self.cfg["reasonCol"]] = self.data[self.cfg["reasonCol"]].map(
            {j: i for i, j in self.cfg["stateDict"].items()})
        self.session.upload({"colNames": list(colDict.keys())})
        self.session.upload({"targetNames": list(colDict.values())})
        self.session.run(f"""
            {self.tableName} = <select _$$colNames as _$$targetNames from {self.tableName}>.eval();
            update {self.tableName} set reasonState = nullFill(stateDict[reasonState],reasonState);
        """)

    def groupByOrder(self):
        """订单聚合"""
        self.session.run(f"""
        {self.tableName} = select sum(vol) as vol, 
                                  first(reason) as reason 
                          from {self.tableName} group by tradeTime, state, direction, symbol, price
        """)