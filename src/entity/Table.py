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

    def upload(self):   # 上传 + 改名
        colDict: Dict[str, str] = {self.cfg[i]: str(i).replace("Col") for i in self.cfg.keys() if "Col" in i}
        self.data.rename(columns=colDict)
        self.session.upload({self.tableName: self.data})

class OrderDetails(Table):
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        super().__init__(session, "orderDetails", data)
        self.tableName: str = "orderDetails"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    def upload(self):   # 规范状态名称 + dmlStr +上传 + 改名
        colDict: Dict[str, str] = {self.cfg[i]: str(i).replace("Col","") for i in self.cfg.keys() if "Col" in i}
        self.data[self.cfg["reasonCol"]] = self.data[self.cfg["reasonCol"]].map(
            {j: i for i, j in self.cfg["reasonState"].items()})
        self.data.rename(columns=colDict, inplace=True)
        self.session.upload({self.tableName: self.data})
        if self.cfg["dmlStr"] not in ["",None]:
            self.session.run(self.cfg["dmlStr"])

class TradeDetails(Table):
    def __init__(self, session: ddb.session, data: pd.DataFrame):
        super().__init__(session, "tradeDetails", data)
        self.tableName: str = "tradeDetails"
        self.cfg: Dict = {}

    def fromDict(self, cfg: Dict):
        self.cfg = cfg

    def upload(self):   # 规范状态名称 + dmlStr +上传 + 改名
        colDict: Dict[str, str] = {self.cfg[i]: str(i).replace("Col","") for i in self.cfg.keys() if "Col" in i}
        self.data[self.cfg["reasonCol"]] = self.data[self.cfg["reasonCol"]].map(
            {j: i for i, j in self.cfg["reasonState"].items()})
        self.data.rename(columns=colDict, inplace=True)
        self.session.upload({self.tableName: self.data})
        if self.cfg["dmlStr"] not in ["",None]:
            self.session.run(self.cfg["dmlStr"])