from typing import List
import datetime
from .Products import _StallProduct, TransactionProduct

FILLUP = 40

class Transaction:
    def __init__(
            self, 
            transactionDatetime:datetime.datetime=datetime.datetime.now()
        ):
        self._productList:list[TransactionProduct] = []
        self.transactionDatetime = transactionDatetime

    def addProduct(self, product:_StallProduct, price:int, quantity:int):
        transactionProduct = TransactionProduct(quantity, price, product=product)
        self._productList.append(transactionProduct)

    def getInfo(self):
        return  f"datetime: {self.transactionDatetime}\n"

    def info(self):
        print("Transaction Info".center(FILLUP, "-"))
        print(self.getInfo()+"-"*FILLUP)

    def detialsInfo(self):
        print("Transaction Details".center(FILLUP,"-"))
        for p in self._productList:
            p.info()
