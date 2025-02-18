from typing import List, TypeVar, Tuple, TypedDict
import datetime
from .Inventory import Inventory, _StallInventory
from .Transaction import TransactionProduct, Transaction
from .Products import _StallProduct
from .Storehouse import Storehouse
import copy

FILLUP = 40

class StallKeySet(TypedDict):
    key: int
    price: int
    quantity: int

class Stall:
    def __init__(
            self, 
            name:str=None, 
            location:str=None, 
            startDatetime:datetime.datetime=None, 
            endDatetime:datetime.datetime=None, 
            description="",
            **kwargs
        ):
        self.name = name
        self.location = location
        self.startDatetime = startDatetime
        self.endDatetime = endDatetime
        self.description = description
        self.storehouse = Storehouse(self.name+"'s storehouse")
        self.transactionList:list[Transaction] = []
    
    # Prepare Products For selling
    def prepare(self, inventory:Inventory, settingPrice:int) -> int:
        stallProduct = _StallProduct(settingPrice, product=inventory.getProduct())
        stallInventory = _StallInventory(stallProduct)
        key = self.storehouse.linkInventory(stallInventory)
        return key
        
    def stocking(self, key:int, inventory:Inventory, quantity:int)->Inventory:
        return self.storehouse.stocking(key, inventory, quantity)
    
    # atomicity
    def transact(self, keys:list[StallKeySet]) -> Transaction:
        transaction = Transaction()
        safe_storehouse = self.storehouse.copy()
        for set in keys:
            key, quantity, price = set["key"], set["quantity"], set["price"]
            try:
                safe_inventory = safe_storehouse.copyInventory(key)
            except IndexError as e:
                raise IndexError("Not Valid Key")
            
            price = price if price else safe_inventory.price
            safe_inventory.offload(quantity)
            safe_storehouse.updateInventory(key, safe_inventory)

            transaction.addProduct(safe_inventory.getProduct(), price, quantity)
        # atomicity??
        self.storehouse = safe_storehouse
        self.transactionList.append(transaction)
        return transaction


    def getStall(self):
        return self.name
    
    def isSameStall(self, other):
        return self.getStall() == other.getStall()

    def getInfo(self):
        return  f"name: {self.name}\n"\
                f"location: {self.location}\n"\
                f"start: {self.startDatetime}\n"\
                f"end: {self.endDatetime}\n"\
                f"description: {self.description}\n"

    def info(self):
        print(self.getInfo()+"-"*FILLUP)
    
    def inventoryInfo(self):
        self.storehouse.inventoryInfo()
    
    def transactionsInfo(self):
        for t in self.transactionList:
            t.info()
            t.detialsInfo()

    def updateName(self, name:str):
        self.name = name
        self.storehouse.updateName(name+"'s storehouse")

