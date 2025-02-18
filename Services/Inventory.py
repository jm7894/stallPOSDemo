from __future__ import annotations
from typing import List, TypeVar
from .Products import Product, _StallProduct
from .Error import DifferentProductError, LoadQuantityError, OffloadQuantityError
import datetime
from abc import ABC, abstractmethod
from copy import copy


FILLUP = 40

# I = TypeVar('I', bound='Inventory')
# SI = TypeVar("SI", bound="_StallInventory")

class BaseInventory(ABC):
    def __init__(
            self, 
            product:Product = None, 
            loadDatetime:datetime.datetime=None,
            **kwargs
        ):
        self.product = product
        self.loadDatetime = loadDatetime
        inventory = kwargs.get("inventory")
        if isinstance(inventory, BaseInventory):
            self.product = inventory.product if self.product is None else self.product
            self.loadDatetime = inventory.loadDatetime if self.loadDatetime is None else self.loadDatetime
        self.loadDatetime = datetime.datetime.now() if loadDatetime is None else self.loadDatetime
        self._quantity:int = 0
    
    @abstractmethod
    def isSameProduct(self, other:Inventory) -> bool:
        ...

    @abstractmethod
    def offload(self, quantity:int) -> BaseInventory:
        ...

    @abstractmethod
    def load(self, quantity:int) -> BaseInventory:
        ...

    def getQuantity(self) -> int:
        return self._quantity

    def getName(self) -> str:
        return self.product.getName()
    
    def getProduct(self) -> Product:
        return self.product
    

class Inventory(BaseInventory):
    
    def isSameProduct(self, other:Inventory) -> bool:
        return self.product.isSameProduct(other.product)

    def offload(self, quantity:int) -> Inventory:
        if quantity <= 0:
            raise OffloadQuantityError("Non Positive Quantity!")

        if quantity <= self._quantity:
            self._quantity -= quantity
            return self
        else:
            raise OffloadQuantityError(f"{self.getName()} is OUT OF STOCK!")
    
    def load(self, quantity:int) -> Inventory:
        if quantity <= 0:
            raise LoadQuantityError("Non Positive Quantity!")
        self._quantity += quantity
        return self
    
    def stocking(self, inventory:Inventory, quantity:int) -> Inventory:
        if not self.isSameProduct(inventory):
            raise DifferentProductError(f"Stocking Different Products {self.product.getName()} from {inventory.getName()}")
        inventory.offload(quantity)
        self.product = Product(product=inventory.getProduct())
        self._quantity += quantity
        return inventory

    def getInfo(self):
        productInfo = self.product.getInfo()
        return  productInfo + \
                f"quantity: {self._quantity}\n"\
                f"loadDatetime: {self.loadDatetime}\n"

    def copy(self) -> Inventory:
        return Inventory(self.product.copy(), self.loadDatetime)

    def info(self):
        print(self.getInfo()+"-"*FILLUP)

class _StallInventory(Inventory):
    def __init__(
            self,
            **kwargs 
        ):
            product = kwargs.get("product")
            if isinstance(product, _StallInventory):
                raise TypeError(f"{product.__class__.__name__} is not {_StallProduct.__name__}")
            super().__init__(**kwargs)

    def stocking(self, inventory:Inventory, quantity:int) -> Inventory:
        if not self.isSameProduct(inventory):
            raise DifferentProductError("Stocking Different Products")

        inventory.offload(quantity)
        self.product = _StallProduct(self.getSettingPrice(), product=inventory.getProduct())
        self._quantity += quantity
        return inventory
    
    def getSettingPrice(self):
        return self.product.getSettingPrice()

    def getInfo(self) -> str:
        return super().getInfo()
    
    def info(self) -> None:
        print(self.getInfo()+"-"*FILLUP)


class InventoryList:
    def __init__(self, inventoryList:list[Inventory]):
        self.list = inventoryList
    
    def info(self):
        print('HERE THE INVENTORY LIST'.center(FILLUP,'-'))
        for inventory in self.list:
            inventory.info()

