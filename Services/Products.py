from __future__ import annotations
from typing import List
from abc import ABC, abstractmethod
from copy import copy, deepcopy

FILLUP = 40

# P = TypeVar('P', bound='Product')


class BaseProduct(ABC):
    # Init
    def __init__(self, name:str=None, cost:int=None, description:str=None, **kwargs):
        self.name = name 
        self.cost = cost 
        self.description = description 
        product = kwargs.get("product")
        if isinstance(product, BaseProduct):
            self.name = product.name if self.name is None else self.name
            self.cost = product.cost if self.cost is None else self.cost
            self.description = product.description if self.description is None else self.description
        
    # Chceck 2 produdcts are same or not
    @abstractmethod
    def isSameProduct(self, other: BaseProduct) -> bool:
        ...

    # return Product Name
    def getName(self) -> str:
        return self.name
    
    # return string for info()
    def getInfo(self) -> str:
        return  f"name: {self.name}\n"\
                f"cost: {self.cost}\n"\
                f"description: {self.description}\n"

    # print Product's all attribute-values
    def info(self) -> None:
        print("Product Info".center(FILLUP, " "))
        print(self.getInfo()+"-"*FILLUP)


class Product(BaseProduct):

    # Override isSameProduct
    # Check whether 2 proudct is same by their name
    def isSameProduct(self, other:Product) -> bool:
        return self.name == other.name
    
    # (NEW) return a copied object of itself
    def copy(self):
        return copy(self)


class _StallProduct(Product):
    def __init__(
            self,
            settingPrice:int=None,
            **kwargs 
        ):
        product = kwargs.get("product")
        self.settingPrice = settingPrice
        if isinstance(product, _StallProduct):
            self.settingPrice = product.settingPrice if self.settingPrice is None else self.settingPrice
        super().__init__(**kwargs)

    # Override getinfo 
    # all StallProduct's Attribute-values
    def getInfo(self) -> str:
        return super().getInfo()+f"settingPrice: {self.settingPrice}\n"

    # (NEW) return StallProduct's settingPrice
    def getSettingPrice(self) -> int:
        return self.settingPrice

class TransactionProduct(_StallProduct):
    def __init__(
            self,
            quantity:int=None,
            sellingPrice:int=None,
            **kwargs
        ):
        self.quantity = quantity
        self.sellingPrice = sellingPrice

        product = kwargs.get("product")
        if isinstance(product, TransactionProduct):
            self.quantity = product.quantity if self.quantity is None else self.quantity
            self.sellingPrice = product.sellingPrice if self.sellingPrice is None else self.sellingPrice
        super().__init__(**kwargs)
    
    # Override getinfo 
    # all TransactionProduct's Attribute-values
    def getInfo(self):
        return super().getInfo()+f"sellingPrice: {self.sellingPrice}\nquantity: {self.quantity}\n"

