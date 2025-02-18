from __future__ import annotations
from .Inventory import Inventory
from .Error import ProductNotPrepared
from typing import List, Dict
import copy
import datetime

FILLUP = 40

class Storehouse:
    def __init__(
            self, 
            name:str = None, 
            location:str = None, 
            description:str = None, 
            inventoryList:Dict[str, Inventory] = None,
            **kwargs
        ):
        self.name = name
        self.location = location
        self.description = description
        self._inventoryList:Dict[str, Inventory] = inventoryList
        storehouse = kwargs.get("storehouse")
        if isinstance(storehouse, Storehouse):
            self.name = storehouse.name if self.name  is None else self.name
            self.location = storehouse.location if self.location  is None else self.name
            self.description = storehouse.description if self.description is None else self.description
            self._inventoryList = storehouse._inventoryList if not self._inventoryList else self._inventoryList
        if self._inventoryList is None:
            self._inventoryList = {}

    def getInventoryList(self):
        return self._inventoryList
    
    # return key
    # **LINK** Product inventory to storehouse
    def linkInventory(self, inventory:Inventory) -> str:
        """Link Product Inventory to Storehouse
        """
        keyExists = self._inventoryList.get(inventory.getName())
        key = inventory.getName()
        if keyExists:
            print(f"{inventory.getName()}'s Inventory already exists in Storehouse {self.name}")
            # self._inventoryList[key] = self._inventoryList[key].load(inventory.getQuantity())
        else:
            self._inventoryList[key] = inventory
            self._inventoryList[key].loadDatetime = datetime.datetime.now()
        return key
    
    def stocking(self, key:str, inventory:Inventory, quantity:int)->Inventory:
        thisInventory = self._inventoryList.get(key)
        if thisInventory is None:
            raise ProductNotPrepared(f"{inventory.getName()} currently not prepared in storehouse {self.name}")
        return thisInventory.stocking(inventory, quantity)

    def updateInventory(self, key:int, inventory:Inventory):
        if self._inventoryList.get(key) is None:
            raise KeyError(f"{key} doesn't exist in storehouse {self.name}")
        self._inventoryList[key] = inventory
        exit()

    def replaceInventoryList(self, inventoryList:Dict[str, Inventory]):
        self._inventoryList = inventoryList
    
    def copy(self):
        copied = copy.copy(self)
        copied.replaceInventoryList(self._inventoryList.copy())
        return copied
    
    def copyInventory(self, key:int):
        inventory = self._inventoryList.get(key)
        if inventory is None:
            raise KeyError(f"{key} doesn't exist in storehouse {self.name}")
        return copy.copy(inventory)

    def access(self, key:int):
        inventory = self._inventoryList.get(key)
        if inventory is None:
            raise KeyError(f"{key} doesn't exist in storehouse {self.name}")
        return inventory

    def getInfo(self):
        return  f"name: {self.name}\n"\
                f"location: {self.location}\n"\
                f"description: {self.description}\n"
    
    def updateName(self, name:str):
        self.name = name

    def updateDescription(self, description:str):
        self.description = description

    def updateLocation(self, location:str):
        self.location = location

    def info(self):
        print(f"Storehouse {self.name} Details".center(FILLUP,"-")+"\n"+
            self.getInfo()+"-"*FILLUP
        )
    
    def inventoryInfo(self):
        print(f"{self.name}'s Inventory".center(FILLUP,"-"))
        for key, i in self._inventoryList.items():
            i.info()

    
