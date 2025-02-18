from __future__ import annotations
from Services.Inventory import Product, Inventory
from Services.Stall import _StallInventory
from .DBProducts import DBProduct, DBStallProduct
from Services.Storehouse import Storehouse
from .DBInventory import DBStallInventory, DBInventory
from typing import Dict, TypeVar
import datetime
from .structure import dynamodb, Structure, getisodatetime, getuuid
from .base import notNone, toString, toFloat, notNegative, convertEmptyString, toString, batch_run, toInt
from decimal import Decimal

FILLUP = 40
# DBS = TypeVar('DBS', bound='DBStorehouse')


class DBStorehouse(Storehouse):
    client = dynamodb()
    table = "Inventory"

    class attr:
        primaryKeySetter = lambda user, storehouseid: f"USER#{user}:Storehouse#{storehouseid}"
        sortKeyFormat = "STOREHOUSE#"
        sortKeySetter =  lambda x: f"STOREHOUSE#{x}"
        storehouseid = 'storehouseid'
        name = 'name'
        description = 'description'
        location = 'location'

        @classmethod
        def itemSetter(cls, **kwargs):
            result = {}
            name = kwargs.get("name")
            storehouseid = kwargs.get("storehouseid")
            description = kwargs.get("description")
            location = kwargs.get("location")
            if name:
                result[cls.name] = {"S": name}
            if storehouseid:
                result[cls.storehouseid] = {"N": str(storehouseid)}
            if description:
                result[cls.description] = {"S": description}
            if location:
                result[cls.location] = {"S": location}
            return result
        
        @classmethod
        def unwrapper(cls, item):
            return {
                cls.storehouseid: toInt(Structure.unwrapper(item[cls.storehouseid])),
                cls.name: Structure.unwrapper(item[cls.name]),
                cls.description: Structure.unwrapper(item[cls.description]),
                cls.location: Structure.unwrapper(item[cls.location])
            }
        
            

    def __init__(
            self,
            user:str = None,
            uid:int = None,
            **kwargs
    ):
        self._inventoryList:dict[int, DBInventory] = {}
        self._uid = uid
        self.user = user
        super().__init__(**kwargs)
        storehouse = kwargs.get("storehouse")
        if isinstance(storehouse, DBStorehouse):
            self._uid = storehouse._uid if self._uid is None else self._uid
            self.user = storehouse.user if self.user is None else self.user


    @classmethod
    def loadStorehouse(cls, user, storehouseid=None)->DBStorehouse:
        if storehouseid == None:
            storehouseid = Structure.DefaultStorehouseId
        pk = cls.attr.primaryKeySetter(user, storehouseid)
        sk = Structure.META
        response = cls.client.query(
            TableName = cls.table,
            KeyConditionExpression = f"{Structure.PrimaryKey} = :primarykey AND begins_with({Structure.SortKey}, :sortkey)",
            ExpressionAttributeValues={
                ':primarykey': {Structure.PrimaryKeyType: pk},
                ':sortkey': {Structure.SortKeyType: sk}
            }
        )
        if not response["Items"]:
            return None
        item = response["Items"][0]
        i = cls.attr.unwrapper(item)
        return DBStorehouse(
            uid = i[cls.attr.storehouseid],
            user = user,
            name = i[cls.attr.name],
            description = i[cls.attr.description],
            location = i[cls.attr.location],
        )
    
    @staticmethod
    def load(user, productid:str, quantity:int, storehouseid=None, **kwargs):
        storehouseid = Structure.DefaultStorehouseId if storehouseid is None else storehouseid
        DBInventory.load(storehouseid = storehouseid, user = user, quantity=quantity, productid=productid, **kwargs)
    
    @staticmethod
    def batchLoad(storehouseid=None, **kwargs)->bool:
        storehouseid = Structure.DefaultStorehouseId if storehouseid is None else storehouseid
        return DBInventory.batchLoad(storehouseid=storehouseid,**kwargs)

    @staticmethod
    def offload(productid:str, quantity:int, **kwargs):
        DBInventory.offload(storehouseid = self._uid, user = self.user, quantity=quantity, productid=productid, **kwargs)

    @staticmethod
    def batchOffload(storehouseid=None, **kwargs)->bool:
        storehouseid = Structure.DefaultStorehouseId if storehouseid is None else storehouseid
        return DBInventory.batchOffload(storehouseid=storehouseid,**kwargs)

    def refreshInventoryList(self):
        productList = DBProduct.getProductList(self.user)
        inventoryList = DBInventory.getInventoryList(self.user, self._uid)
        for key, inventory in inventoryList.items():
            inventory.product = productList.get(key) if productList.get(key) else inventory.product
        self._inventoryList = inventoryList
        return inventoryList

    @staticmethod
    def getProductList(user):
        return DBProduct.getProductList(user)

    def updateProductListFromInventoryList(self, rawInventoryList:dict[str, DBInventory], productList:dict[str, DBProduct])->dict[str, DBInventory]:
        return DBInventory.updateProductListFromInventoryList(self._uid, rawInventoryList, productList)
    
    def getInventoryList(self, user)->set[dict[str, DBProduct], dict[str, DBInventory]]:
        return DBInventory.getInventoryList(user, self._uid)
    
    def getRawInventoryList(self, user)->dict[str, DBInventory]:
        return DBInventory.getRawInventoryList(user, self._uid)
    
    def linkInventory(self, inventory: DBInventory) -> int:
        return NotImplemented

    def refreshMetadata(self,connection:Connection=None, cur:Cursor=None):
        return NotImplemented
        
    def updateMetadata(self,connection:Connection=None, cur:Cursor=None):
        return NotImplemented
        
    def moveout(self, productId:int, storehouse:DBStorehouse, quantity:int, connection:Connection=None, cur:Cursor=None, *args, **kwargs):
        return NotImplemented
        result, key = storehouse.hasProduct(productId)
        if result:
            raise NotExistError(f"Storehouse {storehouse.name}({storehouse._uid}) has no Product({productId})")
        self.refreshInventoryList(connection=connection, cur=cur)

        inventory = self._inventoryList.get(productId)
        if inventory is None:
            raise NotExistError(f"Storehouse {self.name}({self._uid}) has no Product({productId})")
        loadDatetime = datetime.datetime.now()
        inventory.moveout(self._uid, quantity, loadDatetime, connection=connection, cur=cur)
        storehouse.movein(key, quantity, loadDatetime, connection=connection, cur=cur, *args, **kwargs)

    def movein(self, productId:int, quantity:int, loadDatetime:datetime.datetime, connection:Connection=None, cur:Cursor=None, *args, **kwargs):
        return NotImplemented
        inventory = self._inventoryList.get(productId)
        if inventory is None:
            inventory = DBInventory.new(self._uid, productId, quantity, connection=connection, cur=cur, loadDatetime=loadDatetime)
        else:
            inventory.movein(self._uid, quantity, loadDatetime, connection=connection, cur=cur)


    def hasProduct(self, productId:int, connection:Connection=None, cur:Cursor=None) -> tuple[bool, int]:
        return NotImplemented
        self.refreshInventoryList(connection, cur)
        return (True, productId) if self._inventoryList.get(productId) else (False, None)

    def inventoryInfo(self):
        print(f"{self.name}({self._uid})'s Inventory".center(FILLUP,"-"))
        for id, inventory in self._inventoryList.items():
            inventory.info()

    def getInfo(self) -> str:
        return f"uid: {self._uid}\n" + super().getInfo()
    
    def info(self):
        print(f"Storehouse {self.name}({self._uid}) Details".center(FILLUP,"-")+"\n"+
            self.getInfo()+"-"*FILLUP
        )

# Just a interface in dynamodb case
class DBStallStorehouse(DBStorehouse):
    def __init__(
            self,
            stallId:int = None,
            assignId:int = None,
            **kwargs
    ):
        self._inventoryList:dict[int, DBStallInventory] = {}
        self.stallId = stallId
        self.assignId = assignId
        storehouse = kwargs.get("storehouse")
        super().__init__(**kwargs)
        if isinstance(storehouse, DBStallStorehouse):
            self.stallId = storehouse.stallId if self.stallId is None else self.stallId
            self.assignId = assignId if self.assignId is None else self.assignId

    def refreshInventoryList(self, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    def quickOffload(self, sets:list[int,int], connection:Connection=None, cur:Cursor=None, loadDatetime=None) -> bool:
        return NotImplemented

    @staticmethod
    def loadFromStorehouse(user, stallId, stallproductid, quantity) -> bool:
        return NotImplemented

    def quickmove(self, fromId:int, sets:list[tuple[int,int]], connection:Connection, loadDatetime=None) -> bool:
        return NotImplemented
        
    @staticmethod
    def moveout(**kwargs):
        return NotImplemented

    @staticmethod
    def movein(**kwargs):
        return DBStallInventory.movein(**kwargs)

    @staticmethod
    def batchMovein(**kwargs):
        return DBStallInventory.batchMoveIn(storehouseid= Structure.DefaultStorehouseId, **kwargs)

    def hasProduct(self, productId:int, connection:Connection=None, cur:Cursor=None) -> tuple[bool, int]:
        return NotImplemented

if __name__ == '__main__':
    storehouse = DBStorehouse.loadStorehouse("owner")
    storehouse.info()
    storehouse.load(
        productid = "95489b2b-88c2-4c96-b165-4383cb4b0aec", 
        quantity = 1
    )
    storehouse.info()
    storehouse.refreshInventoryList()
    storehouse.inventoryInfo()
