from __future__ import annotations
from typing import List, TypeVar, Dict
from Services.Inventory import Inventory
import datetime
from Services.Stall import _StallInventory
from .DBProducts import DBStallProduct, DBProduct
import uuid
from .structure import dynamodb, Structure, getisodatetime, getuuid, getulid, responseItemUnwrapper
from .base import notNone, toString, toFloat, notNegative, convertEmptyString, toString, toInt, batch_run, removeEmptyValues
from decimal import Decimal
from botocore.exceptions import ClientError


FILLUP = 40
PRODUCTNAME = "name"
PRODUCTCOST = "cost"
PRODUCTDESCRIPTION = "description"
PRODUCTID = "id"


class DBInventory(Inventory):
    client = dynamodb()
    table = "Inventory"
    storehouseInventory = "StorehouseInventory"

    class DBInventoryAction:
        LOAD = "LOAD"
        OFFLOAD = "OFFLOAD"
        MOVEIN = "MOVEIN"
        MOVEOUT = "MOVEOUT"
        SOLD = "SOLD"

    class attr:
        primaryKeySetter = lambda user, storehouseid: f"USER#{user}:Storehouse#{storehouseid}"
        sortKeyFormat = lambda productid: f"Product-Inventory#{productid}"
        sortKeySetter = lambda productid, inventoryid: f"Product-Inventory#{productid}-{inventoryid}"
        metaSortKeyFormat = lambda x,y: f"Product-Inventory#"
        metaSortKeySetter = lambda x: f"Product-Inventory#{x}"
        
        storehouseid, storehouseidTypeSetter = 'storehouseid', Structure.NumberTypeSetter
        productid, productidTypeSetter = 'productid', Structure.StringTypeSetter
        quantity, quantityTypeSetter = 'quantity', Structure.NumberTypeSetter
        datetime, datetimeTypeSetter = 'datetime', Structure.StringTypeSetter
        action, actionTypeSetter = 'action', Structure.StringTypeSetter
        inventoryid, inventoryidTypeSetter = 'inventoryid', Structure.StringTypeSetter
        totalquantity, totalquantityTypeSetter = 'totalquantity', Structure.NumberTypeSetter

        @classmethod
        def itemSetter(cls, **kwargs):
            result = {}
            productid = kwargs.get("productid")
            storehouseid = kwargs.get("storehouseid")
            quantity = kwargs.get("quantity")
            datetime = kwargs.get("datetime")
            action = kwargs.get("action")
            inventoryid = kwargs.get("inventoryid")
            totalquantity = kwargs.get("totalquantity")

            if storehouseid:
                result[cls.storehouseid] = cls.storehouseidTypeSetter(storehouseid)
            if productid:
                result[cls.productid] = cls.productidTypeSetter(productid)
            if quantity:
                result[cls.quantity] = cls.quantityTypeSetter(quantity)
            if datetime:
                result[cls.datetime] = cls.datetimeTypeSetter(getisodatetime(datetime))
            if action:
                result[cls.action] = cls.actionTypeSetter(action)
            if inventoryid:
                result[cls.inventoryid] = cls.inventoryidTypeSetter(inventoryid)
            if totalquantity or totalquantity == 0:
                result[cls.totalquantity] = cls.totalquantityTypeSetter(totalquantity)
            return result
    
    def __init__(
            self, 
            storehouseId:int=None,
            **kwargs
        ):
        self.storehouseId = storehouseId
        inventory = kwargs.get("inventory")
        if inventory:
            super().__init__(inventory.product, inventory.loadDatetime)
            self._quantity = inventory.getQuantity()
        else:
            super().__init__(**kwargs)
        if isinstance(inventory, DBInventory):
            self.storehouseId = inventory.storehouseId if not self.storehouseId else self.storehouseId
        
        quantity = kwargs.get("quantity")
        if quantity:
            self._quantity = quantity

    @classmethod
    def loadHandler(cls, user, storehouseid, productid, quantity, datetime, action, inventoryid=None):
        pk = cls.attr.primaryKeySetter(user,storehouseid)
        if inventoryid is None:
            inventoryid = getulid()
        put_sk = cls.attr.sortKeySetter(productid, inventoryid)
        update_sk = cls.attr.metaSortKeySetter(productid)
        return [
            {
                'Put': {
                    'TableName': cls.table,
                    'Item': {
                        **Structure.itemSetter(pk, put_sk),
                        **cls.attr.itemSetter(
                            storeohuseid = storehouseid,
                            productid = productid,
                            quantity = quantity,
                            datetime = datetime,
                            action = action,
                            inventoryid = inventoryid
                        )
                    }
                }
            },
            {
                'Update': {
                    'TableName': cls.table,
                    'Key': {
                        **Structure.itemSetter(pk, update_sk)
                    },
                    'UpdateExpression': f'SET {cls.attr.storehouseid} = :storehouseid, '\
                                        f'{cls.attr.productid} = :productid, '\
                                        f'{cls.attr.totalquantity} = if_not_exists({cls.attr.totalquantity}, :zero) + :incr',
                    'ExpressionAttributeValues': {
                        ":storehouseid": cls.attr.storehouseidTypeSetter(storehouseid),
                        ":productid": cls.attr.productidTypeSetter(productid),
                        ":zero": cls.attr.totalquantityTypeSetter(0),
                        ":incr": cls.attr.totalquantityTypeSetter(quantity)
                    }
                }
            }
        ]
    
    @classmethod
    def offloadHandler(cls, user, storehouseid, productid, quantity, datetime, action, inventoryid=None):
        pk = cls.attr.primaryKeySetter(user,storehouseid)
        if inventoryid is None:
            inventoryid = getulid()
        put_sk = cls.attr.sortKeySetter(productid, inventoryid)
        update_sk = cls.attr.metaSortKeySetter(productid)
        return [
            {
                'Put': {
                    'TableName': cls.table,
                    'Item': {
                        **Structure.itemSetter(pk, put_sk),
                        **cls.attr.itemSetter(
                            storeohuseid = storehouseid,
                            productid = productid,
                            quantity = -quantity,
                            datetime = datetime,
                            action = action,
                            inventoryid = inventoryid
                        )
                    }
                }
            },
            {
                'Update': {
                    'TableName': cls.table,
                    'Key': {
                        **Structure.itemSetter(pk, update_sk)
                    },
                    'UpdateExpression': f'SET {cls.attr.storehouseid} = :storehouseid, '\
                                        f'{cls.attr.productid} = :productid, '\
                                        f'{cls.attr.totalquantity} = if_not_exists({cls.attr.totalquantity}, :zero) - :decr',
                    'ConditionExpression': f"{cls.attr.totalquantity} >= :decr",
                    'ExpressionAttributeValues': {
                        ":storehouseid": cls.attr.storehouseidTypeSetter(storehouseid),
                        ":productid": cls.attr.productidTypeSetter(productid),
                        ":zero": cls.attr.totalquantityTypeSetter(0),
                        ":decr": cls.attr.totalquantityTypeSetter(quantity)
                    },
                }
            }
        ]


    @classmethod
    def _load(cls, user, storehouseid, productid, quantity, datetime, action)-> bool:
        if quantity <= 0:
            return False
        cls.client.transact_write_items(
            TransactItems = cls.loadHandler(user, storehouseid, productid, quantity, datetime, action)
        )
        return True

    @classmethod
    def _offload(cls, user, storehouseid, productid, quantity, datetime, action) -> bool:
        if quantity <= 0:
            return False
        try:
            cls.client.transact_write_items(
                TransactItems = cls.offloadHandler(user, storehouseid, productid, quantity, datetime, action)
            )
        except ClientError as e:
            # print(e)
            return False
        return True

    @classmethod
    def getProductInventoryList(cls, user, storehouseid, productid):
        response = cls.client.query(
            TableName = cls.table,
            KeyConditionExpression = f"{Structure.PrimaryKey} = :primarykey AND begins_with({Structure.SortKey}, :sortkey)",
            ExpressionAttributeValues = {
                ':primarykey': Structure.PrimaryKeyTypeSetter(cls.attr.primaryKeySetter(user, storehouseid)),
                ':sortkey': Structure.SortKeyTypeSetter(cls.attr.sortKeyFormat(productid)),
            },
            ProjectionExpression = f"{cls.attr.productid}, "\
                                   f"{cls.attr.storehouseid}, "\
                                   f"{cls.attr.quantity}, "\
                                   "#datetime, "\
                                   "#action, "\
                                   f"{cls.attr.totalquantity}",
            ExpressionAttributeNames = {
                "#datetime": cls.attr.datetime,
                "#action": cls.attr.action,
            }
        )
        return response["Items"]

    @classmethod
    def getRawInventoryList(cls, user, storehouseid) -> dict[str, DBInventory]:
        now = getisodatetime()
        response = cls.client.query(
            TableName = cls.table,
            IndexName = cls.storehouseInventory,
            KeyConditionExpression = f"{Structure.PrimaryKey} = :primarykey",
            ExpressionAttributeValues = {
                ':primarykey': Structure.PrimaryKeyTypeSetter(cls.attr.primaryKeySetter(user, storehouseid)),
            },
            ProjectionExpression = f"{cls.attr.productid}, "\
                                   f"{cls.attr.totalquantity}",
        )
        items = response["Items"]
        result = {}
        for item in items:
            i = responseItemUnwrapper(item)
            quantity = int(i[cls.attr.totalquantity])
            if quantity <= 0:
                continue
            result[i[cls.attr.productid]] = DBInventory(
                            storehouseId = storehouseid, 
                            quantity = quantity,
                            loadDatetime = now,
                            product = DBProduct(uid = i[cls.attr.productid]))      
        return result
    
    @classmethod
    def getInventoryList(cls, user, storehouseid) -> set[dict[str, DBProduct], dict[str, DBInventory]]:
        rawInventoryList = cls.getRawInventoryList(user, storehouseid)
        productList = DBProduct.getProductList(user)
        cls.updateInventoryListFromProductList(rawInventoryList, productList)
        return productList, rawInventoryList
    
    @staticmethod
    def updateProductListFromInventoryList(storehouseid, rawInventoryList:dict[str, DBInventory], productList:dict[str, DBProduct])->dict[str, DBInventory]:
        for productid, product in productList.items():
            inventory = rawInventoryList.get(productid)
            if inventory:
                inventory.product.merge(product)
            else:
                rawInventoryList[productid] = DBInventory(storehouseid, product=product, quantity=0)
        return rawInventoryList

    @classmethod
    def updateInventoryListFromProductList(cls, rawInventoryList:dict[str, DBInventory], productList:dict[str, DBProduct])->dict[str, DBInventory]:
        # print(rawInventoryList)
        deleteList = list()
        for productid, inventory in rawInventoryList.items():
            if not cls.updateInventoryFromProductList(inventory, productList):
                deleteList.append(productid)
        for productid in deleteList:
            del rawInventoryList[productid]
        return rawInventoryList

    @staticmethod
    def updateInventoryFromProductList(inventory:DBInventory, productlist:dict[str, DBProduct])->DBInventory:
        productid = inventory.getProduct().getUID()
        product = productlist.get(productid)
        if product:
            inventory.product.merge(product)
            return inventory
        else:
            return None

            

    @classmethod
    def moveout(cls, **kwargs):
        return NotImplemented
        cls._offload(action=cls.DBInventoryAction.MOVEOUT, **kwargs)
        
    @classmethod
    def movein(cls, **kwargs):
        return NotImplemented
        cls._load(action=cls.DBInventoryAction.MOVEIN, **kwargs)
        
    @classmethod
    def load(cls, **kwargs):
        cls._load(datetime=getisodatetime(), action=cls.DBInventoryAction.LOAD, **kwargs)

    @classmethod
    def batchLoad(cls, user, storehouseid, productList:list[set[str, int]]):
        action = cls.DBInventoryAction.LOAD
        now = getisodatetime()
        items = []
        for productid, quantity in productList:
            if quantity <= 0:
                return None
            items.extend(cls.loadHandler(user, storehouseid, productid, quantity, now, action))
        if not items:
            return True
        # print(items)
        
        return cls.client.transact_write_items(TransactItems = items)
        # try:
        #     return cls.client.transact_write_items(TransactItems = items)
        # except ClientError as e:
        #     print(e)
        #     return None
    
    @classmethod
    def batchOffload(cls, user, storehouseid, productList:list[set[str, int]]):
        action = cls.DBInventoryAction.OFFLOAD
        now = getisodatetime()
        items = []
        for productid, quantity in productList:
            if quantity <= 0:
                return None
            items.extend(cls.offloadHandler(user, storehouseid, productid, quantity, now, action))
        if not items:
            return True
        try:
            return cls.client.transact_write_items(TransactItems = items)
        except ClientError as e:
            # print(e)
            return None

    @classmethod
    def offload(cls, **kwargs):
        cls._offload(datetime=getisodatetime(), action=cls.DBInventoryAction.OFFLOAD, **kwargs)

    def getUID(self)->int:
        return self.product.getUID()

    def updateDatetime(self, loadDatetime:datetime.datetime = None):
        self.loadDatetime = loadDatetime if loadDatetime else getisodatetime()

    def getInfo(self)->str:
        return f"storehouseId: {self.storehouseId}\n"+super().getInfo()
    
    def toDict(self):
        return {
            "product": self.product.toDict(),
            "storehouseId": self.storehouseId,
            "quantity": self._quantity,
            "loadDatetime": self.loadDatetime,
        }

class DBStallInventory(_StallInventory, DBInventory):
    class attr(DBInventory.attr):
        primaryKeySetter = lambda user, stallid: f"USER#{user}:Stall#{stallid}"
        sortKeyFormat = lambda stallproductid: f"StallProduct-Inventory#{stallproductid}"
        sortKeySetter = lambda stallproductid, inventoryid: f"StallProduct-Inventory#{stallproductid}#{inventoryid}"
        metaSortKeyFormat = lambda stallproductid: f"StallProduct-Inventory#{stallproductid}#0"
        metaSortKeySetter = lambda stallproductid: f"StallProduct-Inventory#{stallproductid}#0"

        stallinventoryid, stallinventoryidTypeSetter = 'stallinventoryid', Structure.StringTypeSetter
        stallid, stallidTypeSetter = 'stallid', Structure.StringTypeSetter
        stallproductid, stallproductidTypeSetter = DBStallProduct.attr.stallproductid, DBStallProduct.attr.stallproductidTypeSetter

        @classmethod
        def itemSetter(cls, **kwargs):
            result = DBInventory.attr.itemSetter(**kwargs)
            stallinventoryid = kwargs.get("stallinventoryid")
            stallid = kwargs.get("stallid")
            stallproductid = kwargs.get("stallproductid")

            if stallinventoryid:
                result[cls.stallinventoryid] = cls.stallinventoryidTypeSetter(stallinventoryid)
            if stallid:
                result[cls.stallid] = cls.stallidTypeSetter(stallid)
            if stallproductid:
                result[cls.stallproductid] = cls.stallproductidTypeSetter(stallproductid)
            return result

    _DBINVENTORY = "StallInventory"
    def __init__(
            self,
            stallId:str=None,
            stallInventoryId:str=None,
            **kwargs
    ):
        self.stallInventoryId = stallInventoryId
        self.stallId = stallId
        _StallInventory.__init__(self, **kwargs)
        DBInventory.__init__(self, **kwargs)
        inventory = kwargs.get("inventory")
        if isinstance(inventory, DBStallInventory):
            self.stallId = inventory.stallId if not self.stallId else self.stallId
        
    def stocking(self, inventory:DBInventory, quantity:int) -> DBInventory:
        return NotImplemented
    
    @staticmethod
    def _quickLoad(storehouseId:int, sets:list[int,int], loadDatetime:datetime.datetime, connection:Connection=None, cur:Cursor=None) -> bool:
        return NotImplemented

    @staticmethod
    def _quickOffload(storehouseId:int, sets:list[tuple(int,int)], loadDatetime:datetime.datetime,  connection:Connection=None, cur:Cursor=None) -> DBInventory:
        return NotImplemented

    def load(self, quantity:int, connection:Connection=None, cur:Cursor=None):
        return NotImplemented
    
    def offload(self, quantity:int, connection:Connection=None, cur:Cursor=None):
        return NotImplemented


    def moveout(self, quantity:int, loadDatetime:datetime.datetime, connection:Connection=None, cur:Cursor=None):
        return NotImplemented
    
    @classmethod
    def checkProductHandler(cls, user, stallid, productid, stallproductid, storehouseid=Structure.DefaultStorehouseId):
        check_product_pk = DBProduct.attr.primaryKeySetter(user)
        check_product_sk = DBProduct.attr.sortKeySetter(productid)
        check_stallproduct_pk = DBStallProduct.attr.primaryKeySetter(user, stallid)
        check_stallproduct_sk = DBStallProduct.attr.sortKeySetter(stallproductid)
        return [
            {
                'ConditionCheck': {
                    'TableName': DBProduct.table,
                    'Key': {
                        **Structure.itemSetter(check_product_pk, check_product_sk)
                    },
                    'ConditionExpression': f"attribute_exists({Structure.PrimaryKey}) AND attribute_exists({Structure.SortKey})"
                }
            },
            {
                'ConditionCheck': {
                    'TableName': DBProduct.table,
                    'Key': {
                        **Structure.itemSetter(check_stallproduct_pk, check_stallproduct_sk)
                    },
                    'ConditionExpression': f"attribute_exists({Structure.PrimaryKey}) AND attribute_exists({Structure.SortKey}) AND {DBProduct.attr.productid} = :val",
                    'ExpressionAttributeValues': {':val': cls.attr.productidTypeSetter(productid)}
                }
            }
        ]

    @classmethod
    def batchMoveIn(cls, user, stallid, productStallProductQuantityList:list[set[str, str, int]], storehouseid=Structure.DefaultStorehouseId):
        now = getisodatetime()
        items = []
        for productid, stallproductid, quantity in productStallProductQuantityList:
            if quantity <= 0:
                return None
            stallinventoryid = getulid()
            items.extend(cls.checkProductHandler(user, stallid, productid, stallproductid))
            items.extend(DBInventory.offloadHandler(user, storehouseid, productid, quantity, now, cls.DBInventoryAction.MOVEOUT, stallinventoryid))
            items.extend(cls.loadHandler(user, stallid, productid, stallproductid, quantity, now, stallinventoryid, storehouseid))
        if not items:
            return True
        try:
            return cls.client.transact_write_items(TransactItems = items)
        except ClientError as e:
            # print(e)
            return None
    

    @classmethod
    def loadHandler(cls, user, stallid, productid, stallproductid, quantity, loaddatetime, stallinventoryid=None, storehouseid=Structure.DefaultStorehouseId, action=None):
        if not action:
            action = cls.DBInventoryAction.MOVEIN
        stallinventoryid = getulid() if stallinventoryid is None else stallinventoryid
        pk = cls.attr.primaryKeySetter(user, stallid)
        put_sk = cls.attr.sortKeySetter(stallproductid, stallinventoryid)
        update_sk = cls.attr.metaSortKeySetter(stallproductid)
        return [
            {
                'Put': {
                    'TableName': cls.table,
                    'Item': {
                        **Structure.itemSetter(pk, put_sk),
                        **cls.attr.itemSetter(
                            stallproductid = stallproductid,
                            productid = productid,
                            quantity = quantity,
                            datetime = loaddatetime,
                            action = action,
                            stallid = stallid,
                            stallinventoryid = stallinventoryid
                        )
                    }
                }
            },
            {
                'Update': {
                    'TableName': cls.table,
                    'Key': {
                        **Structure.itemSetter(pk, update_sk)
                    },
                    'UpdateExpression': f'SET {cls.attr.stallid} = :stallid, '\
                                        f'{cls.attr.stallproductid} = :stallproductid, '\
                                        f'{cls.attr.productid} = :productid, '\
                                        f'{cls.attr.totalquantity} = if_not_exists({cls.attr.totalquantity}, :zero) + :incr',
                    'ExpressionAttributeValues': {
                        ":stallid": cls.attr.stallidTypeSetter(stallid),
                        ":productid": cls.attr.productidTypeSetter(productid),
                        ":stallproductid": cls.attr.stallproductidTypeSetter(stallproductid),
                        ":zero": cls.attr.totalquantityTypeSetter(0),
                        ":incr": cls.attr.totalquantityTypeSetter(quantity)
                    }
                }
            }
        ]
    
    @classmethod
    def offloadHandler(cls, user, stallid, productid, stallproductid, quantity, loaddatetime, stallinventoryid=None, storehouseid=Structure.DefaultStorehouseId, action=None):
        action = action or cls.DBInventoryAction.MOVEOUT
        stallinventoryid = getulid() if stallinventoryid is None else stallinventoryid
        pk = cls.attr.primaryKeySetter(user, stallid)
        put_sk = cls.attr.sortKeySetter(stallproductid, stallinventoryid)
        update_sk = cls.attr.metaSortKeySetter(stallproductid)
        return [
            {
                'Put': {
                    'TableName': cls.table,
                    'Item': {
                        **Structure.itemSetter(pk, put_sk),
                        **cls.attr.itemSetter(
                            stallproductid = stallproductid,
                            productid = productid,
                            quantity = -quantity,
                            datetime = loaddatetime,
                            action = action,
                            stallid = stallid,
                            stallinventoryid = stallinventoryid
                        )
                    }
                }
            },
            {
                'Update': {
                    'TableName': cls.table,
                    'Key': {
                        **Structure.itemSetter(pk, update_sk)
                    },
                    'UpdateExpression': f'SET {cls.attr.stallid} = :stallid, '\
                                        f'{cls.attr.stallproductid} = :stallproductid, '\
                                        f'{cls.attr.productid} = :productid, '\
                                        f'{cls.attr.totalquantity} = if_not_exists({cls.attr.totalquantity}, :zero) - :decr',
                    'ConditionExpression': f"{cls.attr.totalquantity} >= :decr",
                    'ExpressionAttributeValues': {
                        ":stallid": cls.attr.stallidTypeSetter(stallid),
                        ":productid": cls.attr.productidTypeSetter(productid),
                        ":stallproductid": cls.attr.stallproductidTypeSetter(stallproductid),
                        ":zero": cls.attr.totalquantityTypeSetter(0),
                        ":decr": cls.attr.totalquantityTypeSetter(quantity)
                    }
                }
            }
        ]
    
    @classmethod
    def movein(cls, user, stallid, productid, stallproductid, quantity, storehouseid=Structure.DefaultStorehouseId)->bool:
        stallinventoryid = getulid()
        if quantity <= 0:
            return False
        loaddatetime = getisodatetime()
        try:
            cls.client.transact_write_items(
                TransactItems = [
                    *cls.checkProductHandler(user, stallid, productid, stallproductid), 
                    *DBInventory.offloadHandler(user, storehouseid, productid, quantity, loaddatetime, DBInventory.DBInventoryAction.MOVEOUT, stallinventoryid),
                    *cls.loadHandler(user, stallid, productid, stallproductid, quantity, loaddatetime, stallinventoryid)
                ]
            )
        except ClientError as e:
            # print(e)
            return None
        return True
    
    @classmethod
    def getRawStallInventoryList(cls, user, stallid)-> dict[str, dict[str, DBStallInventory]]:
        pk = cls.attr.primaryKeySetter(user, stallid)
        response = cls.client.query(
            TableName = cls.table,
            IndexName = cls.storehouseInventory,
            KeyConditionExpression = f"{Structure.PrimaryKey} = :primarykey",
            ExpressionAttributeValues={
                ':primarykey': Structure.PrimaryKeyTypeSetter(pk)
            }
        )
        result = {}
        for item in response["Items"]:
            i = responseItemUnwrapper(item)
            quantity = int(i[cls.attr.totalquantity])
            if quantity <= 0:
                continue
            inventory = DBStallInventory(
                stallId = stallid,
                stallproductid = i[cls.attr.stallproductid],
                product = DBStallProduct(stallId=stallid, stallproductid = i[cls.attr.stallproductid], uid = i[cls.attr.productid]),
                quantity = quantity
            )
            result[i[cls.attr.productid]] = {
                i[cls.attr.stallproductid]: inventory
            }
        return result


    @classmethod
    def getStallInventoryList(cls, user, stallid)-> dict[str, dict[str, DBStallInventory]]:
        return cls.getRawStallInventoryList(user, stallid)
        
    def _load(self, quantity:int, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    def _offload(self, quantity:int, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    def _refresh(self, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    def refresh(self, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    def toDict(self):
        result = DBInventory.toDict(self)
        result["stallInventoryId"] = self.stallInventoryId
        result["stallId"] = self.stallId
        return result

    def getProduct(self):
        return _StallInventory.getProduct(self)
    
    def getRefID(self):
        return self.product.getRefID()

    def getInfo(self) -> str:
        info = f"Stall Inventory uid: {self.stallInventoryId}\n"
        info += _StallInventory.getInfo(self)
        return info

    def info(self) -> None:
        _StallInventory.info(self)

if __name__ == '__main__':

    # DBInventory.load("owner", 1, "95489b2b-88c2-4c96-b165-4383cb4b0aec", 5, getisodatetime(), "PUT")
    # DBInventory.load(
    #     user = "owner", 
    #     storehouseid = 1, 
    #     productid = "95489b2b-88c2-4c96-b165-4383cb4b0aec", 
    #     quantity = 1
    # )
    # DBInventory.offload(
    #     user = "owner", 
    #     storehouseid = 1, 
    #     productid = "95489b2b-88c2-4c96-b165-4383cb4b0aec", 
    #     quantity = 1
    # )

    # print(DBInventory.getProductInventoryList("owner", 1, "95489b2b-88c2-4c96-b165-4383cb4b0aec"))
    # for key, i in DBInventory.getInventoryList("owner", 1).items():
    #     i.info()
    # DBStallInventory.movein("owner", "01H673EJB3R988V58C6NV6ZGB9","95489b2b-88c2-4c96-b165-4383cb4b0aec" ,"69376545-5bc5-418c-862c-7b1fb0dd8cd9",1)
    inventoryList = DBStallInventory.getStallInventoryList("owner", "01H673EJB3R988V58C6NV6ZGB9")
    for inventorySet in inventoryList.values():
        for inventory in inventorySet.values():
            inventory.info()
    


    # print(DBInventory.offloadHandler("owner", 1, "95489b2b-88c2-4c96-b165-4383cb4b0aec", 5, getisodatetime(), "PUT"))