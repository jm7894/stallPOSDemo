from __future__ import annotations
from Services.Stall import Stall
from .DBProducts import DBProduct, DBStallProduct
from .DBInventory import DBInventory, DBStallInventory
from .DBStorehouse import DBStorehouse, DBStallStorehouse
from .DBTransaction import DBTransaction
# from .DBError import ValidationFailedError, OffloadQuantityError
import datetime
from .structure import Structure, dynamodb, getulid, getisodatetime, responseItemUnwrapper
from .base import notNone, toString, toFloat, notNegative, convertEmptyString, toString, batch_run, run, removeEmptyValues
from botocore.exceptions import ClientError

FILLUP = 40
SQLDATETIMEFORMAT = "%d/%m/%Y, %H:%M"

class DBStall(Stall):
    
    client = dynamodb()
    table = "Inventory"

    class attr:
        primaryKeySetter = lambda x, y: f"USER#{x}:Stall#{y}"
        primaryKeySorter = lambda x: f"USER#{x}:Stall#"
        stallIndexMetaSetter = lambda user: f"{user}.Stall"

        # sortKeyFormat = "STOREHOUSE#"
        # sortKeySetter =  lambda x: f"STOREHOUSE#{x}"
        meta = "#0"
        stallid = 'stallid'
        name = 'name'
        location = 'location'
        startdatetime = 'startdatetime'
        enddatetime = 'enddatetime'
        description = 'description'

        @classmethod
        def itemSetter(cls, **kwargs):
            result = {}
            name = kwargs.get("name")
            stallid = kwargs.get("stallid")
            description = kwargs.get("description")
            location = kwargs.get("location")
            startdatetime = kwargs.get("startdatetime")
            enddatetime = kwargs.get("enddatetime")
            metafield = kwargs.get("metafield")

            if name:
                result[cls.name] = {"S": name}
            if stallid:
                result[cls.stallid] = {"S": stallid}
            if description is not None:
                result[cls.description] = {"S": str(description)}
            if location:
                result[cls.location] = {"S": location}
            if startdatetime:
                result[cls.startdatetime] = {"S": getisodatetime(startdatetime)}
            if enddatetime:
                result[cls.enddatetime] = {"S": getisodatetime(enddatetime)}
            if metafield:
                result[Structure.MetaField] = Structure.MetaFieldTypeSetter(metafield)
            return result
        
        @classmethod
        def unwrapper(cls, item):
            return {
                cls.stallid: Structure.unwrapper(item[cls.stallid]),
                cls.name: Structure.unwrapper(item[cls.name]),
                cls.description: Structure.unwrapper(item[cls.description]),
                cls.location: Structure.unwrapper(item[cls.location]),
                cls.startdatetime: Structure.unwrapper(item[cls.startdatetime]),
                cls.enddatetime: Structure.unwrapper(item[cls.enddatetime]),
            }
    
    class cleaner:
        name = (notNone, toString)
        location = (notNone, toString)
        description = (convertEmptyString, toString)
        
        @classmethod
        def clean(cls, name, location, description):
            cleaned = batch_run((name, *cls.name), (location, *cls.location), (description, *cls.description))
            if cleaned:
                return {
                    "name": cleaned[0],
                    "location": cleaned[1],
                    "description": cleaned[2]
                }
            else:
                return None

    def __init__(
            self,
            uid:str = None,
            user:str = None,
            **kwargs
    ):
        stall = kwargs.get("stall")
        self._uid = uid
        self.user = user
        super().__init__(**kwargs)
        storehouse = kwargs.get("storehouse")
        if isinstance(storehouse, DBStorehouse):
            self._uid = storehouse._uid if self._uid is None else self._uid
            self.user = storehouse.user if self.user is None else self.user
        self.storehouse:DBStallStorehouse = DBStallStorehouse()

    
    @classmethod
    def create(cls, user, name, description, start, end, location):
        cleaned = cls.cleaner.clean(name, location, description)
        if not cleaned:
            raise ValueError
        uid = getulid()
        pk = cls.attr.primaryKeySetter(user, uid)
        sk = cls.attr.meta
        # new stall
        try:
            cls.client.put_item(
                TableName = cls.table,
                Item = {
                    **Structure.itemSetter(pk, sk),
                    **cls.attr.itemSetter(
                        stallid = uid,
                        name = cleaned["name"],
                        location = cleaned["location"],
                        description = cleaned["description"],
                        startdatetime = start,
                        enddatetime = end,
                        metafield = cls.attr.stallIndexMetaSetter(user)
                    ), 
                }
            )
        except ClientError as e:
            # print(e)
            return None
        return DBStall(uid, user, name=cleaned["name"], location=cleaned["location"], description=cleaned["description"], startDatetime=start, endDatetime=end)

    @classmethod
    def loadStall(cls, user, stallid)->DBStall:
        pk = cls.attr.primaryKeySetter(user, stallid)
        sk = cls.attr.meta
        response = cls.client.query(
            TableName = cls.table,
            KeyConditionExpression = f"{Structure.PrimaryKey} = :primarykey AND {Structure.SortKey} = :sortkey",
            ExpressionAttributeValues={
                ':primarykey': Structure.PrimaryKeyTypeSetter(pk),
                ':sortkey': Structure.SortKeyTypeSetter(sk)
            }
        )
        if not response["Items"]:
            return None
        item = response["Items"][0]
        i = cls.attr.unwrapper(item)
        return DBStall(
            uid = i[cls.attr.stallid],
            user = user,
            name = i[cls.attr.name],
            description = i[cls.attr.description],
            location = i[cls.attr.location],
            startDatetime = i[cls.attr.startdatetime],
            endDatetime = i[cls.attr.enddatetime],
        )


    def refresh(self, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    def refreshInventoryList(self,connection:Connection=None, cur:Cursor=None)->dict[int,DBStallInventory]:
        self.storehouse.refreshInventoryList(connection, cur)
        return self.storehouse.getInventoryList()
    
    @staticmethod
    def movein(**kwargs):
        DBStallStorehouse.movein(**kwargs)

    def getMyInventoryList(self)->set(dict[str, DBProduct], dict[str, dict[str,DBStallProduct]], dict[str, dict[str,DBStallInventory]]):
        return self.getStallInventoryList(self.user, self._uid)

    def getMyProductWithQuantityList(self)->set(dict[str, DBProduct], dict[str, dict[str,DBStallInventory]]):
        return self.getStallProductWithQuantityList(self.user, self._uid)

    @staticmethod
    def getStallInventoryList(user, stallid)->set(dict[str, DBProduct], dict[str, dict[str,DBStallProduct]],dict[str, dict[str,DBStallInventory]]):
        stallRawInventoryList = DBStallInventory.getRawStallInventoryList(user, stallid)
        productList, stallProductList = DBStallProduct.getStallProductList(user, stallid)
        masterDeleteList = list()
        for productid, stallInventorys in stallRawInventoryList.items():
            subDeleteList = list()
            for stallproductid, stallInventory in stallInventorys.items():
                stallproducts = stallProductList.get(productid)
                stallproduct = stallproducts.get(stallproductid) if stallproducts else None
                if stallproduct:
                    stallInventory.product.merge(stallproduct)
                else:
                    subDeleteList.append((productid, stallproductid))
            if len(stallRawInventoryList[productid]) == len(subDeleteList):
                subDeleteList.append((productid, None))
            masterDeleteList + subDeleteList
        
        for productid, stallproductid in masterDeleteList:
            if stallproductid:
                del stallRawInventoryList[productid][stallproductid]
            else:
                del stallRawInventoryList[productid]
            
        return productList, stallProductList, stallRawInventoryList
    
    @staticmethod
    def getStallProductWithQuantityList(user, stallid)->set(dict[str, DBProduct], dict[str, dict[str,DBStallInventory]]):
        stallRawInventoryList = DBStallInventory.getRawStallInventoryList(user, stallid)
        productList, stallProductList = DBStallProduct.getStallProductList(user, stallid)
        for productid, stallProducts in stallProductList.items():
            for stallproductid, stallProduct in stallProducts.items():
                stallInventorys = stallRawInventoryList.get(productid)
                stallInventory = stallInventorys.get(stallproductid) if stallInventorys else None
                if stallInventory:
                    stallInventory.product.merge(stallProduct)
                else:
                    stallRawInventoryList[productid] = stallRawInventoryList.get(productid) or {}
                    stallRawInventoryList[productid][stallproductid] = DBStallInventory(stallid, product=stallProduct, quantity=0)
        return productList, stallRawInventoryList

    @classmethod
    def getStallList(cls, user) -> list[str, DBStall]:
        pk_sorter = cls.attr.primaryKeySorter(user)
        sk = Structure.META
        response = cls.client.query(
            TableName = cls.table,
            IndexName = Structure.MetaIndex,
            KeyConditionExpression = f"{Structure.MetaField} = :primarykey AND begins_with({Structure.PrimaryKey}, :sortkey)",
            ExpressionAttributeValues = {
                ':primarykey': Structure.MetaFieldTypeSetter(cls.attr.stallIndexMetaSetter(user)),
                ':sortkey': Structure.PrimaryKeyTypeSetter(cls.attr.primaryKeySorter(user))
            }
        )
        rows = [responseItemUnwrapper(item) for item in response["Items"]]
        stallList = dict()
        for row in rows:
            stall = DBStall(
                uid = row[cls.attr.stallid],
                user = user,
                name  = row[cls.attr.name],
                description = row[cls.attr.description],
                location = row[cls.attr.location],
                startDatetime = row[cls.attr.startdatetime],
                endDatetime = row[cls.attr.enddatetime]
            ) 
            stallList[row[cls.attr.stallid]] = stall
        return stallList
    
    @staticmethod
    def addStallProduct(user, stallId:int, productId:int, settingPrice:int)->DBStallProduct:
        return DBStallProduct.new(
            user, stallId, productId, settingPrice
        )

    @staticmethod
    def batchLoadStallProduct(user, stallid, productStallProductQuantityList:list[set[str, str, int]]):
        return DBStallStorehouse.batchMovein(
            user = user,
            stallid = stallid,
            productStallProductQuantityList = productStallProductQuantityList
        )

    def toDict(self):
        return {
            "uid": self._uid,
            "name": self.name,
            "location": self.location,
            "startDatetime": self.startDatetime,
            "endDatetime": self.endDatetime,
            "description": "" if self.description is None else self.description
        }

    def createStorehouse(self, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    def prepare(self, set, connection:Connection=None, cur:Cursor=None):
        return NotImplemented

    @staticmethod
    def transact(user, stallid, stallProductQuantityPriceList:list[set[str, str, int, float]])->DBTransaction:
        return DBTransaction.transact(user, stallid, stallProductQuantityPriceList)

    def stocking(self):
        return NotImplemented
    
    def isSameStall(self, other):
        return NotImplemented
        return super().isSameStall(other)
    
    def inventoryInfo(self):
        return NotImplemented
        return super().inventoryInfo()
    
    def transactionsInfo(self):
        return NotImplemented
        for t in self.transactionList:
            t.info()
            t.detialsInfo()
    
    def updateName(self, name:str):
        self.name = name
        return
        self.storehouse.updateName(name+"'s storehouse")

    def getInfo(self):
        return f"stall uid: {self._uid}\n"+super().getInfo()
    
    def getUID(self):
        return self._uid

class StallIDError(Exception):
    pass

if __name__ == '__main__':
    # stall = DBStall.create("owner", "test2", "", getisodatetime(), getisodatetime(), "KT")
    # stall = DBStall.loadStall("owner", "01H654NJQ67JXB22NSZX2FY57Q")
    # stall.info()
    stalllist = DBStall.getStallList("owner")
    [stall.info() for stall in stalllist]
    # DBStall.addStallProduct("owner", "01H672REKFFVD80WCB5GVXCXMW", "95489b2b-88c2-4c96-b165-4383cb4b0aec", 80)

        

