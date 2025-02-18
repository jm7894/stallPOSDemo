from __future__ import annotations
from Services.Products import _StallProduct, TransactionProduct, Product
from .DBError import DBSelectError, DBInsertError, DBUpdateError, DBDeleteError, AlreadyExistError
from .structure import dynamodb, Structure, getisodatetime, getuuid, responseItemUnwrapper
from .base import notNone, toString, toFloat, notNegative, convertEmptyString, toString, batch_run, run, removeEmptyValues
from decimal import Decimal
from botocore.exceptions import ClientError

FILLUP = 40
PRODUCTNAME = "name"
PRODUCTCOST = "cost"
PRODUCTDESCRIPTION = "description"
PRODUCTID = "id"

class DBProduct(Product):
    client = dynamodb()
    table = "Inventory"
    
    class cleaner:
        name = (notNone, toString)
        cost = (toFloat, notNegative)
        description = (convertEmptyString, toString)
        
        @classmethod
        def clean(cls, name, cost, description):
            cleaned = batch_run((name, *cls.name), (cost, *cls.cost), (description, *cls.description))
            if cleaned:
                return {
                    "name": cleaned[0],
                    "cost": Decimal(cleaned[1]),
                    "description": cleaned[2]
                }
            else:
                return None

    class attr:
        productid = 'productid'
        name = 'name'
        description = 'description'
        cost = 'cost'
        datetime = 'createdatetime'
        primaryKeySetter = lambda user: f"USER#{user}:Product"
        sortKeySetter = lambda productid: f"#{productid}"

        @classmethod
        def itemSetter(cls, **kwargs):
            result = {}
            productid = kwargs.get("productid")
            name = kwargs.get("name")
            cost = kwargs.get("cost")
            description = kwargs.get("description")
            datetime = kwargs.get("datetime")
            if productid:
                result[cls.productid] = {"S": productid}
            if name:
                result[cls.name] = {"S": name}
            if cost or cost == 0:
                result[cls.cost] = {"N": str(cost)}
            if description is not None:
                result[cls.description] = {"S": description}
            if datetime:
                result[cls.datetime] = {"S": getisodatetime(datetime)}
            return result

    def __init__(self, uid:int=None, datetime=None, **kwargs):
        super().__init__(**kwargs)
        product = kwargs.get("product")
        self._uid = uid
        self.datetime = datetime
        if isinstance(product, DBProduct):
            self._uid = product._uid if not self._uid else self._uid
            self.datetime = product.datetime if not self.datetime else self.datetime

    # @classmethod
    # def getmeta(cls, user):
    #     cls.client.query(
    #         TableName = cls.table,
    #         ExpressionAttributeValues={
    #             ':pk': {
    #                 Structure.PrimaryKeyType: f'USER#{owner}:PRODUCT',
    #             },
    #             ':sk': {
    #                 Structure.SortKeyType: '#0',
    #             }
    #         },
    #         KeyConditionExpression = f'{Structure.PrimaryKey} = :pk AND {Structure.SortKey} = :sk',
    #         ProjectionExpression = 'productcount'
    #     )

    @classmethod
    def new(cls, user, name, cost, description):
        cleaned = cls.cleaner.clean(name, cost, description)
        if not cleaned:
            raise ValueError
        pk = cls.attr.primaryKeySetter(user)
        uid = getuuid()
        sk = "#"+uid
        # new product
        cls.client.put_item(
            TableName = cls.table,
            Item = {
                **Structure.itemSetter(pk, sk),
                **cls.attr.itemSetter(
                    productid = uid,
                    name = cleaned["name"],
                    cost = cleaned["cost"],
                    description = cleaned["description"],
                    datetime = getisodatetime()
                )
            }
        )

        return DBProduct(uid, name=cleaned["name"], cost=cleaned["cost"], description=cleaned["description"])

    @classmethod
    def getProductList(cls, user)-> dict[str, DBProduct]:
        response = cls.client.query(
            TableName = cls.table,
            KeyConditionExpression = f"{Structure.PrimaryKey} = :primarykey",
            ExpressionAttributeValues={
                ':primarykey': {Structure.PrimaryKeyType: cls.attr.primaryKeySetter(user)},
            }
        )
        items = response["Items"]
        result = {}
        for item in items:
            i = responseItemUnwrapper(item)
            result[i[cls.attr.productid]] = DBProduct(
                            uid = i[cls.attr.productid], 
                            datetime = i[cls.attr.datetime], 
                            name = i[cls.attr.name], 
                            description = i[cls.attr.description], 
                            cost = i[cls.attr.cost]
                        )
        return result


    # (NEW) Refresh this Product's metadata by it's uid
    # Raises Data.DBError.DBSelectError if its uid doesn't exist in Database
    # Raises TypeError if self._uid is not an int
    def refresh(self, connection:Connection=None, cur:Cursor=None)->None:
        # query = f"SELECT * FROM Product WHERE {PRODUCTID}={self._uid}"
        # cur = connection.cursor()
        # cur.execute(query)
        # result = cur.fetchone()
        # if result is None:
        #     raise DBUpdateError(f"Product with uid {self._uid} not exists!")
        # id, name, price, description = result
        # self.name = name
        # self.price = price
        # self.description = description
        return NotImplemented
        if type(self._uid) is not int:
            raise TypeError(f"{self._uid} is not int")
        product = self.loadProduct(self._uid, connection, cur)
        self.name, self.cost, self.description = product.name, product.cost, product.description

    def delete(self, connection:Connection=None, cur:Cursor=None) -> None:
        return NotImplemented
    

    def update(self, connection:Connection=None, cur:Cursor=None)->None:
        return NotImplemented
        
    def getUID(self)->int:
        return self._uid

    def getDBinfo(self):
        return f"uid: {self._uid}\n"\
               f"createdDatetime: {self.datetime}\n"

    def getInfo(self):
        return  self.getDBinfo() + Product.getInfo(self)

    def isSameProduct(self, other: DBProduct) -> bool:
        return self._uid == other.getUID()
    
    @staticmethod
    def loadProduct(uid:int,connection:Connection=None, cur:Cursor=None) -> DBProduct:
        return NotImplemented
        query = "SELECT * FROM Product "\
                f"WHERE {PRODUCTID}={uid}"
        if not cur:
            cur = connection.cursor()
        cur.execute(query)
        result = cur.fetchone()
        if result is None:
            raise DBSelectError(f"Product with uid {uid} doesn't exist")
        _, name, cost, description = result
        return DBProduct(uid, name=name, cost=cost, description=description)
    
    def _isSameProduct(self, product:DBProduct) -> bool:
        a = (self._uid, self.name, self.cost, self.description) 
        b = (product._uid, product.name, product.cost, product.description)
        # merge = lambda xs, ys: [xs[0] or None] + merge(ys, xs[1:]) if xs else ys
        return all(list(map(lambda x, y: (x or None) == (y or None),a, b )))
    
    def merge(self, product:DBProduct):
        productDict = vars(product)
        self._uid =  self._uid or productDict.get("uid")
        self.name = self.name or productDict.get("name")
        self.cost = self.cost or productDict.get("cost")
        self.description = self.description or productDict.get("description")
        self.datetime = self.datetime or productDict.get("datetime")

    def toDict(self):
        return {
            "uid": self._uid,
            "name": self.name,
            "cost": self.cost,
            "description": "" if self.description is None else self.description,
            "createdDatetime": self.datetime
        }


class DBStallProduct(_StallProduct, DBProduct):
    client = dynamodb()
    table = "Inventory"

    class attr:
        productid = 'productid'
        stallid = 'stallid'
        name = 'name'
        description = 'description'
        cost = 'cost'
        datetime = 'createdatetime'
        settingprice = 'settingprice'
        stallproductname = "stallproductname"
        stallproductid, stallproductidTypeSetter = 'stallproductid', Structure.StringTypeSetter
        primaryKeySetter = lambda user, stallid: f"USER#{user}:Stall#{stallid}"
        sortKeySetter = lambda stallproductid: f"StallProduct#{stallproductid}"
        sortKeyFormat = "StallProduct#"

        @classmethod
        def itemSetter(cls, **kwargs):
            result = {}
            productid = kwargs.get("productid")
            stallid = kwargs.get("stallid")
            name = kwargs.get("name")
            cost = kwargs.get("cost")
            description = kwargs.get("description")
            settingprice = kwargs.get("settingprice")
            datetime = kwargs.get("datetime")
            stallproductname = kwargs.get("stallproductname")
            stallproductid = kwargs.get('stallproductid')

            if stallproductid:
                result[cls.stallproductid] = cls.stallproductidTypeSetter(stallproductid)
            if productid:
                result[cls.productid] = {"S": productid}
            if stallid:
                result[cls.stallid] = {"S": stallid}
            if name:
                result[cls.name] = {"S": name}
            if cost or cost == 0:
                result[cls.cost] = {"N": str(cost)}
            if description is not None:
                result[cls.description] = {"S": description}
            if datetime:
                result[cls.datetime] = {"S": getisodatetime(datetime)}
            if settingprice or settingprice == 0:
                result[cls.settingprice] = {"N": str(settingprice)}
            if stallproductname is None or stallproductname == "":
                result[cls.stallproductname] = {"NULL": True}
            else:
                result[cls.stallproductname] = {"N": stallproductname}

            return result

    def __init__(
            self,
            stallId:str=None,
            stallProductId:str=None,
            stallProductName=None,
            **kwargs
        ):
            self.stallId = stallId 
            self.stallProductName=stallProductName
            self.stallProductId = stallProductId
            super().__init__(**kwargs)
            product = kwargs.get("product")
            if isinstance(product, DBStallProduct):
                self.stallId = self.stallId or product.stallId
                self.stallProductName = self.stallProductName or product.stallProductName
                self.stallProductId = self.stallProductId or product.stallProductId
            self.stallProductName = self.stallProductName or self.name
            

    def getDBinfo(self):
        return  f"stallProductId: {self.stallProductId}\n"\
                f"stallId: {self.stallId}\n"\
                f"stallProductName: {self.stallProductName}\n" + DBProduct.getDBinfo(self)

    def getInfo(self) -> str:
        return _StallProduct.getInfo(self)
    
    def getUID(self) -> int:
        return DBProduct.getUID(self)

    @classmethod
    def new(cls, user, stallid, productid, settingprice, stallproductname=None)->DBStallProduct:
        settingprice = run(settingprice, toFloat, notNegative)
        if not settingprice and settingprice != 0:
            return None
        check_product_pk = DBProduct.attr.primaryKeySetter(user)
        check_product_sk = DBProduct.attr.sortKeySetter(productid)
        pk = cls.attr.primaryKeySetter(user, stallid)
        stallproductid = getuuid()
        sk = cls.attr.sortKeySetter(stallproductid)
        # new product
        try:
            cls.client.transact_write_items(
                TransactItems = [
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
                                **Structure.itemSetter(pk, Structure.META)
                            },
                            'ConditionExpression': f"attribute_exists({Structure.PrimaryKey})"
                        }
                    },
                    {
                        'Put': {
                            'TableName': cls.table,
                            'Item': {
                                **Structure.itemSetter(pk, sk),
                                **cls.attr.itemSetter(
                                    productid = productid,
                                    stallid = stallid,
                                    stallproductid = stallproductid,
                                    settingprice = settingprice,
                                    stallproductname = stallproductname,
                                    datetime = getisodatetime()
                                )
                            }
                        }
                    }
                ]
            )
        except ClientError as e:
            # print(e)
            return None
        return DBStallProduct(stallid, stallproductid, stallproductname, uid=productid)
        

    @classmethod
    def loadProduct(cls, user, stallid, stallproductid) -> DBStallProduct:
        pk = cls.attr.primaryKeySetter(user, stallid)
        sk = cls.attr.sortKeySetter(stallproductid)
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
        i = responseItemUnwrapper(item)
        return DBStallProduct(
            stallId = stallid,
            stallProductId = stallproductid,
            stallProductName = i[cls.attr.stallproductname],
            settingprice = i[cls.attr.settingprice],
            uid = i[cls.attr.productid],
            datetime = i[cls.attr.datetime]
        )

    @classmethod
    def getRawStallProductList(cls, user, stallid) -> dict[str, dict[str,DBStallProduct]]:
        pk = cls.attr.primaryKeySetter(user, stallid)
        sk = cls.attr.sortKeyFormat
        response = cls.client.query(
            TableName = cls.table,
            KeyConditionExpression = f"{Structure.PrimaryKey} = :primarykey AND begins_with({Structure.SortKey}, :sortkey)",
            ExpressionAttributeValues={
                ':primarykey': Structure.PrimaryKeyTypeSetter(pk),
                ':sortkey': Structure.SortKeyTypeSetter(sk)
            }
        )
        result = {}
        for item in response["Items"]:
            i = responseItemUnwrapper(item)
            product = DBStallProduct(
                stallId = stallid,
                stallProductId = i[cls.attr.stallproductid],
                stallProductName = i[cls.attr.stallproductname],
                settingPrice = i[cls.attr.settingprice],
                uid = i[cls.attr.productid],
                datetime = i[cls.attr.datetime]
            )
            result[i[cls.attr.productid]] = {
                i[cls.attr.stallproductid]: product
            }
        return result
    
    @classmethod
    def getStallProductList(cls, user, stallid) -> set[dict[str, DBProduct], dict[str, dict[str, DBStallProduct]]]:
        stallProductList = cls.getRawStallProductList(user, stallid)
        productList = DBProduct.getProductList(user)
        masterDeleteList = list()
        for productid, stallProducts in stallProductList.items():
            for stallproductid, stallProduct in stallProducts.items():
                subDeleteList = list()
                product = productList.get(productid)
                if product:
                    stallProduct.merge(product)
                else:
                    subDeleteList.append((productid, stallproductid))
                    
            if len(subDeleteList) == len(stallProductList[productid]):
                subDeleteList.append((productid,None))
            masterDeleteList + subDeleteList
        for productid, stallproductid in masterDeleteList:
            if stallproductid:
                del stallProductList[productid][stallproductid]
            else:
                del stallProductList[productid]
        return productList, stallProductList
        

    def merge(self, product:DBProduct):
        productDict = vars(product)
        self.stallId =  self.stallId or productDict.get("stallId")
        if not self.settingPrice and self.settingPrice != 0:
            self.settingPrice = productDict.get("settingPrice")
        self.stallProductName = self.stallProductName or productDict.get("stallProductName")
        self.stallProductId = self.stallProductId or productDict.get("stallProductId")
        DBProduct.merge(self, product)
        self.stallProductName = self.stallProductName or self.name
        
        

    def refresh(self):
        return NotImplemented

    def delete(self, connection:Connection=None, cur:Cursor=None) -> None:
        return NotImplemented

    def update(self, connection:Connection=None, cur:Cursor=None)->None:
        return NotImplemented

    def updateProduct(self, connection:Connection=None, cur:Cursor=None)->None:
        return NotImplemented
        DBProduct.update(self,connection, cur)
    
    def toDict(self):
        result = DBProduct.toDict(self)
        result["stallProductId"] = self.stallProductId
        result["stallId"] = self.stallId
        result["settingPrice"] = self.settingPrice
        result["stallProductName"] = self.stallProductName
        return result

class DBTransactionProduct(TransactionProduct, DBStallProduct):

    client = dynamodb()
    table = "Inventory"

    class attr(DBStallProduct.attr):
        transactionitemid = 'transactionitemid'
        quantity = 'quantity'
        sellingprice = 'sellingprice'
        stalliventoryid = 'stallinventoryid'
        transactionid = 'transactionid'

        sortKeySetter = lambda transactionid, itemNumber: f"Transaction#{transactionid}-Item#{itemNumber}"
        sortKeyFormat = lambda transactionid: f"Transaction#{transactionid}-Item#"

        @classmethod
        def itemSetter(cls, **kwargs):
            result = DBStallProduct.attr.itemSetter(**kwargs)

            transactionitemid = kwargs.get("transactionitemid")
            quantity = kwargs.get("quantity")
            sellingprice = kwargs.get("sellingprice")
            stalliventoryid = kwargs.get("stalliventoryid")
            transactionid = kwargs.get("transactionid")

            if transactionitemid:
                result[cls.transactionitemid] = {"S": transactionitemid}
            if quantity:
                result[cls.quantity] = {"N": str(quantity)}
            if sellingprice or sellingprice == 0:
                result[cls.sellingprice] = {"N": str(sellingprice)}
            if stalliventoryid:
                result[cls.stalliventoryid] = {"S": stalliventoryid}
            if transactionid:
                result[cls.transactionid] = {"S": transactionid}
            return result

    def __init__(
            self,
            transactionId:str = None,
            quantity:int = None,
            sellingPrice:int = None,
            stallInventoryId:str = None,
            transactionProductId:str=None,
            transactDatetime:str=None,
            **kwargs
        ):
        super().__init__(**kwargs)
        self.transactionId = transactionId
        self.quantity = quantity
        self.sellingPrice = sellingPrice
        self.stallInventoryId = stallInventoryId
        self.transactionProductId = transactionProductId
        self.transactDatetime = transactDatetime

        product = kwargs.get("product")
        if isinstance(product, DBTransactionProduct):
            self.transactionId = transactionId or product.transactionId
            self.quantity = quantity or product.quantity
            self.sellingPrice = sellingPrice or product.sellingPrice
            self.stallInventoryId = stallInventoryId or product.stallInventoryId
            self.transactionProductId = transactionProductId or product.stallInventoryId
            self.transactDatetime = transactDatetime or product.transactDatetime

    @classmethod
    def addTransactionItemHandler(cls, user, stallid, transactionid, stallproductid, productid, itemNumber, quantity, sellingprice, stallinventoryid):
        pk = cls.attr.primaryKeySetter(user, stallid)
        sk = cls.attr.sortKeySetter(transactionid, itemNumber)
        transactionitemid = f"{transactionid}#{itemNumber}"
        return [
            {
                'Put': {
                    'TableName': cls.table,
                    'Item': {
                        **Structure.itemSetter(pk, sk),
                        **cls.attr.itemSetter(
                            stallid = stallid,
                            transactionid = transactionid,
                            quantity = quantity,
                            sellingprice = sellingprice,
                            stallinventoryid = stallinventoryid,
                            transactionitemid = transactionitemid,
                            stallproductid = stallproductid,
                            productid = productid
                        )
                    }
                }
            }
        ]

    def getInfo(self) -> str:
        info = f"transactionId: {self.transactionId}\n"\
                f"stallInventoryId: {self.stallInventoryId}\n"\
                f"transactionProductId: {self.transactionProductId}\n"\
                f"transactDatetime: {self.transactDatetime}\n"
        return info+TransactionProduct.getInfo(self)
    
    def toDict(self):
        result = DBStallProduct.toDict(self)
        result["transactionId"] = self.transactionId
        result["quantity"] = self.quantity
        result["sellingPrice"] = self.sellingPrice
        result["transactionProductId"] = self.transactionProductId
        result["stallInventoryId"] = self.stallInventoryId
        result["transactDatetime"] = self.transactDatetime
        return result

if __name__ == '__main__':
    # product = DBProduct.new("owner", "candleA", 10, "TEST")
    # print(product)

    # productList = DBProduct.getProductList("owner")
    # for key, product in productList.items():
    #     product.info()
    # DBStallProduct.new("owner", "01H673EJB3R988V58C6NV6ZGB9", "95489b2b-88c2-4c96-b165-4383cb4b0aec", 80)
    stallproduct = DBStallProduct.loadProduct("owner", "01H673EJB3R988V58C6NV6ZGB9", "69376545-5bc5-418c-862c-7b1fb0dd8cd9")
    stallproduct.info()
    stallproductList = DBStallProduct.getStallProductList("owner","01H673EJB3R988V58C6NV6ZGB9")
    for stallproducts in stallproductList.values():
        for stallproduct in stallproducts.values():
            stallproduct.info()
    # productlist = DBStallProduct.getStallProductList("owner", "01H673EJB3R988V58C6NV6ZGB9")
    # productlist['95489b2b-88c2-4c96-b165-4383cb4b0aec']['69376545-5bc5-418c-862c-7b1fb0dd8cd9'].info()