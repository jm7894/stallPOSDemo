from __future__ import annotations
from Services.Transaction import Transaction
from .DBProducts import DBTransactionProduct, DBStallProduct, DBProduct
from Services.Products import TransactionProduct
from .DBInventory import DBStallInventory
import datetime
from .structure import dynamodb, Structure, getisodatetime, getuuid, responseItemUnwrapper, getulid
from .base import notNone, toString, toFloat, notNegative, convertEmptyString, toString, batch_run, run, removeEmptyValues
from botocore.exceptions import ClientError


class DBTransaction(Transaction):
    client = dynamodb()
    table = "Inventory"

    class attr:
        primaryKeySetter = lambda user, stallid: f"USER#{user}:Stall#{stallid}"
        transactionSortKeySetter = lambda transactionid: f"Transaction#{transactionid}"
        transactionItemSortKeySetter = lambda transactionid, itemNumber: f"Transaction#{transactionid}-Item#{itemNumber}"
        transactionIndexMetaSetter = lambda user: f"{user}.Transaction"

        
        transactionid, transactionidTypeSetter = 'transactionid', Structure.StringTypeSetter
        stallid, stallidTypeSetter = 'stallid', Structure.StringTypeSetter
        productid, productidTypeSetter = 'productid', Structure.StringTypeSetter
        stallproductid, stallproductidTypeSetter = DBStallProduct.attr.stallproductid, DBStallProduct.attr.stallproductidTypeSetter
        datetime, datetimeTypeSetter = 'datetime', Structure.StringTypeSetter
        totalprice, totalpriceTypeSetter = 'totalprice', Structure.NumberTypeSetter
        metafield, metafieldTypeSetter = Structure.MetaField, Structure.MetaFieldTypeSetter

        @classmethod
        def itemSetter(cls, **kwargs):
            result = {}
            transactionid = kwargs.get("transactionid")
            productid = kwargs.get("productid")
            stallproductid = kwargs.get("stallproductid")
            stallid = kwargs.get("stallid")
            totalprice = kwargs.get("totalprice")
            datetime = kwargs.get("datetime")
            metafield = kwargs.get("metafield")

            if transactionid:
                result[cls.transactionid] = cls.transactionidTypeSetter(transactionid)
            if stallid:
                result[cls.stallid] = cls.stallidTypeSetter(stallid)
            if productid:
                result[cls.productid] = cls.productidTypeSetter(productid)
            if totalprice or totalprice == 0:
                result[cls.totalprice] = cls.totalpriceTypeSetter(totalprice)
            if datetime:
                result[cls.datetime] = cls.datetimeTypeSetter(getisodatetime(datetime))
            if stallproductid:
                result[cls.stallproductid] = cls.stallproductidTypeSetter(stallproductid)
            if metafield:
                result[cls.metafield] = cls.metafieldTypeSetter(metafield)
            return result

    def __init__(
            self,
            transaction:Transaction = None,
            stallId:int = None,
            uid:int = None,
            transactionDatetime=getisodatetime()
    ):
        if transaction:
            Transaction.__init__(self, transaction.transactionDatetime)
        else:
            Transaction.__init__(self, transactionDatetime)
        self._uid = uid
        self.stallId = stallId

    def refreshItemList(self, connection:Connection=None, cur:Cursor=None):
        self._productList = []
        query = "SELECT transactionId, assignId, TI.referenceId, quantity, price, settingPrice, Product.id, name, cost, description FROM "\
                "TransactionItem AS TI LEFT JOIN StallProduct ON StallProduct.referenceId = TI.referenceId "\
                "LEFT JOIN Product ON Product.id = StallProduct.productId "\
                f"WHERE transactionId = {self._uid}"
        if not cur:
            cur = connection.cursor()
        cur.execute(query)
        results = cur.fetchall()
        if not results:
            raise NotExistError(f"Transaction with id {self._uid} doesn't exist")
        for result in results:
            transactionId, assignId, referenceId, quantity, sellingPrice, settingPrice, productId, name, cost, description = result
            stallProduct = DBStallProduct(self.stallId, referenceId=referenceId, settingPrice=settingPrice, product=DBProduct(productId, name=name, cost=cost, description=description))
            transactionProduct = DBTransactionProduct(transactionId, assignId, quantity=quantity, sellingPrice=sellingPrice, product=stallProduct)
            self._productList.append(transactionProduct)


    @staticmethod
    def load(id:int, connection:Connection=None, cur:Cursor=None) -> DBTransaction:
        query = f"SELECT * FROM \'Transaction\' WHERE id = {id}"
        if not cur:
            cur = connection.cursor()
        cur.execute(query)
        result = cur.fetchone()
        if not result:
            raise NotExistError(f"Transaction with id {id} doesn't exist")
        id, transactionDatetime, stallId = result
        return DBTransaction(stallId=stallId, uid=id, transactionDatetime=transactionDatetime)
    
    @staticmethod
    def loadTransactionList(stallId:int, connection:Connection=None, cur:Cursor=None) -> dict[int,DBTransaction]:
        query = "SELECT \'Transaction\'.id, transactionDatetime, quantity, TransactionItem.price ,Product.id, Product.name, Product.cost, Product.description FROM TransactionItem "\
                "LEFT JOIN \'Transaction\' ON TransactionItem.transactionId = \'Transaction\'.id "\
                "LEFT JOIN StallProduct ON StallProduct.referenceId = TransactionItem.referenceId "\
                "LEFT JOIN Product ON Product.id = StallProduct.productId "\
                f"WHERE \'Transaction\'.stallId = {stallId} "
        if not cur:
            cur = connection.cursor()
        cur.execute(query)
        result = cur.fetchall()
        transactions = {}
        for i in result:

            transactionId, transactionDatetime, quantity, sellingPrice, productId, productName, productCost, description  = i
            transaction = transactions.get(transactionId)
            if not transaction:
                transaction = DBTransaction(stallId=stallId, uid=transactionId, transactionDatetime=transactionDatetime)
                transactions[transactionId] = transaction
            transaction._setProduct(DBTransactionProduct(transactionId=transactionId, quantity=quantity, sellingPrice=sellingPrice, uid=productId, name=productName, cost=productCost, description=description))
        return transactions

    def init(self, connection:Connection=None, cur:Cursor=None):
        query = insert(
            "\'Transaction\'",
            ("transactionDatetime", "stallId")
        )
        if not cur:
            cur = connection.cursor()
        data = (self.transactionDatetime, self.stallId)
        # print(query)
        cur.execute(query, data)
        self._uid = cur.lastrowid

    def _setProduct(self, product: DBTransactionProduct):
        self._productList.append(product)
    
    def addProduct(self, product: DBStallProduct, price: int, quantity: int):
        transactionProduct = DBTransactionProduct(transactionId=self._uid, quantity=quantity, sellingPrice=price, product=product)
        self._productList.append(transactionProduct)

    @classmethod
    def transactionHandler(cls, user, stallid, totalprice, datetime, transactionid=None):
        transactionid = transactionid or getulid()
        pk = cls.attr.primaryKeySetter(user,stallid)
        sk = cls.attr.transactionSortKeySetter(transactionid)
        return [
            {
                'Put': {
                    'TableName': cls.table,
                    'Item': {
                        **Structure.itemSetter(pk, sk),
                        **cls.attr.itemSetter(
                            stallid = stallid,
                            transactionid = transactionid,
                            datetime = datetime,
                            totalprice = totalprice,
                            metafield = cls.attr.transactionIndexMetaSetter(user)
                        )
                    }
                }
            }
        ]


    # Transaction
    # DBStallInventory.offloadhandler
    @classmethod
    def transact(cls, user, stallid, stallProductQuantityPriceList:list[set[str, str, int, float]])->DBTransaction:
        now = getisodatetime()
        transactionid = getulid()
        items = list()
        totalprice = 0
        itemNumber = 1
        for stallproductid, productid, quantity, sellingprice in stallProductQuantityPriceList:
            stallinventoryid = getulid()
            items.extend(DBStallInventory.checkProductHandler(user, stallid, productid, stallproductid))
            items.extend(DBStallInventory.offloadHandler(
                user, stallid, productid, stallproductid, quantity, now, stallinventoryid, 
                action=DBStallInventory.DBInventoryAction.SOLD
                )
            )
            #itemhandler
            items.extend(DBTransactionProduct.addTransactionItemHandler(user, stallid, transactionid, stallproductid, productid, itemNumber, quantity, sellingprice, stallinventoryid))
            totalprice += quantity*sellingprice
            itemNumber += 1
        items.extend(DBTransaction.transactionHandler(user, stallid, totalprice, now, transactionid))
        try:
            return cls.client.transact_write_items(TransactItems = items)
        except ClientError as e:
            # print(e)
            return None


        
        items.extend(DBTransaction.transactionHandler(user, stallid, totalprice, now))
        return NotImplemented
    
    def toDict(self):
        date_format = "%d/%m/%Y %I:%M %p"
        return {
            "uid": self._uid,
            "transactionDatetime": self.transactionDatetime.strftime(date_format),
            "stallId": self.stallId,
            "items": [ product.toDict() for product in self._productList ]
        }
if __name__ == "__main__":
   transactionItems = [
       ("69376545-5bc5-418c-862c-7b1fb0dd8cd9", "95489b2b-88c2-4c96-b165-4383cb4b0aec", 4, 40)
   ]
   DBTransaction.transact("owner", "01H673EJB3R988V58C6NV6ZGB9", transactionItems)