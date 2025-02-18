import boto3
from datetime import datetime
import uuid
import ulid
from .base import toFloat, toInt, toString, convertEmptyString


class Structure:
    PrimaryKey = 'PK'
    PrimaryKeyType = 'S'
    SortKey = 'SK'
    SortKeyType = 'S'
    META = "#0"
    MetaField = "metafield"
    MetaIndex = "MetaList"
    DefaultStorehouseId = "1"

    NumberTypeSetter = lambda x: {"N": str(x)}
    StringTypeSetter = lambda x: {"S": x}
    NullTypeSetter = {"NULL": True}
    PrimaryKeyTypeSetter = StringTypeSetter
    SortKeyTypeSetter = StringTypeSetter
    MetaFieldTypeSetter = StringTypeSetter

    @classmethod
    def itemSetter(cls, pk, sk):
        item = {
            cls.PrimaryKey : {cls.PrimaryKeyType: pk},
            cls.SortKey : {cls.SortKeyType: sk}
        }
        return item
    
    @staticmethod
    def unwrapper(item):
        if item.get("N"):
            return toFloat(item.get("N"))
        if item.get("S") or item.get("S") == "":
            return convertEmptyString(item.get("S"))
        if item.get("NULL"):
            return None

def responseItemUnwrapper(item):
    result = {}
    for key, val in item.items():
        result[key] = Structure.unwrapper(val)     
    return result           

# def dynamodb():
#     return boto3.client('dynamodb',
#         endpoint_url="http://localhost:8000",
#         region_name='hi',
#         aws_access_key_id='dummy',
#         aws_secret_access_key='dummy',
#     )

def dynamodb():
    return boto3.client('dynamodb',
        region_name='ap-southeast-2',
        aws_access_key_id='AKIAQFQTEURQ2BXS7IBH',
        aws_secret_access_key='NMJmYZC0kSNWcq+EoTsfoZ7s6S4QfTa9PVYHQvzW',
    )

def getisodatetime(d:datetime=None):
    if d:
        try:
            return d.isoformat()
        except:
            return d
    return datetime.now().isoformat()

def getuuid():
    return str(uuid.uuid4())

def getulid():
    return ulid.new().str


