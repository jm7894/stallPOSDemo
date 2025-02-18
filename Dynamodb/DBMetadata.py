from Dynamodb.structure import dynamodb

class Metadata:
    table = dynamodb.Table('Inventory')
    attr = {
        'primaryKey': 'PK',
        'sortKey': 'SK',
        'product': 'productcount',
        'inventory': 'inventorycount',
        'invoice': 'invoicecount',
        'storehouse': 'storehousecount'
    }
    