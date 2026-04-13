import random
from ..constants import PRODUCTS
from ..models import Purchase

WEIGHTS = [10, 70, 20]


def create_random_purchase() -> Purchase:
    product = random.choices(PRODUCTS, weights=WEIGHTS, k=1)[0]
    return Purchase(
        item_id=product["id"],
        item_name=product["name"],
        customer_id=random.randint(100000000, 999999999),
        quantity=random.randint(1, 5),
        category=product["category"],
        price=product["price"]
    )