from pydantic import BaseModel, Field
from datetime import datetime
import uuid


class Purchase(BaseModel):
    purchase_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    item_id: int
    item_name: str
    customer_id: int
    quantity: int
    purchase_time: datetime = Field(default_factory=datetime.now)
    category: str
    price: float

