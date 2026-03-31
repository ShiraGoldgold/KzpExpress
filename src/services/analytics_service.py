from collections import Counter
from constants import PRODUCTS_MAP


class AnalyticsService:
    def __init__(self, redis_client):
        self.redis = redis_client

    def _get_top_items_info(self, conter_top_items_ids):
        top_items_info = []
        for item_id, count in conter_top_items_ids:
            product = PRODUCTS_MAP.get(int(item_id), {"name": "Unknown"})
            top_items_info.append({
                "name": product["name"],
                "count": count
            })
        return top_items_info

    def get_top_three(self, window_key):
        ids = self.redis.get_hset_values(window_key)
        if not ids:
            return []
        return self._get_top_items_info(Counter(ids).most_common(3))
