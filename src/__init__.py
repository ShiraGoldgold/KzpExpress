from .core.thumbling_window_logic import ThumblingWindowLogic

windowLogic = ThumblingWindowLogic()

from .infra.redis.redis_client import RedisClient
from .infra.kafka.kafka_consumer import KafkaConsumer
from .infra.kafka.kafka_producer import KafkaProducer
from .infra.rabbitmq.rabbitmq_consumer import RabbitMQConsumer
from .infra.rabbitmq.rabbitmq_producer import RabbitMQProducer
from .services.analytics_service import AnalyticsService
from .constants import WINDOW_MINUTES
from .services.generator import create_random_purchase