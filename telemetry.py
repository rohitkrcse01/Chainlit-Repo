import logging
from typing import Optional, Dict, Any

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter


class MetricsCollector:
    _instance = None

    @staticmethod
    def get_instance() -> "MetricsCollector":
        if MetricsCollector._instance is None:
            MetricsCollector._instance = MetricsCollector()
        return MetricsCollector._instance

    def __init__(self):
        self.meter = None
        self.meter_provider = None

        self.user_counter = None
        self.tool_calls_counter = None
        self.tool_call_errors_counter = None

        self.tool_response_time_histogram = None
        self.feature_response_time_histogram = None

        self.features_counter = None
        self.counters: Dict[str, Any] = {}

        self.story_open_counter = None
        self.refine_completed_counter = None
        self.story_created_counter = None
        self.feature_created_counter = None

        self._initialized = False

    def init_metrics(
        self,
        service_name: str = "storyweaver_service",
        otel_collector_url: str = "http://xxxxxxxx:4318/v1/metrics",
        interval: int = 5000,
        service_version: str = "0.1.0",
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 10,
    ) -> None:
        """
        Initializes OpenTelemetry metrics export via OTLP/HTTP.

        otel_collector_url MUST be an OTLP/HTTP metrics endpoint, usually:
        http://<collector-host>:4318/v1/metrics
        """
        if self._initialized:
            logging.getLogger(__name__).info("Metrics already initialized; skipping re-init.")
            return

        # Helpful logs for debugging exporter failures
        logging.getLogger("opentelemetry").setLevel(logging.DEBUG)

        resource = Resource.create(
            {
                "service.name": service_name,
                "service.version": service_version,
            }
        )

        exporter = OTLPMetricExporter(
            endpoint=otel_collector_url,
            headers=headers,
            timeout=timeout,
        )

        reader = PeriodicExportingMetricReader(
            exporter,
            export_interval_millis=interval,
        )

        provider = MeterProvider(
            resource=resource,
            metric_readers=[reader],
        )

        self.meter_provider = provider
        metrics.set_meter_provider(provider)

        self.meter = metrics.get_meter(service_name)

        # Counters
        self.user_counter = self.meter.create_counter("users", description="User Count")
        self.tool_calls_counter = self.meter.create_counter(
            "toolCalls", description="Counter to measure tool calls"
        )
        self.tool_call_errors_counter = self.meter.create_counter(
            "toolCallErrors", description="Counter to measure tool call errors"
        )

        self.story_open_counter = self.meter.create_counter(
            "story_open", description="Counts story open events"
        )
        self.refine_completed_counter = self.meter.create_counter(
            "refine_completed", description="Counts refine completed events"
        )
        self.story_created_counter = self.meter.create_counter(
            "story_created", description="Counts story created events"
        )
        self.feature_created_counter = self.meter.create_counter(
            "feature_created", description="Counts feature created events"
        )

        self.features_counter = self.meter.create_counter(
            "feature_usage", description="Counts feature usage"
        )

        # Histograms
        self.tool_response_time_histogram = self.meter.create_histogram(
            "tool_response_time",
            description="Tool response time",
            unit="ms",
        )
        self.feature_response_time_histogram = self.meter.create_histogram(
            "feature_response_time",
            description="Feature response time",
            unit="ms",
        )

        self._initialized = True
        logging.getLogger(__name__).info(
            "Metrics initialized for service=%s exporting_to=%s interval_ms=%s",
            service_name,
            otel_collector_url,
            interval,
        )

    def _ensure_initialized(self) -> None:
        if not self._initialized or self.meter is None:
            raise RuntimeError(
                "MetricsCollector not initialized. Call init_metrics(...) once at startup."
            )

    def increment_counter(self, counter: str, labels: Optional[Dict[str, Any]] = None) -> None:
        self._ensure_initialized()

        if counter not in self.counters:
            self.counters[counter] = self.meter.create_counter(
                counter, description=f"Counter for {counter}"
            )

        self.counters[counter].add(1, labels or {})
        logging.getLogger(__name__).info(
            "Pushed custom counter metric: %s, labels: %s", counter, labels
        )

    def add_user(self, username: str) -> None:
        self._ensure_initialized()

        self.user_counter.add(1, {"username": username})
        logging.getLogger(__name__).info("Pushed user_tracked metric for user: %s", username)

    def add_feature_usage(self, feature_name: str, username: str) -> None:
        self._ensure_initialized()

        self.features_counter.add(1, {"featureName": feature_name, "username": username})
        self.add_user(username)
        logging.getLogger(__name__).info(
            "Pushed feature_usage metric for feature=%s user=%s", feature_name, username
        )

    def add_tool_call(self, tool_name: str, username: str) -> None:
        self._ensure_initialized()

        self.tool_calls_counter.add(1, {"toolName": tool_name, "username": username})
        self.add_user(username)
        logging.getLogger(__name__).info(
            "Pushed tool_call metric for tool=%s user=%s", tool_name, username
        )

    def add_tool_call_error(self, tool_name: str, username: str) -> None:
        self._ensure_initialized()

        self.tool_call_errors_counter.add(1, {"toolName": tool_name, "username": username})
        self.add_user(username)
        logging.getLogger(__name__).info(
            "Pushed tool_call_error metric for tool=%s user=%s", tool_name, username
        )

    def add_tool_response_time(self, tool_name: str, response_time: float, username: str) -> None:
        self._ensure_initialized()

        self.tool_response_time_histogram.record(
            response_time,
            {"toolName": tool_name, "username": username},
        )
        self.add_user(username)
        logging.getLogger(__name__).info(
            "Pushed tool_response_time metric for tool=%s response_time=%s user=%s",
            tool_name,
            response_time,
            username,
        )

    def add_feature_response_time(
        self, feature_name: str, response_time: float, username: str
    ) -> None:
        self._ensure_initialized()

        self.feature_response_time_histogram.record(
            response_time,
            {"featureName": feature_name, "username": username},
        )
        self.add_user(username)
        logging.getLogger(__name__).info(
            "Pushed feature_response_time metric for feature=%s response_time=%s user=%s",
            feature_name,
            response_time,
            username,
        )

    def add_story_open(self, username: str) -> None:
        self._ensure_initialized()

        self.story_open_counter.add(1, {"username": username})
        self.add_user(username)
        logging.getLogger(__name__).info("Pushed story_open metric for user=%s", username)

    def add_refine_completed(self, username: str) -> None:
        self._ensure_initialized()

        self.refine_completed_counter.add(1, {"username": username})
        self.add_user(username)
        logging.getLogger(__name__).info("Pushed refine_completed metric for user=%s", username)

    def add_story_created(self, username: str) -> None:
        self._ensure_initialized()

        self.story_created_counter.add(1, {"username": username})
        self.add_user(username)
        logging.getLogger(__name__).info("Pushed story_created metric for user=%s", username)

    def add_feature_created(self, username: str) -> None:
        self._ensure_initialized()

        self.feature_created_counter.add(1, {"username": username})
        self.add_user(username)
        logging.getLogger(__name__).info("Pushed feature_created metric for user=%s", username)
