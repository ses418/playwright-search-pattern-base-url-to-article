from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import datetime

@dataclass
class SearchPattern:
    base_url: str
    search_type: str
    pattern: Optional[str]
    method: str = "GET"
    request_body: Optional[Dict] = None
    headers: Optional[Dict] = None
    confidence: float = 0.0


@dataclass
class DetectionResult:
    """Structured result returned by the detection engine.

    Attributes:
        base_url: str: the domain being inspected
        success: bool: whether detection succeeded
        search_pattern: Optional[SearchPattern]: populated when success
        error: Optional[str]: error message when failure
        layer: Optional[str]: the detection layer that produced the result
        duration: Optional[float]: time taken in seconds
        timestamp: datetime.datetime: when the result was generated
    """
    base_url: str
    success: bool
    search_pattern: Optional[SearchPattern] = None
    error: Optional[str] = None
    layer: Optional[str] = None
    duration: Optional[float] = None
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.utcnow)