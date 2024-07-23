from enum import Enum
from datetime import datetime
from pydantic import BaseModel
from typing import Optional, Type, TypeVar, Generic


class FactCheckEventStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    WORKING = "WORKING"


class FactCheckEvent(BaseModel):
    channel_id: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    start_time: Optional[datetime]  
    statement: str
    rationale: str
    score: float
    context: Optional[str] = None
    soundbite_id: Optional[str] = None
    display_duration_seconds: Optional[int] = 15
    force_factcheck: Optional[bool] = False
    is_command: Optional[bool] = False
    sentence_summary: str
    paragraph_summary: str
    three_paragraph_summary: Optional[str] = None
    concise_one_sentence_summary: Optional[str] = None
    support_score: float
    factcheck_status: Optional[FactCheckEventStatus]

