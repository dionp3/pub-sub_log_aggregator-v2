from pydantic import BaseModel, Field
import datetime
from typing import List, Optional

class EventPayload(BaseModel):
    content: str = Field(..., description="Isi log atau pesan utama.")
    metadata: Optional[dict] = Field(None, description="Metadata opsional untuk detail kontekstual.")

class Event(BaseModel):
    topic: str = Field(..., description="Topic event (e.g., payment.notification). Digunakan dalam PRIMARY KEY Dedup Store.")
    event_id: str = Field(..., description="T4: Unique, collision-resistant ID (UUID). Kunci utama untuk Idempotency.")
    timestamp: datetime.datetime = Field(..., description="T5: Waktu event dibuat, format ISO8601 dengan Timezone (UTC disarankan).")
    source: str = Field(..., description="Nama layanan atau sistem yang menghasilkan event.")
    payload: EventPayload = Field(..., description="Data utama event.")

class ProcessedEvent(BaseModel):
    event_id: str
    topic: str
    timestamp: datetime.datetime

class AggregatorStats(BaseModel):
    received: int = Field(..., description="Total event yang diterima Aggregator (termasuk duplikat).")
    unique_processed: int = Field(..., description="T7: Total event unik yang berhasil diproses dan disimpan.")
    duplicate_dropped: int = Field(..., description="T6: Total event duplikat yang ditolak oleh Dedup Store.")
    topics: List[str] = Field(..., description="T10: Daftar topik unik yang pernah diproses.")
    uptime: int = Field(..., description="T12: Waktu Aggregator telah berjalan (detik).")