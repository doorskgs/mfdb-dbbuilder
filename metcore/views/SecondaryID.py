from dataclasses import dataclass, field


@dataclass
class SecondaryID:
    edb_id: str
    edb_source: str
    secondary_ids: list[str]
