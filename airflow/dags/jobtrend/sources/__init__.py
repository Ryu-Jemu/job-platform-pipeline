"""원천 레지스트리 — 플랫폼 이름 → 어댑터. 실제로 돌릴 원천은 config.ENABLED_SOURCES로 고릅니다."""
from .base import Query, QuickParse, SourceAdapter
from .jobkorea import JobKorea
from .saramin import Saramin
from .incruit import Incruit
from .linkareer import Linkareer

REGISTRY: dict[str, SourceAdapter] = {a.platform: a for a in (Saramin(), JobKorea(), Incruit(), Linkareer())}

__all__ = ["REGISTRY", "Query", "QuickParse", "SourceAdapter"]
