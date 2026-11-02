"""Lazy path mapping for new daily partitions; never opens legacy stores."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from kernel.memory_pipeline_models import PipelineScope
from .pipeline_database import reject_directory_link, resolved_pipeline_path

if TYPE_CHECKING:
    from .derived_evidence import DerivedEvidenceStore
    from .memory_ledger import SourceLedger
    from .derivation_runs import DerivationRuns


class ScopedStores:
    def __init__(self, derived_root: Path):
        if not isinstance(derived_root, Path) or not derived_root.is_absolute():
            raise ValueError("explicit absolute derived root required")
        self.root = resolved_pipeline_path(derived_root)

    def partition(self, scope: PipelineScope) -> Path:
        if type(scope) is not PipelineScope:
            raise ValueError("complete pipeline scope required")
        path = self.root / scope.partition_id
        if resolved_pipeline_path(path).parent != self.root:
            raise ValueError("partition path escaped derived root")
        reject_directory_link(path)
        return path

    def partition_registry(self):
        from .partition_registry import PartitionRegistry

        return PartitionRegistry(self.root)

    def partition_state(self, scope: PipelineScope) -> str:
        snapshot = self.partition_registry().snapshot(scope)
        return snapshot["state"] if snapshot is not None else "UNINITIALIZED"

    def ledger(self, scope: PipelineScope) -> SourceLedger:
        from .memory_ledger import SourceLedger

        self.partition(scope)
        return SourceLedger(self.root, scope)

    def runs(self, scope: PipelineScope) -> DerivationRuns:
        from .derivation_runs import DerivationRuns

        self.partition(scope)
        return DerivationRuns(self.root, scope)

    def evidence(self, scope: PipelineScope) -> DerivedEvidenceStore:
        from .derived_evidence import DerivedEvidenceStore

        self.partition(scope)
        return DerivedEvidenceStore(self.root, scope)

    def validate_vector_index(self, scope: PipelineScope,
                              target_ids: tuple[str, ...] = ()) -> None:
        from .daily_vector_index import DailyVectorIndex

        DailyVectorIndex.validate_existing(self, scope, target_ids)

    def gate(self, scope: PipelineScope):
        from .derivation_gate import DailyDerivationGate

        self.partition(scope)
        return DailyDerivationGate(self.root, scope)

    def extractions(self, scope: PipelineScope):
        from .extraction_snapshots import ExtractionSnapshots

        self.partition(scope)
        return ExtractionSnapshots(self.root, scope)
