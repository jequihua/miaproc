from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class BiomassColumns:
    """Column mapping for tree observation tables."""
    species: str = "Species"
    dbh_cm: str = "DBH (cm)"
    height_m: str = "Total Height (m)"