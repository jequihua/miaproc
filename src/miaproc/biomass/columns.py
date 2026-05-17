from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class BiomassColumns:
    """Column mapping for incoming forest-structure / tree observation tables.

    Defaults follow the field-contract reference at
    ``08_pkg/docs/forest_data_schema.csv`` (M16): ``species``, ``dbh_cm``,
    ``tree_height_m``, ``life_stage``. These are the field names downstream
    BigQuery / colleague-facing pipelines will pass in.

    The defaults are deliberately overridable: callers with table schemas
    that use different field names can construct ``BiomassColumns(species=...,
    dbh_cm=..., height_m=..., life_stage=...)`` without rewriting the package
    matching contract. The mapping onto the equation-parquet predictor names
    (``species`` -> ``scientific_name_apg_raw``, ``dbh_cm`` -> ``diam``,
    optional ``tree_height_m`` -> ``alt``) is fixed by the parquet contract,
    not by these defaults.
    """

    species: str = "species"
    dbh_cm: str = "dbh_cm"
    height_m: str = "tree_height_m"
    life_stage: str = "life_stage"
