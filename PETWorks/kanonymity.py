from PETWorks.arx import (
    getAttributeNameByType,
    JavaApi,
    arxAnonymize,
    getDataFrame,
    loadDataFromCsv,
    loadDataHierarchy,
    setDataHierarchies,
)
from PETWorks.attributetypes import QUASI_IDENTIFIER
import pandas as pd
from typing import Dict


def _measureKAnonymity(anonymized: pd.DataFrame, qiNames: list[str]) -> int:
    suppressedValues = ["*"] * len(qiNames)
    anonymized = anonymized.loc[
        ~anonymized[qiNames].isin(suppressedValues).all(axis=1)
    ]
    return anonymized.groupby(qiNames).count().min().min()


def _validateKAnonymity(kValue: int, k: int) -> bool:
    return k <= kValue


def PETValidation(foo, anonymized, bar, attributeTypes, k):
    anonymized = pd.read_csv(anonymized, sep=";", skipinitialspace=True)
    qiNames = list(getAttributeNameByType(attributeTypes, QUASI_IDENTIFIER))

    kValue = int(_measureKAnonymity(anonymized, qiNames))
    fulFillKAnonymity = _validateKAnonymity(kValue, k)

    return {"k": k, "fulfill k-anonymity": fulFillKAnonymity}


def PETAnonymization(
    originalData: str,
    _,
    dataHierarchy: str,
    attributeTypes: Dict,
    maxSuppressionRate: float,
    k: int,
) -> pd.DataFrame:
    javaApi = JavaApi()
    originalData = loadDataFromCsv(
        originalData, javaApi.StandardCharsets.UTF_8, ";", javaApi
    )

    dataHierarchy = loadDataHierarchy(
        dataHierarchy, javaApi.StandardCharsets.UTF_8, ";", javaApi
    )

    setDataHierarchies(originalData, dataHierarchy, attributeTypes, javaApi)

    anonymizedData = arxAnonymize(
        originalData,
        dataHierarchy,
        attributeTypes,
        maxSuppressionRate,
        [javaApi.KAnonymity(k)],
        None,
        javaApi,
    )

    return getDataFrame(anonymizedData)
