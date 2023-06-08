from typing import Dict

import pandas as pd

from PETWorks.arx import (
    JavaApi,
    arxAnonymize,
    getDataFrame,
    loadDataFromCsv,
    loadDataHierarchy,
    setDataHierarchies,
)
from PETWorks.attributetypes import QUASI_IDENTIFIER, SENSITIVE_ATTRIBUTE


def measureLDiversity(
    anonymizedData: pd.DataFrame,
    attributeTypes: Dict[str, str],
) -> list[int]:
    qis = []
    sensitiveAttributes = []
    lValues = []

    for attribute, value in attributeTypes.items():
        if value == QUASI_IDENTIFIER:
            qis.append(attribute)
        if value == SENSITIVE_ATTRIBUTE:
            sensitiveAttributes.append(attribute)

    for index in range(len(sensitiveAttributes)):
        columns = (
            qis
            + sensitiveAttributes[:index]
            + sensitiveAttributes[index + 1:]
        )
        groups = anonymizedData.groupby(columns)

        sensitiveAttribute = sensitiveAttributes[index]
        lValues += [group[sensitiveAttribute].nunique() for _, group in groups]

    return lValues


def validateLDiversity(lValues: list[int], lLimit: int) -> bool:
    return all(value >= lLimit for value in lValues)


def PETValidation(original, anonymized, _, attributeTypes, l):
    anonymizedDataFrame = pd.read_csv(anonymized, sep=";")

    lValues = measureLDiversity(anonymizedDataFrame, attributeTypes)
    fulfillLDiversity = validateLDiversity(lValues, l)

    return {"l": l, "fulfill l-diversity": fulfillLDiversity}


def PETAnonymization(
    originalData: str,
    _,
    dataHierarchy: str,
    attributeTypes: Dict,
    maxSuppressionRate: float,
    l: int,
) -> pd.DataFrame:
    javaApi = JavaApi()
    originalData = loadDataFromCsv(
        originalData, javaApi.StandardCharsets.UTF_8, ";", javaApi
    )

    dataHierarchy = loadDataHierarchy(
        dataHierarchy, javaApi.StandardCharsets.UTF_8, ";", javaApi
    )

    setDataHierarchies(originalData, dataHierarchy, attributeTypes, javaApi)

    privacyModels = []
    for attributeName, attributeType in attributeTypes.items():
        if attributeType == SENSITIVE_ATTRIBUTE:
            privacyModels.append(javaApi.DistinctLDiversity(attributeName, l))

    anonymizedData = arxAnonymize(
        originalData,
        dataHierarchy,
        attributeTypes,
        maxSuppressionRate,
        privacyModels,
        None,
        javaApi,
    )

    return getDataFrame(anonymizedData)
