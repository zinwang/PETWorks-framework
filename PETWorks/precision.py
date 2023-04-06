from typing import List

from PETWorks.arx import Data, gateway, loadDataFromCsv, loadDataHierarchy
from PETWorks.arx import JavaApi, javaApiTable

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
Hierarchy = gateway.jvm.org.deidentifier.arx.AttributeType.Hierarchy


def _setDataHierarchies(
    data: Data, hierarchies: dict[str, List[List[str]]]
) -> None:
    for attributeName, hierarchy in hierarchies.items():
        data.getDefinition().setAttributeType(attributeName, hierarchy)


def _measurePrecision(original: Data, anonymized: Data) -> float:
    utility = (
        original.getHandle()
        .getStatistics()
        .getQualityStatistics(anonymized.getHandle())
    )

    precision = utility.getGeneralizationIntensity().getArithmeticMean(False)
    return precision


def PETValidation(original, anonymized, _, dataHierarchy, **other):
    javaApi = JavaApi(gateway, javaApiTable)

    dataHierarchy = loadDataHierarchy(
        dataHierarchy, StandardCharsets.UTF_8, ";", javaApi
    )

    original = loadDataFromCsv(original, StandardCharsets.UTF_8, ";", javaApi)
    anonymized = loadDataFromCsv(anonymized, StandardCharsets.UTF_8, ";", javaApi)

    _setDataHierarchies(original, dataHierarchy)
    _setDataHierarchies(anonymized, dataHierarchy)

    precision = _measurePrecision(original, anonymized)
    return {"precision": precision}
