from typing import List

from PETWorks.arx import Data, gateway, loadDataFromCsv, loadDataHierarchy
from PETWorks.attributetypes import IDENTIFIER, INSENSITIVE_ATTRIBUTE
from PETWorks.attributetypes import QUASI_IDENTIFIER

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
Hierarchy = gateway.jvm.org.deidentifier.arx.AttributeType.Hierarchy
AttributeType = gateway.jvm.org.deidentifier.arx.AttributeType


def _setDataHierarchies(
    data: Data, hierarchies: dict[str, List[List[str]]], attributeTypes: dict
) -> None:
    for attributeName, hierarchy in hierarchies.items():
        if not attributeTypes:
            data.getDefinition().setAttributeType(attributeName, hierarchy)
            continue

        attributeType = attributeTypes.get(attributeName)

        if attributeType == QUASI_IDENTIFIER:
            data.getDefinition().setAttributeType(attributeName, hierarchy)

        if attributeType == IDENTIFIER:
            data.getDefinition().setAttributeType(
                attributeName, AttributeType.IDENTIFYING_ATTRIBUTE
            )

        if attributeType == INSENSITIVE_ATTRIBUTE:
            data.getDefinition().setAttributeType(
                attributeName, AttributeType.INSENSITIVE_ATTRIBUTE
            )


def _measureAmbiguity(original: Data, anonymized: Data) -> float:
    utility = (
        original.getHandle()
        .getStatistics()
        .getQualityStatistics(anonymized.getHandle())
    )

    ambiguity = utility.getAmbiguity().getValue()
    return ambiguity


def PETValidation(original, anonymized, _, dataHierarchy, **other):
    attributeTypes = other.get("attributeTypes", None)

    dataHierarchy = loadDataHierarchy(
        dataHierarchy, StandardCharsets.UTF_8, ";"
    )

    original = loadDataFromCsv(original, StandardCharsets.UTF_8, ";")
    anonymized = loadDataFromCsv(anonymized, StandardCharsets.UTF_8, ";")

    _setDataHierarchies(original, dataHierarchy, attributeTypes)
    _setDataHierarchies(anonymized, dataHierarchy, attributeTypes)

    ambiguity = _measureAmbiguity(original, anonymized)
    return {"ambiguity": ambiguity}
