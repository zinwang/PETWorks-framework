from PETWorks.arx import Data, gateway, loadDataFromCsv
from PETWorks.attributetypes import IDENTIFIER, INSENSITIVE_ATTRIBUTE
from PETWorks.attributetypes import QUASI_IDENTIFIER

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
Hierarchy = gateway.jvm.org.deidentifier.arx.AttributeType.Hierarchy
AttributeType = gateway.jvm.org.deidentifier.arx.AttributeType


def _setDataHierarchies(data: Data, attributeTypes: dict) -> None:
    for column in range(data.getHandle().getNumColumns()):
        attributeName = data.getHandle().getAttributeName(column)

        if not attributeTypes:
            data.getDefinition().setAttributeType(
                attributeName, Hierarchy.create())
            continue

        attributeType = attributeTypes.get(attributeName)

        if attributeType == QUASI_IDENTIFIER:
            data.getDefinition().setAttributeType(
                attributeName, Hierarchy.create())

        if attributeType == IDENTIFIER:
            data.getDefinition().setAttributeType(
                attributeName, AttributeType.IDENTIFYING_ATTRIBUTE
            )

        if attributeType == INSENSITIVE_ATTRIBUTE:
            data.getDefinition().setAttributeType(
                attributeName, AttributeType.INSENSITIVE_ATTRIBUTE
            )


def _measureAECS(original: Data, anonymized: Data) -> float:
    utility = (
        original.getHandle()
        .getStatistics()
        .getQualityStatistics(anonymized.getHandle())
    )

    aecs = utility.getAverageClassSize().getValue()
    return aecs


def PETValidation(original, anonymized, _, **other):
    attributeTypes = other.get("attributeTypes", None)

    original = loadDataFromCsv(original, StandardCharsets.UTF_8, ";")
    anonymized = loadDataFromCsv(anonymized, StandardCharsets.UTF_8, ";")

    _setDataHierarchies(original, attributeTypes)
    _setDataHierarchies(anonymized, attributeTypes)

    aecs = _measureAECS(original, anonymized)
    return {"AECS": aecs}
