from PETWorks.arx import Data, gateway, loadDataFromCsv, loadDataHierarchy
from PETWorks.attributetypes import IDENTIFIER, INSENSITIVE_ATTRIBUTE
import pandas as pd
import numpy as np

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
DataSubset = gateway.jvm.org.deidentifier.arx.DataSubset
HashSet = gateway.jvm.java.util.HashSet
HashGroupifyEntry = (
    gateway.jvm.org.deidentifier.arx.framework.check.groupify.HashGroupifyEntry
)

ARXConfiguration = gateway.jvm.org.deidentifier.arx.ARXConfiguration
ARXCostBenefitConfiguration = (
    gateway.jvm.org.deidentifier.arx.ARXCostBenefitConfiguration
)

KAnonymity = gateway.jvm.org.deidentifier.arx.criteria.KAnonymity
ARXAnonymizer = gateway.jvm.org.deidentifier.arx.ARXAnonymizer
AttributeType = gateway.jvm.org.deidentifier.arx.AttributeType
Metric = gateway.jvm.org.deidentifier.arx.metric.Metric
Int = gateway.jvm.int

ProfitabilityJournalist = (
    gateway.jvm.org.deidentifier.arx.criteria.ProfitabilityJournalist
)
ProfitabilityJournalistNoAttack = (
    gateway.jvm.org.deidentifier.arx.criteria.ProfitabilityJournalistNoAttack
)


def _setDataHierarchies(
    data: Data, hierarchies: dict[str, list[list[str]]], attributeTypes: dict
) -> None:
    for attributeName, hierarchy in hierarchies.items():
        data.getDefinition().setAttributeType(attributeName, hierarchy)
        attributeType = attributeTypes.get(attributeName)

        if attributeType == IDENTIFIER:
            data.getDefinition().setAttributeType(
                attributeName, AttributeType.IDENTIFYING_ATTRIBUTE
            )

        if attributeType == INSENSITIVE_ATTRIBUTE:
            data.getDefinition().setAttributeType(
                attributeName, AttributeType.INSENSITIVE_ATTRIBUTE
            )


def _getQiIndices(dataHandle: str) -> list[int]:
    qiNames = dataHandle.getDefinition().getQuasiIdentifyingAttributes()
    qiIndices = []
    for qi in qiNames:
        qiIndices.append(dataHandle.getColumnIndexOf(qi))
    return qiIndices


def _findAnonymousLevel(hierarchy: list[list[str]], value: str) -> int:
    for i in range(len(hierarchy)):
        for j in range(len(hierarchy[i])):
            if hierarchy[i][j] == value:
                return j
    return -1


def _getAnonymousLevels(
    anonymizedSubset: Data, hierarchies: dict[str, list[list[str]]]
) -> list[int]:
    subsetDataFrame = _getDataFrame(anonymizedSubset.getHandle())
    subsetRowNum, subsetColNum = subsetDataFrame.shape

    qiIndices = _getQiIndices(anonymizedSubset.getHandle())

    sampleRowIndex = -1
    allSuppressed = False
    for i in range(subsetRowNum):
        for j in range(subsetColNum):
            if not (j in qiIndices):
                continue

            if subsetDataFrame.iloc[i][j] != "*":
                sampleRowIndex = i
                break

        if sampleRowIndex != -1:
            break

        allSuppressed = i == subsetRowNum - 1

    anonymousLevels = []
    for index in range(subsetColNum):
        if not (index in qiIndices):
            continue

        value = subsetDataFrame.iloc[sampleRowIndex][index]
        attributeName = subsetDataFrame.columns[index]
        hierarchy = hierarchies[attributeName].getHierarchy()

        if allSuppressed:
            anonymousLevels.append(len(hierarchy[index]) - 1)
            continue

        anonymousLevels.append(_findAnonymousLevel(hierarchy, value))

    return anonymousLevels


def _getDataFrame(dataHandle: str) -> pd.DataFrame:
    rowNum = dataHandle.getNumRows()
    colNum = dataHandle.getNumColumns()

    colNames = []
    data = []
    for i in range(rowNum):
        row = []
        for j in range(colNum):
            row.append(dataHandle.getValue(i, j))

            if i == 0:
                colNames.append(dataHandle.getAttributeName(j))

        data.append(row)

    return pd.DataFrame(data, columns=colNames)


def _getSubsetIndices(
    table: str,
    subset: str,
) -> list[int]:
    qiNames = table.getDefinition().getQuasiIdentifyingAttributes()
    qis = [qi for qi in qiNames]
    qiIndices = _getQiIndices(table)

    tableDataFrame = _getDataFrame(table)
    groupedSubset = _getDataFrame(subset).groupby(qis)

    tableRowNum = len(tableDataFrame)

    subsetIndices = []
    for _, subsetGroup in groupedSubset:
        subsetGroupList = subsetGroup.values.tolist()
        filter = pd.Series(True, index=range(tableRowNum))
        for i in range(len(qiIndices)):
            qiName = qis[i]
            qiIndex = qiIndices[i]
            filter &= tableDataFrame[qiName] == subsetGroupList[0][qiIndex]

        subsetIndices += np.flatnonzero(filter).tolist()[:len(subsetGroupList)]

    return subsetIndices


def _getAnonymousData(original: Data, anonymousLevels: list[int]) -> str:
    levels = gateway.new_array(Int, len(anonymousLevels))
    for i in range(len(anonymousLevels)):
        levels[i] = anonymousLevels[i]

    arxConfig = ARXConfiguration.create()
    arxConfig.addPrivacyModel(KAnonymity(1))
    anonymizer = ARXAnonymizer()
    result = anonymizer.anonymize(original, arxConfig)

    lattice = result.getLattice()
    node = lattice.getNode(levels)

    return result.getOutput(node, True)


def _measureProfitability(
    original: Data,
    subsetIndices: list[int],
    anonymousLevels: list[int],
    allowAttack: bool,
    cost: float,
    gain: float,
    lost: float,
    benefit: float,
) -> bool:
    indices = HashSet()
    for index in subsetIndices:
        indices.add(index)

    subset = DataSubset.create(original, indices)
    original.getHandle().release()

    config = ARXCostBenefitConfiguration.create()
    config.setAdversaryCost(float(cost))
    config.setAdversaryGain(float(gain))
    config.setPublisherLoss(float(lost))
    config.setPublisherBenefit(float(benefit))

    arxConfig = ARXConfiguration.create()
    arxConfig.setCostBenefitConfiguration(config)
    arxConfig.setQualityModel(Metric.createPublisherPayoutMetric(False))

    if allowAttack:
        profitabilityModel = ProfitabilityJournalist(subset)
    else:
        profitabilityModel = ProfitabilityJournalistNoAttack(subset)

    arxConfig.addPrivacyModel(profitabilityModel)
    arxConfig.setAlgorithm(
            ARXConfiguration.AnonymizationAlgorithm.BEST_EFFORT_TOP_DOWN)

    anonymizer = ARXAnonymizer()
    result = anonymizer.anonymize(original, arxConfig)

    levels = gateway.new_array(Int, len(anonymousLevels))
    for i in range(len(anonymousLevels)):
        levels[i] = anonymousLevels[i]

    lattice = result.getLattice()
    node = lattice.getNode(levels)
    anonymity = str(node.getAnonymity())

    return anonymity == "ANONYMOUS"


def PETValidation(
    original,
    subset,
    tech,
    dataHierarchy,
    attributeTypes,
    allowAttack,
    cost,
    gain,
    lost,
    benefit,
    **other
):
    dataHierarchy = loadDataHierarchy(
            dataHierarchy, StandardCharsets.UTF_8, ";")
    original = loadDataFromCsv(original, StandardCharsets.UTF_8, ";")
    subset = loadDataFromCsv(subset, StandardCharsets.UTF_8, ";")

    _setDataHierarchies(original, dataHierarchy, attributeTypes)
    _setDataHierarchies(subset, dataHierarchy, attributeTypes)

    anonymousLevels = _getAnonymousLevels(subset, dataHierarchy)
    anonymizedData = _getAnonymousData(original, anonymousLevels)

    subsetIndices = _getSubsetIndices(anonymizedData, subset.getHandle())

    profitability = _measureProfitability(
        original, subsetIndices, anonymousLevels, allowAttack, cost, gain, lost, benefit
    )

    return {
        "allow attack": allowAttack,
        "adversary's cost": cost,
        "adversary's gain": gain,
        "publisher's loss": lost,
        "publisher's benefit": benefit,
        "profitability": profitability,
    }
