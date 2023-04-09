import pandas as pd

from PETWorks.arx import Data, gateway, loadDataFromCsv, loadDataHierarchy
from PETWorks.arx import setDataHierarchies, getDataFrame, getQiNames
from PETWorks.arx import getAnonymousLevels, applyAnonymousLevels
from py4j.java_gateway import set_field
from PETWorks.arx import JavaApi, javaApiTable, Hierarchy
from PETWorks.attributetypes import SENSITIVE_ATTRIBUTE, QUASI_IDENTIFIER
import numpy as np
import pandas as pd
from math import fabs

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
#TCloseness = gateway.jvm.org.deidentifier.arx.criteria.TCloseness


# 10 min
# for one attribute
def _convertArxHierarchyToNumpyArray(
    hierarchy: Hierarchy,
) -> np.chararray:
    hierarchyArray = hierarchy.getHierarchy()
    hierarchyWidth = len(hierarchyArray)
    hierarchyHeight = len(hierarchyArray[0])

    hierarchyNpArray = np.empty((hierarchyWidth, hierarchyHeight), dtype=object)
    for rowIndex in range(hierarchyWidth):
        for colIndex in range(hierarchyHeight):
            hierarchyNpArray[rowIndex, colIndex] = str(hierarchyArray[rowIndex][colIndex])

    return hierarchyNpArray


# 60 min
# for one ec
def _computeHierarchicalDistance(
    dataDistribution: dict[str, float],
    groupDistribution: dict[str, float],
    sensitiveHierarchy: np.chararray,
) -> float:

    hierarchyWidth, hierarchyHeight = sensitiveHierarchy.shape

    extraArray = np.zeros((hierarchyWidth, hierarchyHeight), dtype=np.float)
    costArray = np.zeros((hierarchyWidth, hierarchyHeight), dtype=np.float)

    # loop through hierarchy height from 0
    for currentHeight in range(hierarchyHeight):
        for rowIndex in range(hierarchyWidth):

            # if leaf
            if currentHeight == 0:
                costArray[rowIndex, currentHeight] = 0.0

                value = sensitiveHierarchy[rowIndex, 0]
                extra = groupDistribution.get(value, 0) - dataDistribution.get(value, 0)
                extraArray[rowIndex, currentHeight] = extra
                continue

            # if not leaf
            uniqueValues = np.unique(sensitiveHierarchy[:, currentHeight])
            for value in uniqueValues:
                rowIndicesWithMatchedValue = np.where(sensitiveHierarchy[:, currentHeight] == value)[0]
                extraSubset = extraArray[rowIndicesWithMatchedValue, currentHeight - 1]
                maskForPostiveExtras = (extraSubset > 0)
                maskForNegtiveExtras = (extraSubset < 0)

                postiveExtrasSum = np.sum(extraSubset[maskForPostiveExtras])
                negtiveExtrasSum = -1 * np.sum(extraSubset[maskForNegtiveExtras])

                extraArray[rowIndicesWithMatchedValue[0], currentHeight] = postiveExtrasSum - negtiveExtrasSum

                cost = float(currentHeight) * min(postiveExtrasSum, negtiveExtrasSum)
                cost /= hierarchyHeight - 1
                costArray[rowIndicesWithMatchedValue[0], currentHeight] = cost

    return float(np.sum(costArray))



# 40 min
# for one ec
def _computeNumericalDistance(
    dataDistribution: dict[str, float],
    groupDistribution: dict[str, float],
    originalSenstiveData: pd.Series
) -> float:
    originalSenstiveData = originalSenstiveData.sort_values(ascending=True, key=lambda x: pd.to_numeric(x, errors='coerce'))
    numRows = len(originalSenstiveData)


    valueList = sorted([
        originalSenstiveData[index]
        for index in range(numRows)
    ], key=lambda x: pd.to_numeric(x))

    extraList = [
        float(groupDistribution.get(value, 0) - dataDistribution.get(value, 0))
        for value in valueList
    ]

    distance = 0.0
    for index in range(numRows):
        sum = 0
        for subIndex in range(index):
            sum += extraList[subIndex]
        distance += fabs(sum)
    distance /= numRows - 1

    return distance



# 20 min
def _computeTCloseness(
    originalData: pd.DataFrame,
    anonymizedData: pd.DataFrame,
    sensitiveAttributeName: str,
    qiNames: list[str],
    sensitiveHierarchy: np.chararray,
) -> float:

    dataDistribution = dict(originalData[sensitiveAttributeName].value_counts() * 0 + 1 / originalData[sensitiveAttributeName].nunique())
    anonymizedGroups = anonymizedData.groupby(qiNames)


    maxHierarchicalDistance = float('-inf')
    for _, group in anonymizedGroups:

        groupDistribution = dict(group[sensitiveAttributeName].value_counts() * 0 + 1 / len(group))
        if sensitiveHierarchy and 0:
            distance = _computeHierarchicalDistance(
                dataDistribution, groupDistribution, sensitiveHierarchy)
        else:
            distance = _computeNumericalDistance(dataDistribution, groupDistribution, originalData[sensitiveAttributeName])

        if distance > maxHierarchicalDistance:
            maxHierarchicalDistance = distance

    return maxHierarchicalDistance



# 10 min
def measureTCloseness(
    originalData: pd.DataFrame,
    anonymizedData: pd.DataFrame,
    sensitiveAttributeName: str,
    qiNames: list[str],
    sensitiveHierarchy: np.chararray,
) -> float:

    isNumerical = True
    try:
        float(sensitiveHierarchy[0, 0])
    except:
        isNumerical = False

    if isNumerical or True:
        return _computeTCloseness(
            originalData, anonymizedData, sensitiveAttributeName, qiNames, None)

    return _computeTCloseness(
        originalData, anonymizedData, sensitiveAttributeName, qiNames, sensitiveHierarchy)


def _validateTCloseness(tFromData: float, tLimit: float) -> bool:
    return tFromData < tLimit


def PETValidation(original, anonymized, _, dataHierarchy, **other):
    tFromUser = float(other["t"])
    attributeTypes = other.get("attributeTypes", None)

    javaApi = JavaApi(gateway, javaApiTable)
    dataHierarchy = loadDataHierarchy(
        dataHierarchy, StandardCharsets.UTF_8, ";", javaApi
    )

    original = loadDataFromCsv(original, StandardCharsets.UTF_8, ";", javaApi)
    anonymized = loadDataFromCsv(anonymized, StandardCharsets.UTF_8, ";", javaApi)

    originalData = getDataFrame(original)
    anonymizedData = getDataFrame(anonymized)

    # get sensitive attribute
    qiNames = []
    sensitiveAttributeName = ""
    for attributeName, attributeType in attributeTypes.items():
        if attributeType == SENSITIVE_ATTRIBUTE:
            sensitiveAttributeName = attributeName
            continue
        if attributeType == QUASI_IDENTIFIER:
            qiNames.append(attributeName)

    sensitiveHierarchy = None
    if(dataHierarchy):
        sensitiveHierarchy = _convertArxHierarchyToNumpyArray(dataHierarchy[sensitiveAttributeName])

    tFromData = measureTCloseness(
        originalData, anonymizedData, sensitiveAttributeName, qiNames, sensitiveHierarchy)

    fullfillTCloseness = _validateTCloseness(tFromData, tFromUser)

    return {"t": tFromUser,
            "fullfill t-closeness": fullfillTCloseness
            }
