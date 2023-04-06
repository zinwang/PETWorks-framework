from dataclasses import dataclass, field
from itertools import product
import numpy as np
from PETWorks.arx import gateway,Data
from PETWorks.arx import getDataFrame


Hierarchy = gateway.jvm.org.deidentifier.arx.AttributeType.Hierarchy
Str = gateway.jvm.java.lang.String


@dataclass
class Config:
    hierarchies: dict[str, list[list[str]]] = field(init=False)
    suppressionRate: float = field(init=False)
    k: int = field(init=False)


def _enumerateClusters(
    nodeNum: int,
    clusterNum: int,
    clusters: list[list[int]],
    currentCluster: list[int] = None,
) -> None:
    if currentCluster is None:
        currentCluster = []

    if len(currentCluster) == clusterNum:
        if sum(currentCluster) == nodeNum:
            sortedCluster = sorted(currentCluster, reverse=True)
            clusters.append(sortedCluster)
            return

    if not currentCluster:
        start = 1
    else:
        start = currentCluster[-1]

    for i in range(start, nodeNum + 1):
        if sum(currentCluster) + i <= nodeNum:
            _enumerateClusters(
                    nodeNum, clusterNum, clusters, currentCluster + [i])


def _enumerateHierarchy(
    levelNodeNum: int,
    maxDepth: int,
    depth: int,
    count: int,
    hierarchy: np.chararray,
    solutions: list[np.chararray]
) -> list[np.chararray]:

    if depth > maxDepth:
        return

    hierarchyRowNum, _ = hierarchy.shape
    if levelNodeNum == 1:
        solutions.append(hierarchy)
        return

    clustersList = []
    for nodeNumNextLevel in range(1, levelNodeNum):
        clusters = []
        _enumerateClusters(hierarchyRowNum, nodeNumNextLevel, clusters)
        clustersList += clusters

    clustersNum = len(clustersList)
    for clustersIndex in range(clustersNum):

        column = []
        levelNodeCount = count

        clusterSumThisLevel = 0

        _, clusterSizesPreviousLevel = np.unique(
                hierarchy[:, -1], return_counts=True)

        clusterAccumulationPreviousLevel = []
        accumulation = 0
        for size in clusterSizesPreviousLevel:
            accumulation += size
            clusterAccumulationPreviousLevel.append(accumulation)

        clusterNum = len(clustersList[clustersIndex])
        for index in range(clusterNum):
            clusterSize = clustersList[clustersIndex][index]

            clusterSumThisLevel += clusterSize

            if not clusterSumThisLevel in clusterAccumulationPreviousLevel:
                break

            column.extend([[str(levelNodeCount)]] * clusterSize)
            levelNodeCount += 1

        if len(column) != hierarchyRowNum:
            continue

        column = np.array(column, dtype=object)
        newHierarchy = np.append(hierarchy, column, axis=1)

        _enumerateHierarchy(
            len(clustersList[clustersIndex]),
            maxDepth,
            depth + 1,
            levelNodeCount,
            newHierarchy,
            solutions
        )


def _assignHierarchyValue(
    hierarchy: np.chararray,
    qiUniqueValues: list[str],
) -> np.chararray:

    isNumeralData = bool(float(qiUniqueValues[0]))
    rowNum, columnNum = hierarchy.shape

    firstColumn = np.array(qiUniqueValues, dtype=object)
    hierarchy[:, 0] = firstColumn

    if not isNumeralData:
        lastColumn = np.array(["*"] * rowNum, dtype=object)
        hierarchy[:, -1] = lastColumn
    else:
        for columnIndex in range(1, hierarchy.shape[1]):
            uniqueValues = np.unique(hierarchy[:, columnIndex])
            for value in uniqueValues:
                indices = (hierarchy[:, columnIndex] == value)

                uniqueValuesPreviousLevel = np.unique(
                        hierarchy[indices, columnIndex - 1]).astype(float)
                hierarchy[indices, columnIndex] = np.average(
                        uniqueValuesPreviousLevel).astype(str)

    return hierarchy


def _wrapQiHierarchy(
    qiName: str,
    qiUniqueValues: list[str],
    hierarchies: list[np.chararray]
) -> list[tuple[str, np.chararray]]:
    qiHierarchy = []
    for hierarchy in hierarchies:
        hierarchy = _assignHierarchyValue(hierarchy, qiUniqueValues)
        qiHierarchy.append(tuple((qiName, hierarchy)))

    return qiHierarchy


def _genQiHierarchy(
    qiName: str,
    qiUniqueValues: list[str]
) -> list[tuple[str, np.chararray]]:
    qiUniqueValueNum = len(qiUniqueValues)
    solutions = []
    hierarchy = [[str(index)] for index in range(qiUniqueValueNum)]
    hierarchy = np.array(hierarchy, dtype=object)

    _enumerateHierarchy(
        qiUniqueValueNum,
        qiUniqueValueNum,
        0,
        qiUniqueValueNum,
        hierarchy,
        solutions
    )
    return _wrapQiHierarchy(qiName, qiUniqueValues, solutions)


def genHierarchies(
    qiInfos: dict[str, list[str]]
) -> list[dict[str, np.chararray]]:
    hierarchiesForEachQi = []
    for qiName, qiUniqueValues in qiInfos.items():
        hierarchiesForEachQi.append(
                _genQiHierarchy(qiName, qiUniqueValues))

    possibleCombinations = product(*hierarchiesForEachQi)

    hierarchies = []
    for combination in possibleCombinations:
        hierarchy = dict(
            (qiName, qiUniqueValues)
            for qiName, qiUniqueValues in combination)

        hierarchies.append(hierarchy)

    return hierarchies


def genSuppressionRates(
    suppressionStep: float
) -> list[float]:
    suppressionRates = []
    for i in range(0, suppressionStep + 1):
        suppressionRates.append(i / suppressionStep)

    return suppressionRates


def genKValues(
    dataRowNum: int
) -> list[int]:
    kValues = []
    for i in range(1, dataRowNum + 1):
        kValues.append(i)

    return kValues


def _combineParameters(
    hierarchiesList: list[dict[str, np.chararray]],
    suppressionRates: list[float],
    kValues: list[int]
) -> list[Config]:
    combinations = product(hierarchiesList, suppressionRates, kValues)
    configs = []
    for combination in combinations:
        config = Config()
        config.hierarchies = combination[0]
        config.suppressionRate = combination[1]
        config.k = combination[2]
        configs.append(config)

    return configs


def generateConfigs(
    originalData: Data,
    qiNames: list[str]
) -> list[Config]:
    originalData = getDataFrame(originalData)
    suppressionStep = len(originalData)

    qiInfos = {}
    for qiName in qiNames:
        qiUniqueValues = originalData[qiName].unique()
        qiInfos[qiName] = qiUniqueValues

    hierarchiesList = genHierarchies(qiInfos)
    suppressionRatesList = genSuppressionRates(suppressionStep)
    kValues = genKValues(len(qiNames))

    return _combineParameters(
        hierarchiesList,
        suppressionRatesList,
        kValues
    )


if __name__ == "__main__":
    pass
