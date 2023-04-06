from PETWorks.arx import gateway, loadDataFromCsv, loadDataHierarchy
from PETWorks.attributetypes import QUASI_IDENTIFIER
from PETWorks.metriceval import evaluateMetrics, Metrics
from PETWorks.configgen import generateConfigs
from PETWorks.validater import validateMetrics
from PETWorks.arx import JavaApi, javaApiTable, Hierarchy
import numpy as np

from typing import Callable

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets


def _toOutputFormat(metrics: Metrics) -> dict:
    result = {}

    if metrics:
        result["k"] = metrics.privacy.kAnonymity
        result["d"] = metrics.privacy.dPresence
        result["ambibuty"] = metrics.utility.ambiguty
        result["precision"] = metrics.utility.precision
        result["nonUnifromEntropy"] = metrics.utility.nonUniformEntropy
        result["aecs"] = metrics.utility.aecs

    return result


def _wrapHierarchy(hierarchy: Hierarchy) -> np.array:

    hierarchyList = []
    for hierarchyRow in hierarchy.getHierarchy():
        row = []
        for value in hierarchyRow:
            row.append(value)
        hierarchyList.append(row)

    return np.array(hierarchyList, dtype=object)



def autoTune(
    originalData: str,
    dataHierarchies: str,
    attributeTypes: dict[str, str],
    analyzingFunction: Callable,
    error: float,
    numOfProcess: int
):
    javaApi = JavaApi(gateway, javaApiTable)
    originalData = loadDataFromCsv(originalData, StandardCharsets.UTF_8, ";", javaApi)

    dataHierarchies = loadDataHierarchy(
        dataHierarchies, StandardCharsets.UTF_8, ";", javaApi
    )

    hierarchies = {}
    for attributeName, hierarchy in dataHierarchies.items():
        hierarchies[attributeName] = _wrapHierarchy(hierarchy)



    qiNames = []
    for qiName, attributeType in attributeTypes.items():
        if attributeType == QUASI_IDENTIFIER:
            qiNames.append(qiName)

    print("Generating parameter combinations...")
    configs = generateConfigs(originalData, hierarchies, qiNames)
    print("Evaluate metricses...")
    metricsList = evaluateMetrics(
        originalData,
        attributeTypes,
        configs,
        numOfProcess
    )

    print("Validate metricses...")
    result = validateMetrics(originalData, metricsList, analyzingFunction, error)
    return _toOutputFormat(result)

