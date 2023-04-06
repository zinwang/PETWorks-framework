from PETWorks.arx import gateway, loadDataFromCsv
from PETWorks.attributetypes import QUASI_IDENTIFIER
from PETWorks.metriceval import evaluateMetrics, Metrics
from PETWorks.configgen import generateConfigs
from PETWorks.validater import validateMetrics
from PETWorks.arx import JavaApi, javaApiTable

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


def autoTune(
    originalData: str,
    attributeTypes: dict[str, str],
    analyzingFunction: Callable,
    error: float,
    numOfProcess: int
):
    javaApi = JavaApi(gateway, javaApiTable)
    originalData = loadDataFromCsv(originalData, StandardCharsets.UTF_8, ";", javaApi)
    qiNames = []
    for qiName, attributeType in attributeTypes.items():
        if attributeType == QUASI_IDENTIFIER:
            qiNames.append(qiName)

    print("Generating parameter combinations...")
    configs = generateConfigs(originalData, qiNames)
    print("Evaluate metricses...")
    configs = configs[:0]
    metricsList = evaluateMetrics(
        originalData,
        attributeTypes,
        configs,
        numOfProcess
    )

    print("Validate metricses...")
    result = validateMetrics(originalData, metricsList, analyzingFunction, error)
    return _toOutputFormat(result)

