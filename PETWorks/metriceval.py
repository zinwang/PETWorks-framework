from PETWorks.arx import Data, getDataFrame, getQiNames, getQiIndices
from PETWorks.arx import createJavaGateway, setDataHierarchies
from PETWorks.arx import applyAnonymousLevels, anonymizeData, Py4JJavaError
from PETWorks.arx import JavaApi, javaApiTable, packArxData, packArxHierarchies
from PETWorks.arx import Hierarchy, ARXResult
from PETWorks.configgen import Config

from dataclasses import dataclass, field
import pandas as pd

from multiprocessing.pool import Pool
from multiprocessing import cpu_count


@dataclass
class Utility:
    ambiguty: float = field(init=False)
    precision: float = field(init=False)
    nonUniformEntropy: float = field(init=False)
    aecs: float = field(init=False)


@dataclass
class Privacy:
    kAnonymity: int = field(init=False)
    dPresence: float = field(init=False)


@dataclass
class Metrics:
    anonymizedData: pd.DataFrame = field(init=False)
    config: list[int] = field(init=False)
    levels: list[int] = field(init=False)
    utility: Utility = field(init=False)
    privacy: Privacy = field(init=False)


def _evaluteUtility(originalData: Data, anonymizedData: Data) -> Utility:
    statistics = (
        originalData.getHandle()
        .getStatistics()
        .getQualityStatistics(anonymizedData.getHandle())
    )
    utility = Utility()
    utility.ambiguty = statistics.getAmbiguity().getValue()
    utility.precision = statistics.getGeneralizationIntensity().getArithmeticMean(False)
    utility.nonUniformEntropy = statistics.getNonUniformEntropy().getArithmeticMean(False)
    utility.aecs = statistics.getAverageClassSize().getValue()
    return utility


def _evaluteKAnonymity(data: Data, qiNames: list[str]) -> int:
    dataFrame = getDataFrame(data)
    groupedData = dataFrame.groupby(qiNames)

    minSize = len(dataFrame)
    for _, group in groupedData:
        groupList = group.values.tolist()
        count = len(groupList)
        if count < minSize:
            minSize = count

    return minSize


def _evaluteDPresence(
    originalData: Data,
    anonymizedSubset: Data,
    attributeTypes: dict[str, str],
    qiNames: list[str],
    config: Config,
    levels: list[int],
    javaApi: JavaApi
) -> int:
    hierarchies = config.hierarchies

    anonymizedData = applyAnonymousLevels(
        originalData,
        levels,
        hierarchies,
        attributeTypes,
        javaApi
    )

    if not anonymizedData:
        return 1

    qiIndices = getQiIndices(anonymizedData)
    groupedData = getDataFrame(anonymizedData).groupby(qiNames)
    groupedSubset = getDataFrame(anonymizedSubset).groupby(qiNames)

    deltaValues = []
    for _, subsetGroup in groupedSubset:
        count = 0
        pcount = 0

        subsetGroupList = subsetGroup.values.tolist()
        count = len(subsetGroupList)

        for _, dataGroup in groupedData:
            dataGroupList = dataGroup.values.tolist()

            pcount = len(dataGroup)
            for index in qiIndices:
                if subsetGroupList[0][index] != dataGroupList[0][index]:
                    pcount = 0

            if count or pcount:
                deltaValues.append(0)

            if pcount > 0:
                deltaValues.append(count/pcount)

    return max(deltaValues)


def _evalutePrivacy(
    originalData: Data,
    anonymizedData: Data,
    attributeTypes: dict[str, str],
    config: Config,
    levels: list[int],
    javaApi: JavaApi
) -> Privacy:
    qiNames = getQiNames(anonymizedData)
    privacy = Privacy()
    privacy.kAnonymity = _evaluteKAnonymity(anonymizedData, qiNames)
    privacy.dPresence = _evaluteDPresence(
        originalData,
        anonymizedData,
        attributeTypes,
        qiNames,
        config,
        levels,
        javaApi
    )
    return privacy


def _applyConfigs(
    originalData: Data,
    attributeTypes: dict[str, str],
    config: Config,
    hierarchies: dict[str, Hierarchy],
    javaApi: JavaApi
) -> ARXResult:
    k = config.k
    suppressionLimit = config.suppressionRate

    setDataHierarchies(
        originalData, hierarchies, attributeTypes, javaApi)

    privacyModels = [javaApi.KAnonymity(k)]

    try:
        anonymizedResult = anonymizeData(
                originalData, privacyModels, javaApi, None, suppressionLimit)
    except Py4JJavaError:
        return

    return anonymizedResult


def _evaluateMetricsForAnonymizedData(
    originalData: Data,
    anonymizedData: Data,
    attributeTypes: dict[str, str],
    qiIndices: list[int],
    transformation: str,
    config: Config,
    javaApi: JavaApi,
) -> list[Metrics]:

    levels = []
    for index in range(len(qiIndices)):
        levels.append(transformation[index])

    metrics = Metrics()
    metrics.levels = levels
    metrics.utility = _evaluteUtility(originalData, anonymizedData)
    metrics.privacy = _evalutePrivacy(
        originalData,
        anonymizedData,
        attributeTypes,
        config,
        levels,
        javaApi
    )
    metrics.config = config
    metrics.anonymizedData = getDataFrame(anonymizedData)

    return metrics


def _evaluateMetricsParallel(
    originalData: pd.DataFrame,
    attributeTypes: dict[str, str],
    config: Config,
) -> list[Metrics]:

    processGateway = createJavaGateway()
    javaApi = JavaApi(processGateway, javaApiTable)
    originalData = packArxData(originalData, javaApi)
    hierarchies = packArxHierarchies(config.hierarchies, attributeTypes, javaApi)

    anonymizedResult = _applyConfigs(
                            originalData,
                            attributeTypes,
                            config,
                            hierarchies,
                            javaApi
                        )

    if not anonymizedResult:
        return []

    anonymizedSolutions = anonymizedResult.getLattice().getLevels()
    qiIndices = getQiIndices(originalData)

    result = []
    for solutions in anonymizedSolutions:
        for solution in solutions:

            if str(solution.getAnonymity()) != "ANONYMOUS":
                continue

            try:
                anonymized = anonymizedResult.getOutput(solution, True)
            except Py4JJavaError:
                continue

            anonymizedData = javaApi.Data.create(anonymized.iterator())
            setDataHierarchies(
                    anonymizedData, hierarchies, attributeTypes, javaApi)

            transformation = solution.getTransformation()

            metrics = _evaluateMetricsForAnonymizedData(
                        originalData,
                        anonymizedData,
                        attributeTypes,
                        qiIndices,
                        transformation,
                        config,
                        javaApi
                    )

            result.append(metrics)

    processGateway.close()
    processGateway.shutdown()

    return result


def evaluateMetrics(
    originalData: Data,
    attributeTypes: dict[str, str],
    configs: list[Config],
    numOfProcess: int
) -> list[Metrics]:
    originalData = getDataFrame(originalData)

    pool = Pool(min(numOfProcess, cpu_count() - 1))

    asyncResults = []
    for config in configs:
        result = pool.apply_async(
                    _evaluateMetricsParallel,
                    args=(
                        originalData,
                        attributeTypes,
                        config
                    )
                )

        asyncResults.append(result)

    pool.close()
    pool.join()

    finalResults = []
    for asyncResult in asyncResults:
        finalResults += asyncResult.get()


    return finalResults


if __name__ == "__main__":
    pass
