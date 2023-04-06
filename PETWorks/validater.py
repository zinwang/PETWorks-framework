from PETWorks.metriceval import Metrics
from PETWorks.arx import getDataFrame, Data
from tqdm import tqdm

from typing import Callable

import sys

def validate(metricsList: list[Metrics]) -> Metrics:
    bestMetrics = None
    bestPrivacyScore = float('-inf')
    bestUtilityScore = float('inf')

    for metrics in tqdm(metricsList, file=sys.stdout):
        kAnonymityScore = metrics.privacy.kAnonymity
        dPresenceScore = 1 - metrics.privacy.dPresence


        privacyScore = kAnonymityScore + dPresenceScore
        utilityScore = metrics.utility.ambiguty + metrics.utility.precision + \
                            metrics.utility.nonUniformEntropy + metrics.utility.aecs

        if privacyScore > bestPrivacyScore or \
                (privacyScore == bestPrivacyScore and \
                    utilityScore < bestUtilityScore):
            bestMetrics = metrics
            bestPrivacyScore = privacyScore
            bestUtilityScore = utilityScore

    return bestMetrics


def makeFilter(
        originalData: Data, analyzingFunction: Callable, acceptedError: float
) -> None:
    def customFilter(solution: Metrics) -> bool:
        originalDataFrame = getDataFrame(originalData)
        originalValue = analyzingFunction(originalDataFrame)
        anonymizedValue = analyzingFunction(solution.anonymizedData)
        error = abs(originalValue - anonymizedValue)
        return error < acceptedError and error > 0


def validateMetrics(
    originalData: Data,
    possibleSolutions: list[Metrics],
    analyzingFunction: Callable,
    error: float
) -> Metrics:
    filterForUser = makeFilter(originalData, analyzingFunction, error)
    selectedSolutions = list(filter(filterForUser, possibleSolutions))
    return validate(selectedSolutions)


if __name__ == "__main__":
    pass
