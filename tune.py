from PETWorks.autotune import autoTune
from PETWorks.attributetypes import QUASI_IDENTIFIER
from PETWorks import report

originalData = "data/delta_test4.csv"
#originalData = "data/delta.csv"


attributeTypes = {
    "zip": QUASI_IDENTIFIER,
    "age": QUASI_IDENTIFIER,
}


def analyzingFunction(anonymizedData):
    data = anonymizedData["age"]
    return data.astype(float).mean()


result = autoTune(originalData, attributeTypes, analyzingFunction, 0.5, 7)
report(result, "json")
