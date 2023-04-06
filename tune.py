from PETWorks.autotune import autoTune
from PETWorks.attributetypes import QUASI_IDENTIFIER
from PETWorks import report

originalData = "data/adult10.csv"
#originalData = "data/adult100.csv"
#originalData = "data/adult1000.csv"


attributeTypes = {
    "sex": QUASI_IDENTIFIER,
    "age": QUASI_IDENTIFIER,
    "race": QUASI_IDENTIFIER,
    "marital-status": QUASI_IDENTIFIER,
    "education": QUASI_IDENTIFIER,
    "native-country": QUASI_IDENTIFIER,
    "workclass": QUASI_IDENTIFIER,
    "occupation": QUASI_IDENTIFIER,
    "salary-class": QUASI_IDENTIFIER,
}


def analyzingFunction(anonymizedData):
    data = anonymizedData["age"]
    return data.astype(float).mean()


result = autoTune(originalData, attributeTypes, analyzingFunction, 0.5, 64)
report(result, "json")
