from PETWorks.autoturn import generateAnonymityConfigs, findQualifiedAnonymityConfigs
from PETWorks.attributetypes import QUASI_IDENTIFIER, SENSITIVE_ATTRIBUTE
import pandas as pd

originalData="data/adult10.csv"
dataHierarchy="data/adult_hierarchy"
confout = "tests/report/combination.csv"
out = "tests/report/result.txt"

confout = "conf.csv"
out = "result.csv"

attributeTypes={
    "sex": QUASI_IDENTIFIER,
    "age": QUASI_IDENTIFIER,
    "race": QUASI_IDENTIFIER,
    "marital-status": QUASI_IDENTIFIER,
    "education": QUASI_IDENTIFIER,
    "native-country": QUASI_IDENTIFIER,
    "workclass": SENSITIVE_ATTRIBUTE,
    "occupation": SENSITIVE_ATTRIBUTE,
    "salary-class": SENSITIVE_ATTRIBUTE,
}

def analysisFunction(data: pd.DataFrame) -> float:
    return data["age"].astype(float).max()

generateAnonymityConfigs(originalData, dataHierarchy, confout, 5, 3)
findQualifiedAnonymityConfigs(
    originalData,
    dataHierarchy,
    confout,
    attributeTypes,
    analysisFunction,
    3,
    out
)
