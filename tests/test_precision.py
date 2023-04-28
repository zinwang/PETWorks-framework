from PETWorks.precision import PETValidation
from PETWorks.attributetypes import QUASI_IDENTIFIER
from typing import Dict
import pytest

@pytest.fixture(scope="module")
def attributeTypesForAdult() -> Dict[str, str]:
    attributeTypes = {
        "age": QUASI_IDENTIFIER,
        "education": QUASI_IDENTIFIER,
        "marital-status": QUASI_IDENTIFIER,
        "native-country": QUASI_IDENTIFIER,
        "occupation": QUASI_IDENTIFIER,
        "race": QUASI_IDENTIFIER,
        "salary-class": QUASI_IDENTIFIER,
        "sex": QUASI_IDENTIFIER,
        "workclass": QUASI_IDENTIFIER,
    }
    return attributeTypes

def testPETValidation(
    DATASET_PATH_ADULT,
    attributeTypesForAdult
):
    assert (
        PETValidation(
            DATASET_PATH_ADULT["originalData"],
            DATASET_PATH_ADULT["anonymizedData"],
            "Precision",
            dataHierarchy=DATASET_PATH_ADULT["dataHierarchy"],
            attributeTypes=attributeTypesForAdult
        )["precision"]
        == 0.7271401100722763
    )
