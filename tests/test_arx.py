from typing import Dict

import pandas as pd
import pytest
from py4j.java_collections import JavaArray

from PETWorks.arx import (
    Data,
    JavaApi,
    loadDataFromCsv,
    loadDataHierarchy,
    setDataHierarchies,
    arxAnonymize,
    anonymizeData,
    getDataFrame,
)
from PETWorks.attributetypes import QUASI_IDENTIFIER, SENSITIVE_ATTRIBUTE


@pytest.fixture(scope="session")
def javaApi() -> JavaApi:
    return JavaApi()


@pytest.fixture(scope="module")
def errorAttributeTypesForAdult() -> Dict[str, str]:
    attributeTypes = {
        "age": "wrong attribute",
    }
    return attributeTypes


@pytest.fixture(scope="module")
def arxDataAdult(DATASET_PATH_ADULT, javaApi) -> Data:
    return loadDataFromCsv(
        DATASET_PATH_ADULT["originalData"],
        javaApi.StandardCharsets.UTF_8,
        ";",
        javaApi,
    )


@pytest.fixture(scope="module")
def arxHierarchyAdult(DATASET_PATH_ADULT, javaApi) -> Dict[str, JavaArray]:
    return loadDataHierarchy(
        DATASET_PATH_ADULT["dataHierarchy"],
        javaApi.StandardCharsets.UTF_8,
        ";",
        javaApi,
    )


def testSetDataHierarchiesErrorAttributeTypes(
    arxDataAdult, arxHierarchyAdult, errorAttributeTypesForAdult, javaApi
):
    with pytest.raises(ValueError):
        setDataHierarchies(
            arxDataAdult,
            arxHierarchyAdult,
            errorAttributeTypesForAdult,
            javaApi,
        )


def testSetDataHierarchiesNoHierarchies(arxDataAdult, attributeTypesForAdult, javaApi):
    setDataHierarchies(arxDataAdult, None, attributeTypesForAdult, javaApi)
    dataDefinition = arxDataAdult.getDefinition()
    maritalStatusHierarchy = dataDefinition.getHierarchy("marital-status")
    assert len(maritalStatusHierarchy) == 0

    nativeCountryHierarchy = dataDefinition.getHierarchy("native-country")
    assert len(nativeCountryHierarchy) == 0

    occupationHierarchy = dataDefinition.getHierarchy("occupation")
    assert len(occupationHierarchy) == 0

    assert dataDefinition.getAttributeType("age").toString() == "IDENTIFYING_ATTRIBUTE"
    assert (
        dataDefinition.getAttributeType("education").toString()
        == "IDENTIFYING_ATTRIBUTE"
    )
    assert dataDefinition.getAttributeType("race").toString() == "INSENSITIVE_ATTRIBUTE"
    assert (
        dataDefinition.getAttributeType("salary-class").toString()
        == "INSENSITIVE_ATTRIBUTE"
    )
    assert dataDefinition.getAttributeType("sex").toString() == "INSENSITIVE_ATTRIBUTE"
    assert (
        dataDefinition.getAttributeType("workclass").toString()
        == "INSENSITIVE_ATTRIBUTE"
    )

def testSetDataHierarchiesSensitiveEnabled(
        arxDataAdult, arxHierarchyAdult, javaApi
):
    attributeTypes = {
        "age": SENSITIVE_ATTRIBUTE,
    }
    setDataHierarchies(arxDataAdult, arxHierarchyAdult, attributeTypes, javaApi, True)

    dataDefinition = arxDataAdult.getDefinition()
    assert (
            dataDefinition.getAttributeType("age").toString()
            == "SENSITIVE_ATTRIBUTE"
    )

def testSetDataHierarchies(
    arxDataAdult, arxHierarchyAdult, attributeTypesForAdult, javaApi
):
    setDataHierarchies(arxDataAdult, arxHierarchyAdult, attributeTypesForAdult, javaApi)

    dataDefinition = arxDataAdult.getDefinition()
    maritalStatusHierarchy = dataDefinition.getHierarchy("marital-status")
    assert len(maritalStatusHierarchy) == 7
    assert len(maritalStatusHierarchy[0]) == 3

    nativeCountryHierarchy = dataDefinition.getHierarchy("native-country")
    assert len(nativeCountryHierarchy) == 41
    assert len(nativeCountryHierarchy[0]) == 3

    occupationHierarchy = dataDefinition.getHierarchy("occupation")
    assert len(occupationHierarchy) == 14
    assert len(occupationHierarchy[0]) == 3

    assert dataDefinition.getAttributeType("age").toString() == "IDENTIFYING_ATTRIBUTE"
    assert (
        dataDefinition.getAttributeType("education").toString()
        == "IDENTIFYING_ATTRIBUTE"
    )
    assert dataDefinition.getAttributeType("race").toString() == "INSENSITIVE_ATTRIBUTE"
    assert (
        dataDefinition.getAttributeType("salary-class").toString()
        == "INSENSITIVE_ATTRIBUTE"
    )
    assert dataDefinition.getAttributeType("sex").toString() == "INSENSITIVE_ATTRIBUTE"
    assert (
        dataDefinition.getAttributeType("workclass").toString()
        == "INSENSITIVE_ATTRIBUTE"
    )

def testGetDataFrameWithNone():
    assert getDataFrame(None).empty is True

def testGetDataFrame(arxDataAdult):
    assert len(getDataFrame(arxDataAdult)) == 30163

def testAnonymizeData(
    arxDataAdult, arxHierarchyAdult, attributeTypesForAdultAllQi, javaApi
):
    setDataHierarchies(
        arxDataAdult, arxHierarchyAdult, attributeTypesForAdultAllQi, javaApi
    )

    assert (
        anonymizeData(
            arxDataAdult,
            [javaApi.KAnonymity(5)],
            javaApi,
            javaApi.createPrecomputedEntropyMetric(0.1, True),
            0.04,
        )
        .getGlobalOptimum()
        .getHighestScore()
        .toString()
        == "255559.85455731067"
    )


def testArxAnonymizeWithKAnonymity(
    arxDataAdult, arxHierarchyAdult, attributeTypesForAdultAllQi, javaApi
):
    setDataHierarchies(
        arxDataAdult, arxHierarchyAdult, attributeTypesForAdultAllQi, javaApi
    )

    result = getDataFrame(
        arxAnonymize(
            arxDataAdult,
            0.04,
            [javaApi.KAnonymity(5)],
            javaApi.createLossMetric(),
            javaApi,
        )
    )

    assert result.equals(
        pd.read_csv("data/KAnonymization.csv", sep=";", skipinitialspace=True)
    )


def testArxAnonymizeWithLDiversity(arxDataAdult, arxHierarchyAdult, javaApi):

    attributeTypes = {
        "age": QUASI_IDENTIFIER,
        "education": QUASI_IDENTIFIER,
        "marital-status": QUASI_IDENTIFIER,
        "native-country": QUASI_IDENTIFIER,
        "occupation": SENSITIVE_ATTRIBUTE,
        "race": QUASI_IDENTIFIER,
        "salary-class": QUASI_IDENTIFIER,
        "sex": QUASI_IDENTIFIER,
        "workclass": QUASI_IDENTIFIER,
    }

    setDataHierarchies(
        arxDataAdult, arxHierarchyAdult, attributeTypes, javaApi, True
    )

    result = getDataFrame(
        arxAnonymize(
            arxDataAdult,
            0.04,
            [javaApi.DistinctLDiversity("occupation", 5)],
            javaApi.createLossMetric(),
            javaApi,
        )
    )

    assert result.equals(
        pd.read_csv("data/LAnonymization.csv", sep=";", skipinitialspace=True)
    )


def testArxAnonymizeWithDPresence(
    arxDataAdult, arxHierarchyAdult, attributeTypesForAdultAllQi, javaApi
):
    setDataHierarchies(
        arxDataAdult, arxHierarchyAdult, attributeTypesForAdultAllQi, javaApi, True
    )

    subsetData = loadDataFromCsv(
        "data/adult10.csv", javaApi.StandardCharsets.UTF_8, ";", javaApi
    )
    dataSubset = javaApi.DataSubset.create(arxDataAdult, subsetData)

    result = getDataFrame(
        arxAnonymize(
            arxDataAdult,
            0.05,
            [javaApi.DPresence(0.0, 0.2, dataSubset)],
            javaApi.createLossMetric(),
            javaApi,
        )
    )
    result["age"] = result["age"].astype(float)
    assert result.equals(
        pd.read_csv("data/DAnonymization.csv", sep=";", skipinitialspace=True)
    )


def testArxAnonymizeWithOrderedDistanceTCloseness(
    arxDataAdult, arxHierarchyAdult, javaApi
):
    attributeTypes = {
        "age": SENSITIVE_ATTRIBUTE,
        "education": QUASI_IDENTIFIER,
        "marital-status": QUASI_IDENTIFIER,
        "native-country": QUASI_IDENTIFIER,
        "occupation": QUASI_IDENTIFIER,
        "race": QUASI_IDENTIFIER,
        "salary-class": QUASI_IDENTIFIER,
        "sex": QUASI_IDENTIFIER,
        "workclass": QUASI_IDENTIFIER,
    }

    setDataHierarchies(arxDataAdult, arxHierarchyAdult, attributeTypes, javaApi, True)

    result = getDataFrame(
        arxAnonymize(
            arxDataAdult,
            0.04,
            [javaApi.OrderedDistanceTCloseness("age", 0.2)],
            javaApi.createLossMetric(),
            javaApi,
        )
    )
    result["age"] = result["age"].astype(int)
    assert result.equals(
        pd.read_csv("data/OrderedTAnonymization.csv", sep=";", skipinitialspace=True)
    )


def testArxAnonymizeWithHierarchicalDistanceTCloseness(
    arxDataAdult, arxHierarchyAdult, javaApi
):
    attributeTypes = {
        "age": QUASI_IDENTIFIER,
        "education": QUASI_IDENTIFIER,
        "marital-status": QUASI_IDENTIFIER,
        "native-country": QUASI_IDENTIFIER,
        "occupation": SENSITIVE_ATTRIBUTE,
        "race": QUASI_IDENTIFIER,
        "salary-class": QUASI_IDENTIFIER,
        "sex": QUASI_IDENTIFIER,
        "workclass": QUASI_IDENTIFIER,
    }

    setDataHierarchies(arxDataAdult, arxHierarchyAdult, attributeTypes, javaApi, True)

    result = getDataFrame(
        arxAnonymize(
            arxDataAdult,
            0.04,
            [
                javaApi.HierarchicalDistanceTCloseness(
                    "occupation", 0.2, arxHierarchyAdult.get("occupation")
                )
            ],
            javaApi.createLossMetric(),
            javaApi,
        )
    )

    assert result.equals(
        pd.read_csv(
            "data/HierarchicalTAnonymization.csv",
            sep=";",
            skipinitialspace=True,
        )
    )
