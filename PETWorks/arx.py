import re
import pandas as pd
import numpy as np
from os import PathLike, listdir
from os.path import join
from typing import List
from PETWorks.attributetypes import IDENTIFIER, INSENSITIVE_ATTRIBUTE

from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JJavaError


PATH_TO_ARX_LIBRARY = "arx/lib/libarx-3.9.0.jar"
gateway = JavaGateway.launch_gateway(
    classpath=PATH_TO_ARX_LIBRARY, die_on_exit=True
)

Data = gateway.jvm.org.deidentifier.arx.Data
Charset = gateway.jvm.java.nio.charset.Charset
CSVHierarchyInput = gateway.jvm.org.deidentifier.arx.io.CSVHierarchyInput
Hierarchy = gateway.jvm.org.deidentifier.arx.AttributeType.Hierarchy
ARXConfiguration = gateway.jvm.org.deidentifier.arx.ARXConfiguration
KAnonymity = gateway.jvm.org.deidentifier.arx.criteria.KAnonymity
ARXAnonymizer = gateway.jvm.org.deidentifier.arx.ARXAnonymizer
ARXResult = gateway.jvm.org.deidentifier.arx.ARXResult
AttributeType = gateway.jvm.org.deidentifier.arx.AttributeType
Int = gateway.jvm.int

javaApiTable = {
    "Data": "jvm.org.deidentifier.arx.Data",
    "Charset": "jvm.java.nio.charset.Charset",
    "CSVHierarchyInput": "jvm.org.deidentifier.arx.io.CSVHierarchyInput",
    "Hierarchy": "jvm.org.deidentifier.arx.AttributeType.Hierarchy",
    "DefaultHierarchy": "jvm.org.deidentifier.arx.AttributeType.Hierarchy.DefaultHierarchy",
    "ARXConfiguration": "jvm.org.deidentifier.arx.ARXConfiguration",
    "KAnonymity": "jvm.org.deidentifier.arx.criteria.KAnonymity",
    "ARXAnonymizer": "jvm.org.deidentifier.arx.ARXAnonymizer",
    "AttributeType": "jvm.org.deidentifier.arx.AttributeType",
    "Int": "jvm.int",
    "String": "jvm.java.lang.String",
    "new_array": "new_array"
}


class JavaApi:
    def __init__(
        self,
        gatewayObject: JavaGateway,
        apiTable: dict[str, str]
    ):
        for name, javaApi in apiTable.items():
            api = eval("gatewayObject." + javaApi)
            setattr(self, name, api)


def createJavaGateway() -> JavaGateway:
    return JavaGateway.launch_gateway(
        classpath=PATH_TO_ARX_LIBRARY
    )


def loadDataFromCsv(
    path: PathLike,
    charset: Charset,
    delimiter: str,
    javaApi: JavaApi
) -> Data:
    return javaApi.Data.create(path, charset, delimiter)


def loadDataHierarchy(
    path: PathLike,
    charset: Charset,
    delimiter: str,
    javaApi: JavaApi
) -> dict[str, Hierarchy]:
    hierarchies = {}
    for filename in listdir(path):
        result = re.match(".*hierarchy_(.*?).csv", filename)
        if result is None:
            continue

        attributeName = result.group(1)

        dataHierarchyFile = join(path, filename)
        hierarchy = javaApi.CSVHierarchyInput(
            dataHierarchyFile, charset, delimiter
        ).getHierarchy()

        hierarchies[attributeName] = javaApi.Hierarchy.create(hierarchy)

    return hierarchies


def setDataHierarchies(
    data: Data,
    hierarchies: dict[str, Hierarchy],
    attributeTypes: dict[str, str],
    javaApi: JavaApi
) -> None:
    for attributeName, hierarchy in hierarchies.items():
        data.getDefinition().setAttributeType(attributeName, hierarchy)
        attributeType = attributeTypes.get(attributeName)

        if attributeType == IDENTIFIER:
            data.getDefinition().setAttributeType(
                attributeName, javaApi.AttributeType.IDENTIFYING_ATTRIBUTE
            )

        if attributeType == INSENSITIVE_ATTRIBUTE:
            data.getDefinition().setAttributeType(
                attributeName, javaApi.AttributeType.INSENSITIVE_ATTRIBUTE
            )


def getQiNames(data: Data) -> list[str]:
    dataHandle = data.getHandle()
    qiNameSet = dataHandle.getDefinition().getQuasiIdentifyingAttributes()
    qiNames = [qiName for qiName in qiNameSet]
    qiNames.sort(key=dataHandle.getColumnIndexOf)
    return qiNames


def getQiIndices(data: Data) -> list[int]:
    dataHandle = data.getHandle()
    qiNames = getQiNames(data)
    qiIndices = []
    for qiName in qiNames:
        qiIndices.append(dataHandle.getColumnIndexOf(qiName))

    return qiIndices


def findAnonymousLevel(hierarchy: list[list[str]], value: str) -> int:
    for hierarchyRow in hierarchy:
        for level in range(len(hierarchyRow)):
            if hierarchyRow[level] == value:
                return level
    return -1


def getAnonymousLevels(
    anonymizedSubset: Data, hierarchies: dict[str, Hierarchy]
) -> list[int]:
    subsetDataFrame = getDataFrame(anonymizedSubset)
    subsetRowNum = len(subsetDataFrame)

    qiIndices = getQiIndices(anonymizedSubset)

    sampleRowIndex = -1
    allSuppressed = False
    for subsetRowIndex in range(subsetRowNum):
        for qiIndex in qiIndices:
            if subsetDataFrame.iloc[subsetRowIndex][qiIndex] != "*":
                sampleRowIndex = subsetRowIndex
                break

        if sampleRowIndex != -1:
            break

        allSuppressed = (subsetRowIndex == subsetRowNum - 1)

    anonymousLevels = []
    for qiIndex in qiIndices:
        value = subsetDataFrame.iloc[sampleRowIndex][qiIndex]
        attributeName = subsetDataFrame.columns[qiIndex]
        hierarchy = hierarchies[attributeName].getHierarchy()

        if allSuppressed:
            anonymousLevels.append(len(hierarchy[0]) - 1)
            continue

        anonymousLevels.append(findAnonymousLevel(hierarchy, value))

    return anonymousLevels


def getDataFrame(data: Data) -> pd.DataFrame:
    dataHandle = data.getHandle()
    rowNum = dataHandle.getNumRows()
    colNum = dataHandle.getNumColumns()

    data = []
    for rowIndex in range(rowNum):
        row = []
        for colIndex in range(colNum):
            row.append(dataHandle.getValue(rowIndex, colIndex))
        data.append(row)

    colNames = [
        dataHandle.getAttributeName(colIndex) for colIndex in range(colNum)
    ]

    return pd.DataFrame(data, columns=colNames)


def getSubsetIndices(
    table: Data,
    subset: Data,
) -> list[int]:
    qiNames = getQiNames(table)
    qiIndices = getQiIndices(table)

    tableDataFrame = getDataFrame(table)
    groupedSubset = getDataFrame(subset).groupby(qiNames)

    tableRowNum = len(tableDataFrame)

    subsetIndices = []
    for _, subsetGroup in groupedSubset:
        subsetGroupList = subsetGroup.values.tolist()
        filter = pd.Series(True, index=range(tableRowNum))
        for qiName, qiIndex in zip(qiNames, qiIndices):
            filter &= (tableDataFrame[qiName] == subsetGroupList[0][qiIndex])

        subsetIndices += np.flatnonzero(filter).tolist()[:len(subsetGroupList)]

    return subsetIndices


def packArxData(
    data: pd.DataFrame,
    javaApi: JavaApi
) -> Data:
    rowNum, colNum = data.shape

    arxData = javaApi.Data.create()

    colNameData = javaApi.new_array(javaApi.String, colNum)
    colNames = data.columns.values.tolist()
    for colIndex in range(colNum):
        colNameData[colIndex] = colNames[colIndex]
    arxData.add(colNameData)

    for rowIndex in range(rowNum):
        row = javaApi.new_array(javaApi.String, colNum)

        for colIndex in range(colNum):
            row[colIndex] = data.iloc[rowIndex, colIndex]

        arxData.add(row)

    return arxData


def packArxHierarchies(
    hierarchies: dict[str, np.chararray],
    attributeTypes: dict[str, str],
    javaApi: JavaApi
) -> dict[str, Hierarchy]:

    arxHierarchies = {}

    for attributeName, hierarchy in hierarchies.items():

        _, colNum = hierarchy.shape

        arxQiHierarchy = javaApi.DefaultHierarchy.create()

        for hierarchyRow in hierarchy:
            row = javaApi.new_array(javaApi.String, colNum)
            for colIndex in range(colNum):
                row[colIndex] = hierarchyRow[colIndex]

            arxQiHierarchy.add(row)

        arxHierarchies[attributeName] = arxQiHierarchy
    return arxHierarchies


def anonymizeData(
    original: Data,
    privacyModels: list[str],
    javaApi: JavaApi,
    utilityModel: str = None,
    suppressionLimit: float = 0.0
) -> ARXResult:

    arxConfig = javaApi.ARXConfiguration.create()

    arxConfig.setSuppressionLimit(suppressionLimit)

    for privacyModel in privacyModels:
        arxConfig.addPrivacyModel(privacyModel)

    if utilityModel:
        arxConfig.setQualityModel(utilityModel)

    try:
        anonymizer = javaApi.ARXAnonymizer()
        anonymizedData = anonymizer.anonymize(original, arxConfig)
    except Py4JJavaError as e:
        raise e

    return anonymizedData


def applyAnonymousLevels(
    original: Data,
    anonymousLevels: list[int],
    hierarchies: dict[str, Hierarchy],
    attributeTypes: dict[str, str],
    javaApi: JavaApi
) -> Data:
    levels = javaApi.new_array(javaApi.Int, len(anonymousLevels))
    for i in range(len(anonymousLevels)):
        levels[i] = anonymousLevels[i]

    privacyModels = [javaApi.KAnonymity(1)]

    try:
        anonymizedDatum = anonymizeData(original, privacyModels, javaApi)
    except Py4JJavaError:
        return

    lattice = anonymizedDatum.getLattice()
    node = lattice.getNode(levels)

    result = javaApi.Data.create(
            anonymizedDatum.getOutput(node, True).iterator())

    setDataHierarchies(result, hierarchies, attributeTypes, javaApi)

    return result
