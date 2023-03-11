from PETWorks.arx import Data, gateway, loadDataFromCsv, loadDataHierarchy
from PETWorks.attributetypes import IDENTIFIER, INSENSITIVE_ATTRIBUTE
from py4j.java_gateway import set_field

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
Groupify = gateway.jvm.org.deidentifier.arx.common.Groupify
TupleWrapper = gateway.jvm.org.deidentifier.arx.common.TupleWrapper
DataSubset = gateway.jvm.org.deidentifier.arx.DataSubset
HashSet = gateway.jvm.java.util.HashSet
DPresence = gateway.jvm.org.deidentifier.arx.criteria.DPresence
HashGroupifyEntry = gateway.jvm.org.deidentifier.arx.framework.check.groupify.HashGroupifyEntry
ARXConfiguration = gateway.jvm.org.deidentifier.arx.ARXConfiguration
KAnonymity = gateway.jvm.org.deidentifier.arx.criteria.KAnonymity
ARXAnonymizer = gateway.jvm.org.deidentifier.arx.ARXAnonymizer
AttributeType = gateway.jvm.org.deidentifier.arx.AttributeType

Int = gateway.jvm.int
String = gateway.jvm.String
ArrayList = gateway.jvm.java.util.ArrayList


def _setDataHierarchies(
    data: Data, hierarchies: dict[str, list[list[str]]],
    attributeTypes: dict
) -> None:
    for attributeName, hierarchy in hierarchies.items():
        data.getDefinition().setAttributeType(attributeName, hierarchy)
        attributeType = attributeTypes.get(attributeName)

        if attributeType == IDENTIFIER:
            data.getDefinition().setAttributeType(
                    attributeName, AttributeType.IDENTIFYING_ATTRIBUTE)

        if attributeType == INSENSITIVE_ATTRIBUTE:
            data.getDefinition().setAttributeType(
                    attributeName, AttributeType.INSENSITIVE_ATTRIBUTE)


def _getGroupify(dataHandle: str, indices: list[int]) -> str:
    numDataRows = dataHandle.getNumRows()
    groupify = Groupify(numDataRows)

    indicesArray = gateway.new_array(Int, len(indices))
    for i in range(len(indices)):
        indicesArray[i] = indices[i]

    for row in range(numDataRows):
        tuple = TupleWrapper(dataHandle, indicesArray, row)
        groupify.add(tuple)

    return groupify


def _getQiIndices(dataHandle: str) -> list[int]:
    qiNames = dataHandle.getDefinition().getQuasiIdentifyingAttributes()
    qiIndices = []
    for qi in qiNames:
        qiIndices.append(dataHandle.getColumnIndexOf(qi))
    return qiIndices


def _isRowSuppressed(
        table: Data, rowIndex: int
) -> bool:
    qiIndices = _getQiIndices(table.getHandle())
    for i in qiIndices:
        if table.getHandle().getValue(rowIndex, i) != "*":
            return False
    return True


def _findAnonymousLevel(
        hier: list[list[str]], value: str
) -> int:
    for i in range(len(hier)):
        for j in range(len(hier[i])):
            if hier[i][j] == value:
                return j
    return -1


def _getAnonymizedData(
        originalData: Data, anonymizedSubset: Data,
        hierarchies: dict[str, list[list[str]]]
) -> str:
    numDataRows = anonymizedSubset.getHandle().getNumRows()

    sampleRowIndex = 1
    allSuppressed = False
    for i in range(numDataRows):
        if not _isRowSuppressed(anonymizedSubset, i):
            sampleRowIndex = i
            break
        allSuppressed = (i == numDataRows - 1)

    qiIndices = _getQiIndices(originalData.getHandle())

    anonymousLevels = []
    for index in qiIndices:
        value = anonymizedSubset.getHandle().getValue(sampleRowIndex, index)
        attributeName = anonymizedSubset.getHandle().getAttributeName(index)
        hierarchy = hierarchies[attributeName].getHierarchy()

        if allSuppressed:
            anonymousLevels.append(hierarchy[0].length)
        else:
            anonymousLevels.append(_findAnonymousLevel(hierarchy, value))

    levelArray = gateway.new_array(Int, len(qiIndices))
    for j in range(len(levelArray)):
        levelArray[j] = anonymousLevels[j]

    arxconfig = ARXConfiguration.create()
    arxconfig.addPrivacyModel(KAnonymity(1))
    anonymizer = ARXAnonymizer()
    result = anonymizer.anonymize(originalData, arxconfig)

    lattice = result.getLattice()
    node = lattice.getNode(levelArray)

    transformedData = result.getOutput(node, True)
    return transformedData


def _compareArray(arrayA, arrayB, qiIndices) -> bool:
    for index in qiIndices:
        if arrayA[index] != arrayB[index]:
            return False
    return True


def _measureDPresence(
    dataHandle: str, subset: Data,
    dMin: float, dMax: float
) -> bool:
    numDataRows = dataHandle.getNumRows()

    qiIndices = _getQiIndices(dataHandle)
    groupedData = _getGroupify(dataHandle, qiIndices)
    groupedSubset = _getGroupify(subset.getHandle(), qiIndices)

    subsetGroup = groupedSubset.first()
    while subsetGroup.getElement():

        pcount = 0
        dataGroup = groupedData.first()
        while dataGroup.getElement():
            dataRow = dataGroup.getElement().getValues()
            subsetRow = subsetGroup.getElement().getValues()
            suppressedRow = ["*"]*len(qiIndices)

            if _compareArray(subsetRow, suppressedRow, qiIndices):
                pcount = numDataRows
                break

            if _compareArray(subsetRow, dataRow, qiIndices):
                pcount = dataGroup.getCount()
                break

            dataGroup = dataGroup.next()
            if not dataGroup:
                break

        dummySubset = DataSubset.create(0, HashSet())
        model = DPresence(dMin, dMax, dummySubset)
        entry = HashGroupifyEntry(None, 0, 0)

        set_field(entry, "count", subsetGroup.getCount())
        set_field(entry, "pcount", pcount)

        if not model.isAnonymous(None, entry):
            return False

        subsetGroup = subsetGroup.next()
        if not subsetGroup:
            break

    return True


def PETValidation(original, subset, _, dataHierarchy, **other):
    dMax = other["dMax"]
    dMin = other["dMin"]
    attributeType = other.get("attributeTypes", None)

    dataHierarchy = loadDataHierarchy(
        dataHierarchy, StandardCharsets.UTF_8, ";"
    )
    original = loadDataFromCsv(original, StandardCharsets.UTF_8, ";")
    subset = loadDataFromCsv(subset, StandardCharsets.UTF_8, ";")

    _setDataHierarchies(original, dataHierarchy, attributeType)
    _setDataHierarchies(subset, dataHierarchy, attributeType)

    anonymizedData = _getAnonymizedData(original, subset, dataHierarchy)
    dPresence = _measureDPresence(anonymizedData, subset, dMin, dMax)

    return {"dMin": dMin,
            "dMax": dMax,
            "d-presence": dPresence}
