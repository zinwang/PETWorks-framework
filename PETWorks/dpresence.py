from PETWorks.arx import Data, gateway, loadDataFromCsv, loadDataHierarchy
from PETWorks.arx import setDataHierarchies, getDataFrame, getQiNames
from PETWorks.arx import getAnonymousLevels, applyAnonymousLevels
from py4j.java_gateway import set_field
from PETWorks.arx import JavaApi, javaApiTable

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
DataSubset = gateway.jvm.org.deidentifier.arx.DataSubset
HashSet = gateway.jvm.java.util.HashSet
DPresence = gateway.jvm.org.deidentifier.arx.criteria.DPresence
HashGroupifyEntry = (
    gateway.jvm.org.deidentifier.arx.framework.check.groupify.HashGroupifyEntry
)


def _measureDPresence(
    data: Data, subset: Data, dMin: float, dMax: float
) -> bool:
    qiNames = getQiNames(data)

    groupedData = getDataFrame(data).groupby(qiNames)
    groupedSubset = getDataFrame(subset).groupby(qiNames)

    for _, subsetGroup in groupedSubset:
        count = 0
        pcount = 0

        subsetGroupList = subsetGroup.values.tolist()
        count = len(subsetGroupList)

        for _, dataGroup in groupedData:
            dataGroupList = dataGroup.values.tolist()

            if subsetGroupList[0] == dataGroupList[0]:
                pcount = len(dataGroup)

        dummySubset = DataSubset.create(0, HashSet())
        model = DPresence(dMin, dMax, dummySubset)
        entry = HashGroupifyEntry(None, 0, 0)

        set_field(entry, "count", count)
        set_field(entry, "pcount", pcount)

        if not model.isAnonymous(None, entry):
            return False

    return True


def PETValidation(original, subset, _, dataHierarchy, **other):
    dMax = other["dMax"]
    dMin = other["dMin"]
    attributeType = other.get("attributeTypes", None)

    javaApi = JavaApi(gateway, javaApiTable)
    dataHierarchy = loadDataHierarchy(
        dataHierarchy, StandardCharsets.UTF_8, ";", javaApi
    )
    original = loadDataFromCsv(original, StandardCharsets.UTF_8, ";", javaApi)
    subset = loadDataFromCsv(subset, StandardCharsets.UTF_8, ";", javaApi)

    setDataHierarchies(original, dataHierarchy, attributeType, javaApi)
    setDataHierarchies(subset, dataHierarchy, attributeType, javaApi)

    anonymousLevels = getAnonymousLevels(subset, dataHierarchy)
    anonymizedData = applyAnonymousLevels(
            original, anonymousLevels, dataHierarchy, attributeType, javaApi)

    dPresence = _measureDPresence(anonymizedData, subset, dMin, dMax)

    return {"dMin": dMin,
            "dMax": dMax,
            "d-presence": dPresence}
