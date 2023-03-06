from PETWorks.arx import Data, gateway, loadDataFromCsv
from py4j.java_gateway import set_field

StandardCharsets = gateway.jvm.java.nio.charset.StandardCharsets
Groupify = gateway.jvm.org.deidentifier.arx.common.Groupify
TupleWrapper = gateway.jvm.org.deidentifier.arx.common.TupleWrapper
DataSubset = gateway.jvm.org.deidentifier.arx.DataSubset
HashSet = gateway.jvm.java.util.HashSet
DPresence = gateway.jvm.org.deidentifier.arx.criteria.DPresence
HashGroupifyEntry = gateway.jvm.org.deidentifier.arx.framework.check.groupify.HashGroupifyEntry


Int = gateway.jvm.int
String = gateway.jvm.String
ArrayList = gateway.jvm.java.util.ArrayList


def _getGroupify(data: Data, indices: list[int]) -> str:
    numDataRows = data.getHandle().getNumRows()
    groupify = Groupify(numDataRows)

    indicesArray = gateway.new_array(Int, len(indices))
    for i in range(len(indices)):
        indicesArray[i] = indices[i]

    for row in range(numDataRows):
        tuple = TupleWrapper(data.getHandle(), indicesArray, row)
        groupify.add(tuple)

    return groupify


def _measureDPresence(
        data: Data, subsetIndices: list[int],
        dMin: float, dMax: float
        ) -> bool:

    numDataRows = data.getHandle().getNumRows()
    numDataColumns = data.getHandle().getNumColumns()

    dataSubset = gateway.new_array(String, numDataRows, numDataColumns)
    for i in range(len(subsetIndices)):

        index = subsetIndices[i]
        for j in range(numDataColumns):
            dataSubset[index][j] = data.getHandle().getValue(index, j)

    indices = [i for i in range(numDataColumns)]
    groupedData = _getGroupify(data, indices)

    group = groupedData.first()
    while group.getElement():

        count = 0
        rowData = group.getElement().getValues()
        for i in range(numDataRows):
            if not (i in subsetIndices):
                continue

            isInDataSubset = True
            for j in range(numDataColumns):
                if rowData[j] != dataSubset[i][j]:
                    isInDataSubset = False
                    break

            if isInDataSubset:
                count += 1

        indicesArrayList = ArrayList()
        for index in subsetIndices:
            indicesArrayList.append(index)

        subset = DataSubset.create(data, HashSet(indicesArrayList))
        model = DPresence(dMin, dMax, subset)
        entry = HashGroupifyEntry(None, 0, 0)

        set_field(entry, "count", count)
        set_field(entry, "pcount", group.getCount())
        if not model.isAnonymous(None, entry):
            return False

        group = group.next()
        if not group:
            break

    return True


def PETValidation(foo, data, bar, **other):
    dMax = other["dMax"]
    dMin = other["dMin"]
    subsetIndices = other["subsetIndices"]

    data = loadDataFromCsv(data, StandardCharsets.UTF_8, ";")

    dPresense = _measureDPresence(data, subsetIndices, dMin, dMax)

    return {"dMin": dMin,
            "dMax": dMax,
            "d-presence": dPresense}
