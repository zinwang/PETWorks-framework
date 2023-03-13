# PETWorks-framework

A framework for validating PET enhanced data.

Data Privacy is the keystone promoting stronger and deeper analysis of problem in our society. PET (Privacy Enhancing Tool) is a great helper for making all these happened. However, a trust-worthy and easy-to-use validation tool for PET is still rare. 

Here we provide a framework dealing with the validation problem of PET enhanced data.

### Showcase

#### Validation of Data Processed With Federated Learning

```python
from PETWorks import dataProcess, PETValidation, report

gradient = "/home/Doc/gradient"
model = "/home/Doc/model"
originalData = "/home/Doc/o.png"

recoveredData = dataProcess(model, gradient, "FL", "recover")
result = PETValidation(recoveredData, originalData, "FL")
report(result, "web")
```

#### Measurement of the Re-Identification Risk

```python
from PETWorks import PETValidation, report

originalData = "data/deidentifiedData.csv"

result = PETValidation(originalData, None, "ReidentificationRisk")
report(result, "json")
```

Execution Result

```bash
{
    "Re-identification Risk": 1.0
}
```

#### Measurement of the Ambiguity Metric

```python
from PETWorks import PETValidation, report
from PETWorks.attributetypes import QUASI_IDENTIFIER

originalData = "data/adult.csv"
anonymizedData = "data/adult_anonymized.csv"
dataHierarchy = "data/adult_hierarchy"

attributeTypes = {
    "age": QUASI_IDENTIFIER,
    "sex": QUASI_IDENTIFIER,
}

result = PETValidation(
    originalData, anonymizedData, "Ambiguity", dataHierarchy=dataHierarchy,
    attributeTypes=attributeTypes
)
report(result, "json")
```

Execution Result

```bash
$ python3 ambiguity.py
{
    "ambiguity": 0.7271401100722763
}
```

#### Measurement of the Precision Metric

```python
from PETWorks import PETValidation, report
from PETWorks.attributetypes import QUASI_IDENTIFIER

originalData = "data/adult.csv"
anonymizedData = "data/adult_anonymized.csv"
dataHierarchy = "data/adult_hierarchy"

attributeTypes = {
    "age": QUASI_IDENTIFIER,
    "sex": QUASI_IDENTIFIER,
}

result = PETValidation(
    originalData, anonymizedData, "Precision", dataHierarchy=dataHierarchy,
    attributeTypes=attributeTypes
)
report(result, "json")
```

Execution Result
```bash
$ python3 precision.py
{
    "precision": 0.7271401100722763
}
```


#### Measurement of the Non-Uniform Entropy Metric

```python
from PETWorks import PETValidation, report
from PETWorks.attributetypes import QUASI_IDENTIFIER

originalData = "data/adult.csv"
anonymizedData = "data/adult_anonymized.csv"
dataHierarchy = "data/adult_hierarchy"

attributeTypes = {
    "age": QUASI_IDENTIFIER,
    "sex": QUASI_IDENTIFIER,
}

result = PETValidation(
    originalData, anonymizedData, "Non-Uniform Entropy",
    dataHierarchy=dataHierarchy, attributeTypes=attributeTypes
)
report(result, "json")
```

Execution Result
```python
$ python nonUniformEntropy.py
{
    "Non-Uniform Entropy": 0.7229833454874146
}
```

#### Measurement of the AECS Metric

```python
from PETWorks import PETValidation, report
from PETWorks.attributetypes import QUASI_IDENTIFIER

originalData = "data/adult.csv"
anonymizedData = "data/adult_anonymized.csv"

attributeTypes = {
    "age": QUASI_IDENTIFIER,
    "sex": QUASI_IDENTIFIER,
}

result = PETValidation(
    originalData, anonymizedData, "AECS", attributeTypes=attributeTypes
    )
report(result, "json")
```

Execution Result
```python
$ python aecs.py
{
    "AECS": 0.9982189856691079
}
```

#### Compute the k-anonymity

```python
from PETWorks import PETValidation, report
from PETWorks.attributetypes import QUASI_IDENTIFIER

anonymizedData = "data/adult_anonymized.csv"

attributeTypes = {
    "age": QUASI_IDENTIFIER,
    "sex": QUASI_IDENTIFIER,
}

result = PETValidation(
        None, anonymizedData, "k-anonymity", attributeTypes=attributeTypes, k=6
)
report(result, "json")
```

Execution Result
```python
$ python3 k-anonymity.py
{
    "k": 6,
    "k-anonymity": true
}
```


#### Compute the δ-presence
```python
from PETWorks import PETValidation, report
from PETWorks.attributetypes import SENSITIVE_ATTRIBUTE, QUASI_IDENTIFIER


origin = "data/delta.csv"
anonymized = "data/delta_anonymized.csv"
dataHierarchy = "data/delta_hierarchy"

attributeTypes = {
    "zip": QUASI_IDENTIFIER,
    "age": QUASI_IDENTIFIER,
    "nationality": QUASI_IDENTIFIER,
    "salary-class": SENSITIVE_ATTRIBUTE
}

result = PETValidation(
        origin, anonymized, "d-presence", dataHierarchy=dataHierarchy, attributeTypes=attributeTypes, dMin=1/2, dMax=2/3
    )
report(result, "json")
```

Execution Result
```python
$ python3 d-presence.py
{
    "dMin": 0.5,
    "dMax": 0.6666666666666666,
    "d-presence": true
}
```



### How it works?
| Module                    | Description                                                                                                                           |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| PET Enhanced Data Process | This module aims to load and process (e.g. recover) PET-enhanced data accordingly to the data and the PET it used.                    |                                                                                                                                       |
| PET Validation            | This module has validation methodologies built in. Returning the result of the validation.                                            |
| Report                    | This module handles the output format of the report.  It could be text-based on the terminal or GUI-based showing on the web browser. |

#### Web Report

Here is the showcase of the web report.

![](https://i.imgur.com/p9wE8BP.png)

The web report also shows the process of recovery.

![](https://i.imgur.com/tCtVqBu.png)

### Current Status
This project is now maintained by Telecom Technology Center, Taiwan. We're now providing just a really simple showcase demonstrating how we can use this framework to validate PET protection of the data using the technology of federated learning. We still need plenty of implementation for every module. This is a very early stage project. Stay tuned!  
