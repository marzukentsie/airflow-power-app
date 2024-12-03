## Overview

In the `part-two` directory, we have utilized a script named `script.py` to process the original `YellowTaxiData.csv` file. The purpose of this script was to reduce the number of rows in the dataset to 100 rows. This was necessary because the original file was too large to be sent as an attachment for testing the cloud flow.

## Process

1. **Original File**: `YellowTaxiData.csv` (not included in the repository)
2. **Script Used**: `script.py`
3. **Operation**: Reduced the rows to 100
4. **New File**: `YellowTaxiData.csv` (not included in the repository)

## Purpose

The reduced file size ensures that the dataset can be easily attached and sent for testing the cloud flow without any issues related to file size limitations.

## Directory Structure

```
part-two/
│
├── script.py
├── YellowTaxiData.csv (original, gitignored)
└── YellowTaxiData.csv (reduced to 100 rows, gitignored)
```

## Conclusion

By reducing the size of the `YellowTaxiData.csv` file, we have made it feasible to test the cloud flow effectively. The `script.py` in the `part-two` directory handles this reduction process efficiently. Note that the `.csv` files are gitignored and not included in the repository due to their size.
