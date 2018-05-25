/*
 * Copyright (C) 2017 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.pitt.dbmi.data.validation.tabular;

import edu.pitt.dbmi.data.Delimiter;
import edu.pitt.dbmi.data.reader.tabular.MixedVarInfo;
import edu.pitt.dbmi.data.validation.MessageType;
import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * May 24, 2017 12:24:12 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedTabularDataFileValidation extends AbstractTabularDataValidation {

    private final int numberOfDiscreteCategories;

    public MixedTabularDataFileValidation(int numberOfDiscreteCategories, File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
        this.numberOfDiscreteCategories = numberOfDiscreteCategories;
    }

    @Override
    protected void validateDataFromFile(int[] excludedColumns) throws IOException {
        MixedVarInfo[] varInfos = validateMixedVariables(excludedColumns);
        varInfos = analysMixedVariableValidation(varInfos, excludedColumns);

        int numOfDiscrete = 0;
        int numOfContinuous = 0;
        for (MixedVarInfo var : varInfos) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }

            if (var.isContinuous()) {
                numOfContinuous++;
            } else {
                numOfDiscrete++;
            }

            // clear all the values since we are not doing any value encoding
            var.clearValues();
        }

        int numOfVars = varInfos.length;
        int numOfRows = validateData(varInfos, excludedColumns);

        String infoMsg = String.format("There are %d cases and %d variables.", numOfRows, numOfVars);
        ValidationResult result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
        result.setAttribute(ValidationAttribute.ROW_NUMBER, numOfRows);
        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, numOfVars);
        validationResults.add(result);

        infoMsg = String.format("There are %d discrete and %d continuous variables.", numOfDiscrete, numOfContinuous);
        result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
        result.setAttribute(ValidationAttribute.DISCRETE_VAR_COUNT, numOfDiscrete);
        result.setAttribute(ValidationAttribute.CONTINUOUS_VAR_COUNT, numOfContinuous);
        validationResults.add(result);

        int totalMissingValues = markedMissing + assumedMissing;
        if (totalMissingValues > 0) {
            infoMsg = String.format("There are %d missing values, %d marked missing and %d assumed missing.", totalMissingValues, markedMissing, assumedMissing);
            result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
            result.setAttribute(ValidationAttribute.ASSUMED_MISSING_COUNT, assumedMissing);
            result.setAttribute(ValidationAttribute.LABELED_MISSING_COUNT, markedMissing);
            validationResults.add(result);

            infoMsg = String.format("There are %d rows and %d columns with missing values.", numOfRowsWithMissingValues, numOfColsWithMissingValues);
            result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
            result.setAttribute(ValidationAttribute.ROW_WITH_MISSING_VALUE_COUNT, numOfRowsWithMissingValues);
            result.setAttribute(ValidationAttribute.COLUMN_WITH_MISSING_VALUE_COUNT, numOfColsWithMissingValues);
            validationResults.add(result);
        }
    }

    /**
     * Ensure continuous variables contain continuous numbers.
     *
     * @param mixedVarInfos
     * @param excludedColumns
     * @return
     * @throws IOException
     */
    private int validateData(MixedVarInfo[] mixedVarInfos, int[] excludedColumns) throws IOException {
        int numOfVars = mixedVarInfos.length;
        int numOfRows = (hasHeader) ? getNumberOfLines() - 1 : getNumberOfLines();

        double[][] continuousData = new double[numOfVars][];
        int[][] discreteData = new int[numOfVars][];

        int mixedVarInfoIndex = 0;
        for (MixedVarInfo mixedVarInfo : mixedVarInfos) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }

            if (mixedVarInfo.isContinuous()) {
                mixedVarInfo.clearValues();
                continuousData[mixedVarInfoIndex++] = new double[numOfRows];
            } else {
                mixedVarInfo.recategorize();
                discreteData[mixedVarInfoIndex++] = new int[numOfRows];
            }
        }

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
            int lineNum = 1;
            int col = 0;  // array column number
            int numOfData = 0; // number of data read in per column
            int numOfExCols = excludedColumns.length;
            int excludedIndex = 0;
            boolean skipHeader = hasHeader;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean hasQuoteChar = false;
            boolean reqRowMissingCount = true;
            boolean[] hasCountMissingCols = new boolean[numOfVars + numOfExCols + 1];
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            mixedVarInfoIndex = 0;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                // skip header, if any
                if (skipHeader) {
                    while (buffer.hasRemaining() && skipHeader && !Thread.currentThread().isInterrupted()) {
                        byte currChar = buffer.get();
                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                prevNonBlankChar = SPACE_CHAR;
                                skipHeader = false;
                            }

                            lineNum++;
                            if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                                lineNum--;
                            }
                        } else if (!skipLine) {
                            if (currChar > SPACE_CHAR) {
                                prevNonBlankChar = currChar;
                            }

                            if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                                if (currChar == prefix[index]) {
                                    index++;
                                    if (index == prefix.length) {
                                        index = 0;
                                        skipLine = true;
                                        prevNonBlankChar = SPACE_CHAR;
                                    }
                                } else {
                                    index = 0;
                                    skipLine = true;
                                }
                            }
                        }

                        prevChar = currChar;
                    }
                }

                // read in data
                while (buffer.hasRemaining() && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                                numOfData++;

                                // ensure we don't go out of bound
                                if (numOfData > numOfVars) {
                                    String errMsg = String.format(
                                            "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                            lineNum, colNum, numOfVars, numOfData);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, numOfData);
                                    validationResults.add(result);
                                } else if (numOfData < numOfVars) {
                                    String errMsg = String.format(
                                            "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                            lineNum, colNum, numOfVars, numOfData);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, numOfData);
                                    validationResults.add(result);
                                } else {
                                    if (value.length() > 0) {
                                        if (value.equals(missingValueMarker)) {
                                            markedMissing++;
                                            if (reqRowMissingCount) {
                                                numOfRowsWithMissingValues++;
                                            }
                                            if (!hasCountMissingCols[colNum]) {
                                                hasCountMissingCols[colNum] = true;
                                                numOfColsWithMissingValues++;
                                            }
                                        } else {
                                            if (mixedVarInfos[col].isContinuous()) {
                                                try {
                                                    Double.parseDouble(value);
                                                } catch (NumberFormatException exception) {
                                                    String errMsg = String.format("Line %d, column %d: Invalid continuous number %s.", lineNum, colNum, value);
                                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                                    result.setAttribute(ValidationAttribute.VALUE, value);
                                                    validationResults.add(result);
                                                }
                                            }
                                        }
                                    } else {
                                        String errMsg = String.format("Line %d, column %d: Missing value.  No missing marker was found. Assumed value is missing.", lineNum, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.WARNING, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        validationResults.add(result);

                                        assumedMissing++;
                                        if (reqRowMissingCount) {
                                            numOfRowsWithMissingValues++;
                                        }
                                        if (!hasCountMissingCols[colNum]) {
                                            hasCountMissingCols[colNum] = true;
                                            numOfColsWithMissingValues++;
                                        }
                                    }
                                    col++;
                                }
                            }
                        }

                        col = 0;
                        colNum = 0;
                        numOfData = 0;
                        excludedIndex = 0;
                        mixedVarInfoIndex = 0;
                        reqRowMissingCount = true;
                        reqCheck = prefix.length > 0;
                        prevNonBlankChar = SPACE_CHAR;
                        skipLine = false;

                        lineNum++;
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            lineNum--;
                        }
                    } else if (!skipLine) {
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                            if (currChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    prevNonBlankChar = SPACE_CHAR;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                reqCheck = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        } else {
                            boolean isDelimiter;
                            switch (delimiter) {
                                case WHITESPACE:
                                    isDelimiter = (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR);
                                    break;
                                default:
                                    isDelimiter = (currChar == delimChar);
                            }

                            if (isDelimiter) {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    numOfData++;

                                    // ensure we don't go out of bound
                                    if (numOfData > numOfVars) {
                                        String errMsg = String.format(
                                                "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                                lineNum, colNum, numOfVars, numOfData);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, numOfData);
                                        validationResults.add(result);
                                    } else {
                                        if (value.length() > 0) {
                                            if (value.equals(missingValueMarker)) {
                                                markedMissing++;
                                                if (reqRowMissingCount) {
                                                    reqRowMissingCount = false;
                                                    numOfRowsWithMissingValues++;
                                                }
                                                if (!hasCountMissingCols[colNum]) {
                                                    hasCountMissingCols[colNum] = true;
                                                    numOfColsWithMissingValues++;
                                                }
                                            } else {
                                                if (mixedVarInfos[col].isContinuous()) {
                                                    try {
                                                        Double.parseDouble(value);
                                                    } catch (NumberFormatException exception) {
                                                        String errMsg = String.format("Line %d, column %d: Invalid continuous number %s.", lineNum, colNum, value);
                                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                                        result.setAttribute(ValidationAttribute.VALUE, value);
                                                        validationResults.add(result);
                                                    }
                                                }
                                            }
                                        } else {
                                            String errMsg = String.format("Line %d, column %d: Missing value.  No missing marker was found. Assumed value is missing.", lineNum, colNum);
                                            ValidationResult result = new ValidationResult(ValidationCode.WARNING, MessageType.FILE_MISSING_VALUE, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                            validationResults.add(result);

                                            assumedMissing++;
                                            if (reqRowMissingCount) {
                                                reqRowMissingCount = false;
                                                numOfRowsWithMissingValues++;
                                            }
                                            if (!hasCountMissingCols[colNum]) {
                                                hasCountMissingCols[colNum] = true;
                                                numOfColsWithMissingValues++;
                                            }
                                        }
                                        col++;
                                    }
                                }
                            } else {
                                dataBuilder.append((char) currChar);
                            }
                        }
                    }

                    prevChar = currChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize && !Thread.currentThread().isInterrupted());

            // case when no newline char at the end of the file
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    numOfData++;

                    // ensure we don't go out of bound
                    if (numOfData > numOfVars) {
                        String errMsg = String.format(
                                "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                lineNum, colNum, numOfVars, numOfData);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, numOfData);
                        validationResults.add(result);
                    } else if (numOfData < numOfVars) {
                        String errMsg = String.format(
                                "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                lineNum, colNum, numOfVars, numOfData);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, numOfData);
                        validationResults.add(result);
                    } else {
                        if (value.length() > 0) {
                            if (value.equals(missingValueMarker)) {
                                markedMissing++;
                                if (reqRowMissingCount) {
                                    numOfRowsWithMissingValues++;
                                }
                                if (!hasCountMissingCols[colNum]) {
                                    hasCountMissingCols[colNum] = true;
                                    numOfColsWithMissingValues++;
                                }
                            } else {
                                if (mixedVarInfos[col].isContinuous()) {
                                    try {
                                        Double.parseDouble(value);
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Line %d, column %d: Invalid continuous number %s.", lineNum, colNum, value);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        result.setAttribute(ValidationAttribute.VALUE, value);
                                        validationResults.add(result);
                                    }
                                }
                            }
                        } else {
                            String errMsg = String.format("Line %d, column %d: Missing value.  No missing marker was found. Assumed value is missing.", lineNum, colNum);
                            ValidationResult result = new ValidationResult(ValidationCode.WARNING, MessageType.FILE_MISSING_VALUE, errMsg);
                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                            validationResults.add(result);

                            assumedMissing++;
                            if (reqRowMissingCount) {
                                numOfRowsWithMissingValues++;
                            }
                            if (!hasCountMissingCols[colNum]) {
                                hasCountMissingCols[colNum] = true;
                                numOfColsWithMissingValues++;
                            }
                        }
                        col++;
                    }
                }
            }
        }

        return numOfRows;
    }

    private MixedVarInfo[] analysMixedVariableValidation(MixedVarInfo[] mixedVarInfos, int[] excludedColumns) throws IOException {
        int numOfVars = mixedVarInfos.length;
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
            int numOfData = 0; // number of data read in per column
            int numOfExCols = excludedColumns.length;
            int excludedIndex = 0;
            int varInfoIndex = 0;
            boolean skipHeader = hasHeader;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                // skip header, if any
                if (skipHeader) {
                    while (buffer.hasRemaining() && skipHeader && !Thread.currentThread().isInterrupted()) {
                        byte currChar = buffer.get();
                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                prevNonBlankChar = SPACE_CHAR;
                                skipHeader = false;
                            }
                        } else if (!skipLine) {
                            if (currChar > SPACE_CHAR) {
                                prevNonBlankChar = currChar;
                            }

                            if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                                if (currChar == prefix[index]) {
                                    index++;
                                    if (index == prefix.length) {
                                        index = 0;
                                        skipLine = true;
                                        prevNonBlankChar = SPACE_CHAR;
                                    }
                                } else {
                                    index = 0;
                                    skipLine = true;
                                }
                            }
                        }

                        prevChar = currChar;
                    }
                }

                // read in data
                while (buffer.hasRemaining() && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                                numOfData++;

                                // ensure we don't go out of bound
                                if (numOfData == numOfVars) {
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        MixedVarInfo mixedVarInfo = mixedVarInfos[varInfoIndex];
                                        if (!mixedVarInfo.isContinuous()) {
                                            mixedVarInfo.setValue(value);
                                            if (mixedVarInfo.getNumberOfValues() > numberOfDiscreteCategories) {
                                                mixedVarInfo.setContinuous(true);
                                            }
                                        }
                                    }
                                    varInfoIndex++;
                                }
                            }
                        }

                        skipLine = false;
                        colNum = 0;
                        numOfData = 0;
                        excludedIndex = 0;
                        varInfoIndex = 0;
                        reqCheck = prefix.length > 0;
                        prevNonBlankChar = SPACE_CHAR;
                    } else if (!skipLine) {
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                            if (currChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    prevNonBlankChar = SPACE_CHAR;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                reqCheck = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        } else {
                            boolean isDelimiter;
                            switch (delimiter) {
                                case WHITESPACE:
                                    isDelimiter = (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR);
                                    break;
                                default:
                                    isDelimiter = (currChar == delimChar);
                            }

                            if (isDelimiter) {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    numOfData++;

                                    // ensure we don't go out of bound
                                    if (numOfData <= numOfVars) {
                                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                            MixedVarInfo mixedVarInfo = mixedVarInfos[varInfoIndex];
                                            if (!mixedVarInfo.isContinuous()) {
                                                mixedVarInfo.setValue(value);
                                                if (mixedVarInfo.getNumberOfValues() > numberOfDiscreteCategories) {
                                                    mixedVarInfo.setContinuous(true);
                                                }
                                            }
                                        }
                                        varInfoIndex++;
                                    }
                                }
                            } else {
                                dataBuilder.append((char) currChar);
                            }
                        }
                    }

                    prevChar = currChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize && !Thread.currentThread().isInterrupted());

            // case when no newline at end of file
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    numOfData++;

                    // ensure we don't go out of bound
                    if (numOfData == numOfVars) {
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            MixedVarInfo mixedVarInfo = mixedVarInfos[varInfoIndex];
                            if (!mixedVarInfo.isContinuous()) {
                                mixedVarInfo.setValue(value);
                                if (mixedVarInfo.getNumberOfValues() > numberOfDiscreteCategories) {
                                    mixedVarInfo.setContinuous(true);
                                }
                            }
                        }
                        varInfoIndex++;
                    }
                }
            }
        }

        return mixedVarInfos;
    }

    private MixedVarInfo[] validateMixedVariables(int[] excludedColumns) throws IOException {
        int numOfCols = getNumberOfColumns();
        int numOfExCols = excludedColumns.length;
        int numOfVars = numOfCols - numOfExCols;
        MixedVarInfo[] mixedVars = new MixedVarInfo[numOfVars];

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
            int lineNum = 1;
            int excludedIndex = 0;
            int varInfoIndex = 0;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean taskFinished = false;
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !taskFinished && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        skipLine = false;
                        if (prevNonBlankChar > SPACE_CHAR) {
                            taskFinished = true;
                        } else {
                            lineNum++;
                            if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                                lineNum--;
                            }
                        }
                    } else if (!skipLine) {
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                            if (currChar == prefix[index]) {
                                index++;

                                // all the comment chars are matched
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    colNum = 0;
                                    prevNonBlankChar = SPACE_CHAR;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                reqCheck = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        } else {
                            boolean isDelimiter;
                            switch (delimiter) {
                                case WHITESPACE:
                                    isDelimiter = (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR);
                                    break;
                                default:
                                    isDelimiter = (currChar == delimChar);
                            }

                            if (isDelimiter) {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    mixedVars[varInfoIndex++] = new MixedVarInfo(value);
                                    if (value.isEmpty()) {
                                        String errMsg = String.format("Line %d, column %d: Missing variable name.", lineNum, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        validationResults.add(result);
                                    }
                                }
                            } else {
                                dataBuilder.append((char) currChar);
                            }
                        }
                    }

                    prevChar = currChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize && !taskFinished && !Thread.currentThread().isInterrupted());

            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    switch (delimiter) {
                        case WHITESPACE:
                            if (value.length() > 0) {
                                mixedVars[varInfoIndex++] = new MixedVarInfo(value);
                            }
                            break;
                        default:
                            mixedVars[varInfoIndex++] = new MixedVarInfo(value);
                            if (value.isEmpty()) {
                                String errMsg = String.format("Line %d, column %d: Missing variable name.", lineNum, colNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                validationResults.add(result);
                            }
                    }
                }
            }
        }

        return mixedVars;
    }

}
