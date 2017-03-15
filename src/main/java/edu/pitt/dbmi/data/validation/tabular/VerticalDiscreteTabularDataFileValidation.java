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
import edu.pitt.dbmi.data.validation.MessageType;
import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Set;

/**
 *
 * Feb 17, 2017 5:00:27 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDataFileValidation extends AbstractTabularDataFileValidation implements TabularDataValidation {

    public VerticalDiscreteTabularDataFileValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    public void validate(Set<String> excludedVariables) {
        try {
            validateDataFromFile(getColumnNumbers(excludedVariables));
        } catch (IOException exception) {
            String errMsg = String.format("Unable to read file %s.", dataFile.getName());
            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
            result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
            validationResults.add(result);
        }
    }

    @Override
    public void validate(int[] excludedColumns) {
        try {
            validateDataFromFile(filterValidColumnNumbers(excludedColumns));
        } catch (IOException exception) {
            String errMsg = String.format("Unable to read file %s.", dataFile.getName());
            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
            result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
            validationResults.add(result);
        }
    }

    @Override
    public void validate() {
        validate(Collections.EMPTY_SET);
    }

    private void validateDataFromFile(int[] excludedColumns) throws IOException {
        int numOfVars = hasHeader ? validateVariables(excludedColumns) : getNumberOfColumns() - excludedColumns.length;
        int numOfRows = validateData(numOfVars, excludedColumns);

        String infoMsg = String.format("There are %d cases and %d variables.", numOfRows, numOfVars);
        ValidationResult result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
        result.setAttribute(ValidationAttribute.ROW_NUMBER, numOfRows);
        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, numOfVars);
        validationResults.add(result);
    }

    private int validateData(int numOfVars, int[] excludedColumns) throws IOException {
        int count = 0;

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
            int numOfData = 0; // number of data read in per column
            int numOfExCols = excludedColumns.length;
            int excludedIndex = 0;
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
                    while (buffer.hasRemaining() && skipHeader) {
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

                while (buffer.hasRemaining()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
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
                                    if (value.length() == 0) {
                                        String errMsg = String.format("Line %d, column %d: Missing value.  No missing marker was found. Assumed value is missing.", lineNum, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.WARNING, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        validationResults.add(result);
                                    }
                                }
                            }
                        }

                        colNum = 0;
                        numOfData = 0;
                        excludedIndex = 0;
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
                                        if (value.length() == 0) {
                                            String errMsg = String.format("Line %d, column %d: Missing value.  No missing marker was found. Assumed value is missing.", lineNum, colNum);
                                            ValidationResult result = new ValidationResult(ValidationCode.WARNING, MessageType.FILE_MISSING_VALUE, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                            validationResults.add(result);
                                        }
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
            } while (position < fileSize);

            // case when no newline char at the end of the file
            if (colNum > 0 || dataBuilder.length() > 0) {
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
                        if (value.length() == 0) {
                            String errMsg = String.format("Line %d, column %d: Missing value.  No missing marker was found. Assumed value is missing.", lineNum, colNum);
                            ValidationResult result = new ValidationResult(ValidationCode.WARNING, MessageType.FILE_MISSING_VALUE, errMsg);
                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                            validationResults.add(result);
                        }
                    }
                }
            }
        }

        return count;
    }

}
