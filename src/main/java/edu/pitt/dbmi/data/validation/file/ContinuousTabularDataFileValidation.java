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
package edu.pitt.dbmi.data.validation.file;

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
 * Feb 16, 2017 2:47:07 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ContinuousTabularDataFileValidation extends AbstractTabularDataFileValidation {

    public ContinuousTabularDataFileValidation(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    protected void validateDataFromFile(int[] excludedColumns) throws IOException {
        int numOfVars = validateVariablesFromFile(excludedColumns);
        int numOfRows = commentMarker.isEmpty()
                ? validateData(numOfVars, excludedColumns)
                : validateData(numOfVars, excludedColumns, commentMarker);
        String infoMsg = String.format("There are %d cases and %d variables.", numOfRows, numOfVars);
        ValidationResult result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
        result.setAttribute(ValidationAttribute.ROW_NUMBER, numOfRows);
        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, numOfVars);
        validationResults.add(result);
    }

    private int validateData(int numOfVars, int[] excludedColumns, String comment) throws IOException {
        int rowCount = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            boolean hasQuoteChar = false;
            boolean skipHeader = hasHeader;
            boolean skipLine = false;
            boolean done = false;
            boolean isHeader = false;
            boolean checkRequired = true;  // require check for comment
            byte[] prefix = comment.getBytes();
            int index = 0;
            int excludedIndex = 0;
            int numOfExCols = excludedColumns.length;
            int lineNumber = 1; // actual line number in file
            int colNum = 0;  // actual file columm number
            int dataColNum = 0;  // actual data column number
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipHeader) {
                    while (buffer.hasRemaining() && !done) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                if (isHeader) {
                                    done = true;
                                }

                                lineNumber++;
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            if (currentChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    skipLine = true;
                                    index = 0;
                                }
                            } else {
                                skipLine = true;
                                index = 0;
                                isHeader = true;
                            }
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            if (index > 0) {
                                done = true;
                            }
                            index = 0;

                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
                    }
                    skipHeader = false;
                }

                while (buffer.hasRemaining()) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currentChar >= SPACE || currentChar == delimiter) {
                        // case where line starts with spaces
                        if (currentChar == SPACE && currentChar != delimiter && dataBuilder.length() == 0) {
                            previousChar = currentChar;
                            continue;
                        }

                        if (checkRequired) {
                            if (currentChar == prefix[index]) {
                                index++;

                                // all the comment chars are matched
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    dataBuilder.delete(0, dataBuilder.length());
                                    colNum = 0;

                                    previousChar = currentChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                checkRequired = false;
                            }
                        }

                        if (currentChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (currentChar == delimiter) {
                            if (hasQuoteChar) {
                                dataBuilder.append((char) currentChar);
                            } else {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    dataColNum++;

                                    // ensure we don't go out of bound
                                    if (dataColNum > numOfVars) {
                                        String errMsg = String.format(
                                                "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                                lineNumber, colNum, numOfVars, dataColNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                                        validationResults.add(result);
                                    }

                                    if (value.length() > 0) {
                                        try {
                                            Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNumber, colNum, value);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                            result.setAttribute(ValidationAttribute.VALUE, value);
                                            validationResults.add(result);
                                        }
                                    } else {
                                        String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        validationResults.add(result);
                                    }
                                }
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                                dataColNum++;

                                // ensure the data is within bound
                                if (dataColNum > numOfVars) {
                                    String errMsg = String.format(
                                            "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                            lineNumber, colNum, numOfVars, dataColNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                                    validationResults.add(result);
                                } else if (dataColNum < numOfVars) {
                                    String errMsg = String.format(
                                            "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                            lineNumber, colNum, numOfVars, dataColNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                                    validationResults.add(result);
                                } else {
                                    if (value.length() > 0) {
                                        try {
                                            Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNumber, colNum, value);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                            result.setAttribute(ValidationAttribute.VALUE, value);
                                            validationResults.add(result);
                                        }
                                    } else {
                                        String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        validationResults.add(result);
                                    }
                                }
                            }

                            rowCount++;
                        }

                        colNum = 0;
                        dataColNum = 0;
                        excludedIndex = 0;
                        checkRequired = true;

                        lineNumber++;
                        if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                            lineNumber--;
                        }
                    }

                    previousChar = currentChar;
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

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    dataColNum++;

                    // ensure the data is within bound
                    if (dataColNum > numOfVars) {
                        String errMsg = String.format(
                                "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                lineNumber, colNum, numOfVars, dataColNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                        validationResults.add(result);
                    } else if (dataColNum < numOfVars) {
                        String errMsg = String.format(
                                "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                lineNumber, colNum, numOfVars, dataColNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                        validationResults.add(result);
                    } else {
                        if (value.length() > 0) {
                            try {
                                Double.parseDouble(value);
                            } catch (NumberFormatException exception) {
                                String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNumber, colNum, value);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                result.setAttribute(ValidationAttribute.VALUE, value);
                                validationResults.add(result);
                            }
                        } else {
                            String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                            validationResults.add(result);
                        }
                    }
                }

                rowCount++;
            }
        }

        return rowCount;
    }

    private int validateData(int numOfVars, int[] excludedColumns) throws IOException {
        int rowCount = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            boolean hasQuoteChar = false;
            boolean skipHeader = hasHeader;
            boolean skipLine = false;
            boolean done = false;
            int excludedIndex = 0;
            int numOfExCols = excludedColumns.length;
            int lineNumber = 1; // actual line number in file
            int colNum = 0;  // actual file columm number
            int dataColNum = 0;  // actual data column number
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipHeader) {
                    while (buffer.hasRemaining() && !done) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                done = true;

                                lineNumber++;
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            skipLine = true;
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
                    }
                    skipHeader = false;
                }

                while (buffer.hasRemaining()) {
                    byte currentChar = buffer.get();

                    if (currentChar >= SPACE || currentChar == delimiter) {
                        // case where line starts with spaces
                        if (currentChar == SPACE && currentChar != delimiter && dataBuilder.length() == 0) {
                            previousChar = currentChar;
                            continue;
                        }

                        if (currentChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (currentChar == delimiter) {
                            if (hasQuoteChar) {
                                dataBuilder.append((char) currentChar);
                            } else {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    dataColNum++;

                                    // ensure we don't go out of bound
                                    if (dataColNum > numOfVars) {
                                        String errMsg = String.format(
                                                "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                                lineNumber, colNum, numOfVars, dataColNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                                        validationResults.add(result);
                                    }

                                    if (value.length() > 0) {
                                        try {
                                            Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNumber, colNum, value);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                            result.setAttribute(ValidationAttribute.VALUE, value);
                                            validationResults.add(result);
                                        }
                                    } else {
                                        String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        validationResults.add(result);
                                    }
                                }
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                                dataColNum++;

                                // ensure the data is within bound
                                if (dataColNum > numOfVars) {
                                    String errMsg = String.format(
                                            "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                            lineNumber, colNum, numOfVars, dataColNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                                    validationResults.add(result);
                                } else if (dataColNum < numOfVars) {
                                    String errMsg = String.format(
                                            "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                            lineNumber, colNum, numOfVars, dataColNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                                    validationResults.add(result);
                                } else {
                                    if (value.length() > 0) {
                                        try {
                                            Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNumber, colNum, value);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                            result.setAttribute(ValidationAttribute.VALUE, value);
                                            validationResults.add(result);
                                        }
                                    } else {
                                        String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        validationResults.add(result);
                                    }
                                }
                            }

                            rowCount++;
                        }

                        colNum = 0;
                        dataColNum = 0;
                        excludedIndex = 0;

                        lineNumber++;
                        if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                            lineNumber--;
                        }
                    }

                    previousChar = currentChar;
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

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    dataColNum++;

                    // ensure the data is within bound
                    if (dataColNum > numOfVars) {
                        String errMsg = String.format(
                                "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                lineNumber, colNum, numOfVars, dataColNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                        validationResults.add(result);
                    } else if (dataColNum < numOfVars) {
                        String errMsg = String.format(
                                "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                lineNumber, colNum, numOfVars, dataColNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, dataColNum);
                        validationResults.add(result);
                    } else {
                        if (value.length() > 0) {
                            try {
                                Double.parseDouble(value);
                            } catch (NumberFormatException exception) {
                                String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNumber, colNum, value);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                result.setAttribute(ValidationAttribute.VALUE, value);
                                validationResults.add(result);
                            }
                        } else {
                            String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                            validationResults.add(result);
                        }
                    }
                }

                rowCount++;
            }
        }

        return rowCount;
    }

}
