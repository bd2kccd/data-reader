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

import edu.pitt.dbmi.data.reader.AbstractDataReader;
import edu.pitt.dbmi.data.validation.MessageType;
import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * Feb 23, 2017 3:36:02 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class CovarianceDataFileValidation extends AbstractDataReader implements DataFileValidation {

    private final List<ValidationResult> validationResults;

    public CovarianceDataFileValidation(File dataFile, char delimiter) {
        super(dataFile, delimiter);
        this.validationResults = new LinkedList<>();
    }

    @Override
    public void validate() {
        try {
            int numberOfCases = validateNumberOfCases();
            int numberOfVariables = validateVariables();
            validateCovarianceData(numberOfVariables);

            String infoMsg = String.format("There are %d cases and %d variables.", numberOfCases, numberOfVariables);
            ValidationResult result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
            result.setAttribute(ValidationAttribute.ROW_NUMBER, numberOfCases);
            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, numberOfVariables);
            validationResults.add(result);
        } catch (IOException exception) {
            String errMsg = String.format("Unable to read file %s.", dataFile.getName());
            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
            result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
            validationResults.add(result);
        }
    }

    public void validateCovarianceData(int numberOfVariables) throws IOException {
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNumber = 1;
            int colNum = 0;
            int rowNum = 1;
            int numOfLineData = 0;
            boolean taskDone = false;
            boolean skipLine = false;
            boolean skipToData = true;
            boolean checkRequired = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipToData) {
                    while (buffer.hasRemaining() && !taskDone) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                taskDone = (numOfLineData == 2);

                                lineNumber++;
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            if (prefix.length > 0 && currentChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    skipLine = true;
                                    index = 0;
                                }
                            } else {
                                skipLine = true;
                                index = 0;
                                numOfLineData++;
                            }
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            if (index > 0) {
                                numOfLineData++;
                                taskDone = (numOfLineData == 2);
                                index = 0;
                            }

                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
                    }
                    skipToData = false;
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
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (rowNum > numberOfVariables) {
                                String errMsg = String.format(
                                        "Line %d: Excess data.  Expect %d case(s) but encounter %d.",
                                        lineNumber, numberOfVariables, rowNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                                result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                                validationResults.add(result);
                            }
                            if (colNum < rowNum) {
                                String errMsg = String.format(
                                        "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                        lineNumber, colNum, rowNum, colNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                                result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                                validationResults.add(result);
                            } else if (colNum > rowNum) {
                                String errMsg = String.format(
                                        "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                        lineNumber, colNum, rowNum, colNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                                result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
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

                            rowNum++;
                        }

                        colNum = 0;
                        checkRequired = prefix.length > 0;

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

            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (rowNum > numberOfVariables) {
                    String errMsg = String.format(
                            "Line %d: Excess data.  Expect %d case(s) but encounter %d.",
                            lineNumber, numberOfVariables, rowNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                    validationResults.add(result);
                }
                if (colNum < rowNum) {
                    String errMsg = String.format(
                            "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                            lineNumber, colNum, rowNum, colNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                    validationResults.add(result);
                } else if (colNum > rowNum) {
                    String errMsg = String.format(
                            "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                            lineNumber, colNum, rowNum, colNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
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
    }

    public int validateVariables() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNumber = 1;
            int colNum = 0;
            boolean firstTaskDone = false;
            boolean secondTaskDone = false;
            boolean skipLine = false;
            boolean skipToData = true;
            boolean isLineData = false;
            boolean checkRequired = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipToData) {
                    while (buffer.hasRemaining() && !firstTaskDone) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                if (isLineData) {
                                    firstTaskDone = true;
                                }

                                lineNumber++;
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            if (prefix.length > 0 && currentChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    skipLine = true;
                                    index = 0;
                                }
                            } else {
                                skipLine = true;
                                index = 0;
                                isLineData = true;
                            }
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            if (index > 0) {
                                firstTaskDone = true;
                            }
                            index = 0;

                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
                    }
                    skipToData = false;
                }

                while (buffer.hasRemaining() && !secondTaskDone) {
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

                                if (value.length() == 0) {
                                    String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                    validationResults.add(result);
                                }

                                count++;
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        secondTaskDone = colNum > 0 || dataBuilder.length() > 0;

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
            } while (position < fileSize && !(firstTaskDone && secondTaskDone));

            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (value.length() == 0) {
                    String errMsg = String.format("Line %d, column %d: Missing value.", lineNumber, colNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                    validationResults.add(result);
                }

                count++;
            }
        }

        return count;
    }

    public int validateNumberOfCases() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNumber = 1;
            boolean done = false;
            boolean skipLine = false;
            boolean checkRequired = prefix.length > 0;  // require check for comment
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !done) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currentChar > SPACE) {
                        if (checkRequired) {
                            if (currentChar == prefix[index]) {
                                index++;

                                // all the comment chars are matched
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    previousChar = currentChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                checkRequired = false;
                            }
                        }

                        dataBuilder.append((char) currentChar);
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        if (dataBuilder.length() > 0) {
                            done = true;
                        } else {
                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }
                    }

                    previousChar = currentChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while ((position < fileSize) && !done);

            if (dataBuilder.length() > 0) {
                String value = dataBuilder.toString().trim();
                try {
                    count = Integer.parseInt(value);
                } catch (NumberFormatException exception) {
                    String errMsg = String.format("Line %d: Invalid number %s.", lineNumber, value);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                    result.setAttribute(ValidationAttribute.VALUE, value);
                    validationResults.add(result);
                }
            }
        }

        return count;
    }

    @Override
    public List<ValidationResult> getValidationResults() {
        return validationResults;
    }

}
