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
package edu.pitt.dbmi.data.validation.covariance;

import edu.pitt.dbmi.data.Delimiter;
import edu.pitt.dbmi.data.reader.AbstractDataFileReader;
import edu.pitt.dbmi.data.validation.MessageType;
import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationResult;
import edu.pitt.dbmi.data.validation.tabular.DataFileValidation;
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
public class CovarianceDataFileValidation extends AbstractDataFileReader implements DataFileValidation {

    private final List<ValidationResult> validationResults;

    public CovarianceDataFileValidation(File dataFile, Delimiter delimiter) {
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

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNum = 1;
            int colNum = 0;
            int rowNum = 1;
            boolean skipLine = false;
            boolean skipToData = true;
            int skipLineCount = 0;
            boolean reqCheck = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipToData) {
                    while (buffer.hasRemaining() && skipToData) {
                        byte currChar = buffer.get();

                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                skipLineCount++;
                                skipToData = !(skipLineCount == 2);
                                prevNonBlankChar = SPACE_CHAR;
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

                            if (rowNum > numberOfVariables) {
                                String errMsg = String.format(
                                        "Line %d: Excess data.  Expect %d row(s) but encounter %d.",
                                        lineNum, numberOfVariables, rowNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                                result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                                validationResults.add(result);
                            }
                            if (colNum < rowNum) {
                                String errMsg = String.format(
                                        "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                        lineNum, colNum, rowNum, colNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                                result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                                validationResults.add(result);
                            } else if (colNum > rowNum) {
                                String errMsg = String.format(
                                        "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                                        lineNum, colNum, rowNum, colNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                                result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                                validationResults.add(result);
                            } else {
                                if (value.length() > 0) {
                                    try {
                                        Double.parseDouble(value);
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNum, colNum, value);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        result.setAttribute(ValidationAttribute.VALUE, value);
                                        validationResults.add(result);
                                    }
                                } else {
                                    String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                    validationResults.add(result);
                                }
                            }

                            rowNum++;
                        }

                        skipLine = false;
                        colNum = 0;
                        reqCheck = prefix.length > 0;

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

                                if (colNum > rowNum) {
                                    String errMsg = String.format(
                                            "Line %d: Excess data.  Expect %d row(s) but encounter %d.",
                                            lineNum, numberOfVariables, rowNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                                    validationResults.add(result);
                                } else {
                                    if (value.length() > 0) {
                                        try {
                                            Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNum, colNum, value);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                            result.setAttribute(ValidationAttribute.VALUE, value);
                                            validationResults.add(result);
                                        }
                                    } else {
                                        String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
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
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);

            // case where no newline at end of file
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (rowNum > numberOfVariables) {
                    String errMsg = String.format(
                            "Line %d: Excess data.  Expect %d case(s) but encounter %d.",
                            lineNum, numberOfVariables, rowNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                    validationResults.add(result);
                }
                if (colNum < rowNum) {
                    String errMsg = String.format(
                            "Line %d, column %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                            lineNum, colNum, rowNum, colNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DAT, errMsg);
                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                    validationResults.add(result);
                } else if (colNum > rowNum) {
                    String errMsg = String.format(
                            "Line %d, column %d: Excess data.  Expect %d value(s) but encounter %d.",
                            lineNum, colNum, rowNum, colNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numberOfVariables);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                    validationResults.add(result);
                } else {
                    if (value.length() > 0) {
                        try {
                            Double.parseDouble(value);
                        } catch (NumberFormatException exception) {
                            String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNum, colNum, value);
                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                            result.setAttribute(ValidationAttribute.VALUE, value);
                            validationResults.add(result);
                        }
                    } else {
                        String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                        validationResults.add(result);
                    }
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

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNum = 1;
            int colNum = 0;
            boolean skipLine = false;
            boolean skipCaseNum = true;
            boolean doneExtractVars = false;
            boolean reqCheck = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipCaseNum) {
                    while (buffer.hasRemaining() && skipCaseNum) {
                        byte currChar = buffer.get();

                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                skipCaseNum = false;
                                prevNonBlankChar = SPACE_CHAR;
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

                while (buffer.hasRemaining() && !doneExtractVars) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        skipLine = false;
                        if (prevNonBlankChar > SPACE_CHAR) {
                            doneExtractVars = true;
                            prevNonBlankChar = SPACE_CHAR;
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
                                count++;
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (value.length() == 0) {
                                    String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                    validationResults.add(result);
                                }
                            } else {
                                dataBuilder.append((char) currChar);
                            }
                        }
                    }
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize && !doneExtractVars);

            // data at the end of line
            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (value.length() == 0) {
                    String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
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
            int lineNum = 1;
            boolean finished = false;
            boolean skipLine = false;
            boolean reqCheck = prefix.length > 0;  // require check for comment
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !finished) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        skipLine = false;

                        if (prevNonBlankChar > SPACE_CHAR) {
                            finished = true;
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

                        dataBuilder.append((char) currChar);
                    }

                    prevChar = currChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while ((position < fileSize) && !finished);

            if (dataBuilder.length() > 0) {
                String value = dataBuilder.toString().trim();
                try {
                    count += Integer.parseInt(value);
                } catch (NumberFormatException exception) {
                    String errMsg = String.format("Line %d: Invalid number %s.", lineNum, value);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                    result.setAttribute(ValidationAttribute.VALUE, value);
                    validationResults.add(result);
                }
            } else {
                String errMsg = String.format("Line %d: Missing value.", lineNum);
                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                validationResults.add(result);
            }
        }

        return count;
    }

    @Override
    public List<ValidationResult> getValidationResults() {
        return validationResults;
    }

}
