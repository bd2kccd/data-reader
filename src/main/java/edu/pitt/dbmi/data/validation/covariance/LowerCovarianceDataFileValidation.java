/*
 * Copyright (C) 2018 University of Pittsburgh.
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

import edu.pitt.dbmi.data.reader.Delimiter;
import edu.pitt.dbmi.data.validation.AbstractValidation;
import edu.pitt.dbmi.data.validation.MessageType;
import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 *
 * Nov 20, 2018 1:34:55 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class LowerCovarianceDataFileValidation extends AbstractValidation {

    public LowerCovarianceDataFileValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    public void validate() {
        try {
            int numOfCases = validateNumberOfCases(maximumNumberOfMessages);
            int numOfVars = validateNumberOfVariables(maximumNumberOfMessages);
            validateData(numOfVars, maximumNumberOfMessages);

            if (infos.size() <= maximumNumberOfMessages) {
                String infoMsg = String.format("There are %d cases and %d variables.", numOfCases, numOfVars);
                ValidationResult result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
                result.setAttribute(ValidationAttribute.ROW_NUMBER, numOfCases);
                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, numOfVars);
                infos.add(result);
            }
        } catch (IOException exception) {
            if (errors.size() <= maximumNumberOfMessages) {
                String errMsg = String.format("Unable to read file %s.", dataFile.getName());
                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
                result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
                errors.add(result);
            }
        }
    }

    private void validateData(int numOfVars, int maxNumOfMsg) throws IOException {
        try (InputStream in = Files.newInputStream(dataFile.toPath(), StandardOpenOption.READ)) {
            boolean skip = false;
            boolean hasSeenNonblankChar = false;
            boolean hasQuoteChar = false;

            byte delimChar = delimiter.getByteValue();

            // comment marker check
            byte[] comment = commentMarker.getBytes();
            int cmntIndex = 0;
            boolean checkForComment = comment.length > 0;

            int lineDataNum = 1;
            int lineNum = 1;
            int colNum = 0;
            int rowNum = 1;

            StringBuilder dataBuilder = new StringBuilder();
            byte prevChar = -1;
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1 && !Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < len && !Thread.currentThread().isInterrupted(); i++) {
                    byte currChar = buffer[i];

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            prevChar = currChar;
                            continue;
                        }

                        if (hasSeenNonblankChar && !skip) {
                            if (lineDataNum >= 3) {
                                colNum++;
                                String value = dataBuilder.toString().trim();

                                if (colNum > rowNum) {
                                    if (errors.size() <= maxNumOfMsg) {
                                        String errMsg = String.format(
                                                "Line %d: Excess data.  Expect %d value(s) but encounter %d.",
                                                lineNum, rowNum, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                                        errors.add(result);
                                    }
                                } else if (colNum < rowNum) {
                                    if (errors.size() <= maxNumOfMsg) {
                                        String errMsg = String.format(
                                                "Line %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                                lineNum, rowNum, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DATA, errMsg);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                                        result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                                        errors.add(result);
                                    }
                                } else {
                                    if (value.isEmpty()) {
                                        if (errors.size() <= maxNumOfMsg) {
                                            String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                            errors.add(result);
                                        }
                                    } else {
                                        try {
                                            Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            if (errors.size() <= maxNumOfMsg) {
                                                String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNum, colNum, value);
                                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                                result.setAttribute(ValidationAttribute.VALUE, value);
                                                errors.add(result);
                                            }
                                        }
                                    }
                                }

                                rowNum++;
                            }

                            lineDataNum++;
                        }

                        lineNum++;

                        // clear data
                        dataBuilder.delete(0, dataBuilder.length());

                        // reset states
                        skip = false;
                        hasSeenNonblankChar = false;
                        cmntIndex = 0;
                        colNum = 0;
                        checkForComment = comment.length > 0;
                    } else if (!skip) {
                        if (currChar > SPACE_CHAR) {
                            hasSeenNonblankChar = true;
                        }

                        // skip blank chars at the begining of the line
                        if (currChar <= SPACE_CHAR && !hasSeenNonblankChar) {
                            continue;
                        }

                        // check for comment marker to skip line
                        if (checkForComment) {
                            if (currChar == comment[cmntIndex]) {
                                cmntIndex++;
                                if (cmntIndex == comment.length) {
                                    skip = true;
                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                checkForComment = false;
                            }
                        }

                        if (lineDataNum >= 3) {
                            if (currChar == quoteCharacter) {
                                hasQuoteChar = !hasQuoteChar;
                            } else if (!hasQuoteChar) {
                                boolean isDelimiter;
                                switch (delimiter) {
                                    case WHITESPACE:
                                        isDelimiter = (currChar <= SPACE_CHAR) && (prevChar > SPACE_CHAR);
                                        break;
                                    default:
                                        isDelimiter = (currChar == delimChar);
                                }

                                if (isDelimiter) {
                                    colNum++;
                                    String value = dataBuilder.toString().trim();
                                    dataBuilder.delete(0, dataBuilder.length());

                                    if (colNum > rowNum) {
                                        if (errors.size() <= maxNumOfMsg) {
                                            String errMsg = String.format(
                                                    "Line %d: Excess data.  Expect %d value(s) but encounter %d.",
                                                    lineNum, rowNum, colNum);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                            result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                                            result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                                            errors.add(result);
                                        }
                                    } else {
                                        if (value.isEmpty()) {
                                            if (errors.size() <= maxNumOfMsg) {
                                                String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                                errors.add(result);
                                            }
                                        } else {
                                            try {
                                                Double.parseDouble(value);
                                            } catch (NumberFormatException exception) {
                                                if (errors.size() <= maxNumOfMsg) {
                                                    String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNum, colNum, value);
                                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                                    result.setAttribute(ValidationAttribute.VALUE, value);
                                                    errors.add(result);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    dataBuilder.append((char) currChar);
                                }
                            }
                        }
                    }

                    prevChar = currChar;
                }
            }

            if (hasSeenNonblankChar && !skip) {
                if (lineDataNum >= 3) {
                    colNum++;
                    String value = dataBuilder.toString().trim();

                    if (colNum > rowNum) {
                        if (errors.size() <= maxNumOfMsg) {
                            String errMsg = String.format(
                                    "Line %d: Excess data.  Expect %d value(s) but encounter %d.",
                                    lineNum, rowNum, colNum);
                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                            result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                            result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                            errors.add(result);
                        }
                    } else if (colNum < rowNum) {
                        if (errors.size() <= maxNumOfMsg) {
                            String errMsg = String.format(
                                    "Line %d: Insufficient data.  Expect %d value(s) but encounter %d.",
                                    lineNum, rowNum, colNum);
                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INSUFFICIENT_DATA, errMsg);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                            result.setAttribute(ValidationAttribute.EXPECTED_COUNT, rowNum);
                            result.setAttribute(ValidationAttribute.ACTUAL_COUNT, colNum);
                            errors.add(result);
                        }
                    } else {
                        if (value.isEmpty()) {
                            if (errors.size() <= maxNumOfMsg) {
                                String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                errors.add(result);
                            }
                        } else {
                            try {
                                Double.parseDouble(value);
                            } catch (NumberFormatException exception) {
                                if (errors.size() <= maxNumOfMsg) {
                                    String errMsg = String.format("Line %d, column %d: Invalid number %s.", lineNum, colNum, value);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                    result.setAttribute(ValidationAttribute.VALUE, value);
                                    errors.add(result);
                                }
                            }
                        }
                    }

                    rowNum++;
                }
            }

            rowNum--;  // minus the extra count for possibly the next line
            if (rowNum > numOfVars) {
                if (errors.size() <= maxNumOfMsg) {
                    String errMsg = String.format(
                            "Excess data.  Expect %d row(s) but encounter %d.",
                            numOfVars, rowNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                    errors.add(result);
                }
            } else if (rowNum < numOfVars) {
                if (errors.size() <= maxNumOfMsg) {
                    String errMsg = String.format(
                            "Insufficient data.  Expect %d row(s) but encounter %d.",
                            numOfVars, rowNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_EXCESS_DATA, errMsg);
                    result.setAttribute(ValidationAttribute.EXPECTED_COUNT, numOfVars);
                    result.setAttribute(ValidationAttribute.ACTUAL_COUNT, rowNum);
                    errors.add(result);
                }
            }
        }
    }

    private int validateNumberOfVariables(int maxNumOfMsg) throws IOException {
        int numOfVars = 0;

        try (InputStream in = Files.newInputStream(dataFile.toPath(), StandardOpenOption.READ)) {
            boolean skip = false;
            boolean hasSeenNonblankChar = false;
            boolean hasQuoteChar = false;
            boolean finished = false;

            byte delimChar = delimiter.getByteValue();

            // comment marker check
            byte[] comment = commentMarker.getBytes();
            int cmntIndex = 0;
            boolean checkForComment = comment.length > 0;

            int lineDataNum = 1;
            int colNum = 0;
            int lineNum = 1;

            StringBuilder dataBuilder = new StringBuilder();
            byte prevChar = -1;
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1 && !finished && !Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < len && !finished && !Thread.currentThread().isInterrupted(); i++) {
                    byte currChar = buffer[i];

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            prevChar = currChar;
                            continue;
                        }

                        if (hasSeenNonblankChar && !skip) {
                            if (lineDataNum == 2) {
                                String value = dataBuilder.toString().trim();

                                colNum++;
                                if (value.isEmpty()) {
                                    if (errors.size() <= maxNumOfMsg) {
                                        String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        errors.add(result);
                                    }
                                }

                                numOfVars++;

                                finished = true;
                            }

                            lineDataNum++;
                        }

                        lineNum++;

                        // clear data
                        dataBuilder.delete(0, dataBuilder.length());

                        // reset states
                        skip = false;
                        hasSeenNonblankChar = false;
                        cmntIndex = 0;
                        checkForComment = comment.length > 0;
                    } else if (!skip) {
                        if (currChar > SPACE_CHAR) {
                            hasSeenNonblankChar = true;
                        }

                        // skip blank chars at the begining of the line
                        if (currChar <= SPACE_CHAR && !hasSeenNonblankChar) {
                            continue;
                        }

                        // check for comment marker to skip line
                        if (checkForComment) {
                            if (currChar == comment[cmntIndex]) {
                                cmntIndex++;
                                if (cmntIndex == comment.length) {
                                    skip = true;
                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                checkForComment = false;
                            }
                        }

                        if (lineDataNum == 2) {
                            if (currChar == quoteCharacter) {
                                hasQuoteChar = !hasQuoteChar;
                            } else if (!hasQuoteChar) {
                                boolean isDelimiter;
                                switch (delimiter) {
                                    case WHITESPACE:
                                        isDelimiter = (currChar <= SPACE_CHAR) && (prevChar > SPACE_CHAR);
                                        break;
                                    default:
                                        isDelimiter = (currChar == delimChar);
                                }

                                if (isDelimiter) {
                                    String value = dataBuilder.toString().trim();
                                    dataBuilder.delete(0, dataBuilder.length());

                                    colNum++;
                                    if (value.isEmpty()) {
                                        if (errors.size() <= maxNumOfMsg) {
                                            String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                            errors.add(result);
                                        }
                                    }

                                    numOfVars++;
                                } else {
                                    dataBuilder.append((char) currChar);
                                }
                            }
                        }
                    }

                    prevChar = currChar;
                }
            }

            if (hasSeenNonblankChar && !skip) {
                if (lineDataNum == 2) {
                    String value = dataBuilder.toString().trim();
                    dataBuilder.delete(0, dataBuilder.length());

                    colNum++;
                    if (value.isEmpty()) {
                        if (errors.size() <= maxNumOfMsg) {
                            String errMsg = String.format("Line %d, column %d: Missing value.", lineNum, colNum);
                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                            errors.add(result);
                        }
                    }

                    numOfVars++;
                }
            }
        }

        if (numOfVars == 0) {
            if (errors.size() <= maxNumOfMsg) {
                String errMsg = "Covariance file does not contain variable names.";
                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                errors.add(result);
            }
        }

        return numOfVars;
    }

    private int validateNumberOfCases(int maxNumOfMsg) throws IOException {
        int numOfCases = 0;

        try (InputStream in = Files.newInputStream(dataFile.toPath(), StandardOpenOption.READ)) {
            boolean skip = false;
            boolean hasSeenNonblankChar = false;
            boolean hasQuoteChar = false;
            boolean finished = false;

            // comment marker check
            byte[] comment = commentMarker.getBytes();
            int cmntIndex = 0;
            boolean checkForComment = comment.length > 0;

            int lineNum = 1;

            StringBuilder dataBuilder = new StringBuilder();
            byte prevChar = -1;
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1 && !finished && !Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < len && !finished && !Thread.currentThread().isInterrupted(); i++) {
                    byte currChar = buffer[i];

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            prevChar = currChar;
                            continue;
                        }

                        finished = hasSeenNonblankChar && !skip;
                        if (!finished) {
                            lineNum++;

                            // clear data
                            dataBuilder.delete(0, dataBuilder.length());

                            // reset states
                            skip = false;
                            hasSeenNonblankChar = false;
                            cmntIndex = 0;
                            checkForComment = comment.length > 0;
                        }
                    } else if (!skip) {
                        if (currChar > SPACE_CHAR) {
                            hasSeenNonblankChar = true;
                        }

                        // skip blank chars at the begining of the line
                        if (currChar <= SPACE_CHAR && !hasSeenNonblankChar) {
                            continue;
                        }

                        // check for comment marker to skip line
                        if (checkForComment) {
                            if (currChar == comment[cmntIndex]) {
                                cmntIndex++;
                                if (cmntIndex == comment.length) {
                                    skip = true;
                                    prevChar = currChar;

                                    continue;
                                }
                            } else {
                                checkForComment = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (!hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        }
                    }

                    prevChar = currChar;
                }
            }

            String value = dataBuilder.toString().trim();
            if (value.isEmpty()) {
                if (errors.size() <= maxNumOfMsg) {
                    String errMsg = String.format("Line %d: Missing number of cases.", lineNum);
                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                    errors.add(result);
                }
            } else {
                try {
                    numOfCases += Integer.parseInt(value);
                } catch (NumberFormatException exception) {
                    if (errors.size() <= maxNumOfMsg) {
                        String errMsg = String.format("Line %d: Invalid number %s.", lineNum, value);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_INVALID_NUMBER, errMsg);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                        result.setAttribute(ValidationAttribute.VALUE, value);
                        errors.add(result);
                    }
                }
            }
        }

        return numOfCases;
    }

}
