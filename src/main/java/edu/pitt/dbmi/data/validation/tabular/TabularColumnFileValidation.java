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
package edu.pitt.dbmi.data.validation.tabular;

import edu.pitt.dbmi.data.reader.Delimiter;
import edu.pitt.dbmi.data.validation.AbstractColumnValidation;
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
 * Nov 28, 2018 11:02:32 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularColumnFileValidation extends AbstractColumnValidation {

    public TabularColumnFileValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    protected void validateFile(int[] excludedColumns) throws IOException {
        int numOfVars = 0;

        try (InputStream in = Files.newInputStream(dataFile.toPath(), StandardOpenOption.READ)) {
            boolean skip = false;
            boolean hasSeenNonblankChar = false;
            boolean hasQuoteChar = false;
            boolean finished = false;

            byte delimChar = delimiter.getByteValue();
            byte prevChar = -1;

            // comment marker check
            byte[] comment = commentMarker.getBytes();
            int cmntIndex = 0;
            boolean checkForComment = comment.length > 0;

            // excluded columns check
            int numOfExCols = excludedColumns.length;
            int exColsIndex = 0;

            int colNum = 0;
            int lineNum = 1;
            StringBuilder dataBuilder = new StringBuilder();

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
                        if (finished) {
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            colNum++;
                            if (numOfExCols == 0 || exColsIndex >= numOfExCols || colNum != excludedColumns[exColsIndex]) {
                                numOfVars++;
                                if (value.isEmpty()) {
                                    String errMsg = String.format("Line %d, column %d: Missing variable name.", lineNum, colNum);
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                    errors.add(result);
                                }
                            }
                        } else {
                            dataBuilder.delete(0, dataBuilder.length());
                        }

                        lineNum++;

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

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        } else {
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
                                if (numOfExCols > 0 && (exColsIndex < numOfExCols && colNum == excludedColumns[exColsIndex])) {
                                    exColsIndex++;
                                } else {
                                    numOfVars++;
                                    if (value.isEmpty()) {
                                        String errMsg = String.format("Line %d, column %d: Missing variable name.", lineNum, colNum);
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                                        errors.add(result);
                                    }
                                }

                            } else {
                                dataBuilder.append((char) currChar);
                            }
                        }
                    }

                    prevChar = currChar;
                }
            }

            finished = hasSeenNonblankChar && !skip;
            if (finished) {
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                colNum++;
                if (numOfExCols == 0 || exColsIndex >= numOfExCols || colNum != excludedColumns[exColsIndex]) {
                    numOfVars++;
                    if (value.isEmpty()) {
                        String errMsg = String.format("Line %d, column %d: Missing variable name.", lineNum, colNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                        errors.add(result);
                    }
                }
            }
        }

        if (numOfVars <= 0) {
            String errMsg = "No variable was read in.";
            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
            errors.add(result);
        }

        String infoMsg = String.format("There are %d variables.", numOfVars);
        ValidationResult result = new ValidationResult(ValidationCode.INFO, MessageType.FILE_SUMMARY, infoMsg);
        infos.add(result);
    }

}
