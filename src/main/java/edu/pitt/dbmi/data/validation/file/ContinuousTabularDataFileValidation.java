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

import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationMessage;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Feb 16, 2017 2:47:07 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ContinuousTabularDataFileValidation extends AbstractTabularDataFileValidation {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousTabularDataFileValidation.class);

    public ContinuousTabularDataFileValidation(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    protected void validateDataFile(int[] excludedColumns) throws IOException {
        int numOfVars = extractVariablesFromFile(excludedColumns);
    }

    private void validateData(int numOfVars, int[] excludedColumns, String comment) throws IOException {
    }

    private void validateData(int numOfVars, int[] excludedColumns) throws IOException {
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            boolean skipHeader = hasHeader;
            boolean hasQuoteChar = false;
            boolean skipLine = false;
            boolean done = false;
            int dataCol = 0;  // data column number
            int lineNumber = 1; // actual row number
            int colNum = 0;  // actual columm number
            int excludedIndex = 0;
            int numOfExCols = excludedColumns.length;
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
                                    dataCol++;

                                    // ensure we don't go out of bound
                                    if (dataCol > numOfVars) {
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.EXCESS_DATA);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        validationResults.add(result);
                                    } else {
                                        if (value.length() > 0) {
                                            try {
                                                Double.parseDouble(value);
                                            } catch (NumberFormatException exception) {
                                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.INVALID_NUMBER);
                                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                                validationResults.add(result);
                                            }
                                        } else {
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.MISSING_VALUE);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                            validationResults.add(result);
                                        }
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
                                dataCol++;

                                // ensure we don't go out of bound
                                if (dataCol > numOfVars) {
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.EXCESS_DATA);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                    validationResults.add(result);
                                } else if (dataCol < numOfVars) {
                                    ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.INSUFFICIENT_DATA);
                                    result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                    result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                    validationResults.add(result);
                                } else {
                                    if (value.length() > 0) {
                                        try {
                                            Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.INVALID_NUMBER);
                                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                            validationResults.add(result);
                                        }
                                    } else {
                                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.MISSING_VALUE);
                                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                        validationResults.add(result);
                                    }
                                }
                            }
                        }

                        colNum = 0;
                        dataCol = 0;
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
                    dataCol++;

                    // ensure we don't go out of bound
                    if (dataCol > numOfVars) {
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.EXCESS_DATA);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        validationResults.add(result);
                    } else if (dataCol < numOfVars) {
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.INSUFFICIENT_DATA);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        validationResults.add(result);
                    } else {
                        if (value.length() > 0) {
                            try {
                                Double.parseDouble(value);
                            } catch (NumberFormatException exception) {
                                ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.INVALID_NUMBER);
                                result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                                result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                                validationResults.add(result);
                            }
                        } else {
                            ValidationResult result = new ValidationResult(ValidationCode.ERROR, ValidationMessage.MISSING_VALUE);
                            result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                            result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                            validationResults.add(result);
                        }
                    }
                }
            }
        }
    }

}
