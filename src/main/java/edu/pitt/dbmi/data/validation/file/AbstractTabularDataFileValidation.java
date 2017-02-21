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
 * Feb 9, 2017 2:44:27 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataFileValidation extends AbstractTabularDataValidation {

    public AbstractTabularDataFileValidation(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    protected int validateVariablesFromFile(int[] excludedColumns) throws IOException {
        if (hasHeader) {
            return (commentMarker == null || commentMarker.trim().isEmpty())
                    ? validateVariables(excludedColumns)
                    : validateVariables(excludedColumns, commentMarker);
        } else {
            return getNumberOfColumns();
        }
    }

    private int validateVariables(int[] excludedColumns, String comment) throws IOException {
        int numOfVars = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = comment.getBytes();
            int index = 0;
            int colNum = 0;
            int excludedIndex = 0;
            int numOfExCols = excludedColumns.length;
            int lineNumber = 1;
            boolean skipLine = false;
            boolean checkRequired = true;  // require check for comment
            boolean hasQuoteChar = false;
            boolean done = false;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !done) {
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
                                    numOfVars++;
                                    if (value.length() == 0) {
                                        String errMsg = String.format("Missing value on line %d at column %d.", lineNumber, colNum);
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
            } while (position < fileSize);

            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    numOfVars++;
                    if (value.length() == 0) {
                        String errMsg = String.format("Missing value on line %d at column %d.", lineNumber, colNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        validationResults.add(result);
                    }
                }
            }
        }

        return numOfVars;
    }

    private int validateVariables(int[] excludedColumns) throws IOException {
        int numOfVars = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            int colNum = 0;
            int excludedIndex = 0;
            int numOfExCols = excludedColumns.length;
            int lineNumber = 1;
            boolean hasQuoteChar = false;
            boolean done = false;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !done) {
                    byte currentChar = buffer.get();

                    if (currentChar >= SPACE || currentChar == delimiter) {
                        // case where line starts with spaces
                        if (currentChar == SPACE && currentChar != delimiter && dataBuilder.length() == 0) {
                            continue;
                        }

                        if (currentChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (currentChar == delimiter) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                excludedIndex++;
                            } else {
                                numOfVars++;
                                if (value.length() == 0) {
                                    String errMsg = String.format("Missing value on line %d at column %d.", lineNumber, colNum);
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
            } while (position < fileSize);

            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    numOfVars++;
                    if (value.length() == 0) {
                        String errMsg = String.format("Missing value on line %d at column %d.", lineNumber, colNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNumber);
                        validationResults.add(result);
                    }
                }
            }
        }

        return numOfVars;
    }

}
