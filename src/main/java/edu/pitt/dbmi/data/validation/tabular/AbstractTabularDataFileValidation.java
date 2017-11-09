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
import edu.pitt.dbmi.data.reader.tabular.AbstractBasicTabularDataFileReader;
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
 * Feb 9, 2017 2:44:27 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataFileValidation extends AbstractBasicTabularDataFileReader {

    protected final List<ValidationResult> validationResults;

    public AbstractTabularDataFileValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
        this.validationResults = new LinkedList<>();
    }

    protected int validateVariables(int[] excludedColumns) throws IOException {
        int numOfVars = 0;

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
            int numOfExCols = excludedColumns.length;
            int excludedIndex = 0;
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
                                    numOfVars++;
                                    if (value.length() == 0) {
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
                    numOfVars++;
                    if (value.length() == 0) {
                        String errMsg = String.format("Line %d, column %d: Missing variable name.", lineNum, colNum);
                        ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_MISSING_VALUE, errMsg);
                        result.setAttribute(ValidationAttribute.COLUMN_NUMBER, colNum);
                        result.setAttribute(ValidationAttribute.LINE_NUMBER, lineNum);
                        validationResults.add(result);
                    }
                }
            }
        }

        return numOfVars;
    }

    public List<ValidationResult> getValidationResults() {
        return validationResults;
    }

}
