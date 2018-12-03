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
package edu.pitt.dbmi.data.validation;

import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * Dec 3, 2018 3:55:32 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractColumnValidation extends AbstractValidation implements ColumnValidation {

    public AbstractColumnValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    protected abstract void validateFile(int maxNumOfMsg);

    protected int[] toColumnNumbers(Set<String> columnNames) throws IOException {
        if (columnNames.isEmpty()) {
            return new int[0];
        }

        List<Integer> colNums = new LinkedList<>();
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

            int colNum = 0;
            StringBuilder dataBuilder = new StringBuilder();

            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1 && !finished && !Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < len && !finished && !Thread.currentThread().isInterrupted(); i++) {
                    byte currChar = buffer[i];

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        finished = hasSeenNonblankChar && !skip;
                        if (finished) {
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            colNum++;
                            if (columnNames.contains(value)) {
                                colNums.add(colNum);
                            }
                        } else {
                            dataBuilder.delete(0, dataBuilder.length());
                        }

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
                                if (columnNames.contains(value)) {
                                    colNums.add(colNum);
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
                if (columnNames.contains(value)) {
                    colNums.add(colNum);
                }
            }
        }

        return colNums.stream().mapToInt(e -> e).toArray();
    }

    @Override
    public void validate(Set<String> excludedVariables) {
        Set<String> excludedVars = (excludedVariables == null || excludedVariables.isEmpty())
                ? Collections.EMPTY_SET
                : excludedVariables.stream().map(String::trim).collect(Collectors.toSet());

        try {
            validate(toColumnNumbers(excludedVars));
        } catch (IOException exception) {
            if (errors.size() <= maximumNumberOfMessages) {
                String errMsg = String.format("Unable to read file %s.", dataFile.getName());
                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
                result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
                errors.add(result);
            }
        }
    }

    @Override
    public void validate(int[] excludedColumns) {
        int size = (excludedColumns == null) ? 0 : excludedColumns.length;
        int[] excludedCols = new int[size];
        if (size > 0) {
            System.arraycopy(excludedColumns, 0, excludedCols, 0, size);
            Arrays.sort(excludedCols);
        }
    }

    @Override
    public void validate() {
        validate(Collections.EMPTY_SET);
    }

}
