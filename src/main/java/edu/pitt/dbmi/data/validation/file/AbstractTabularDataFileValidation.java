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

import edu.pitt.dbmi.data.reader.tabular.AbstractTabularDataReader;
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
public abstract class AbstractTabularDataFileValidation extends AbstractTabularDataReader implements TabularDataFileValidation {

    protected final List<String> errors;
    protected final List<String> infos;
    protected final List<String> warnings;

    public AbstractTabularDataFileValidation(File dataFile, char delimiter) {
        super(dataFile, delimiter);

        this.errors = new LinkedList<>();
        this.infos = new LinkedList<>();
        this.warnings = new LinkedList<>();
    }

    protected List<String> extractVariablesFromFile(int[] excludedColumns) throws IOException {
        if (hasHeader) {
            return (commentMarker == null || commentMarker.trim().isEmpty())
                    ? extractVariables(excludedColumns)
                    : extractVariables(excludedColumns, commentMarker);
        } else {
            return generateVariables(excludedColumns);
        }
    }

    private List<String> extractVariables(int[] excludedColumns, String comment) throws IOException {
        List<String> variables = new LinkedList<>();

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
            boolean skipLine = false;
            boolean checkRequired = true;  // require check for comment
            boolean hasQuoteChar = false;
            boolean endOfLine = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !endOfLine) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currentChar >= SPACE || currentChar == delimiter) {
                        // case where line starts with spaces
                        if (currentChar == SPACE && currentChar != delimiter && dataBuilder.length() == 0) {
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
                                    variables.add(value);
                                    if (value.length() == 0) {
                                        errors.add(String.format("Column %d: missing variable name.", colNum));
                                    }
                                }
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        endOfLine = colNum > 0 || dataBuilder.length() > 0;
                    }
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
                    variables.add(value);
                    if (value.length() == 0) {
                        errors.add(String.format("Column %d: missing variable name.", colNum));
                    }
                }
            }

        }

        return variables;
    }

    private List<String> extractVariables(int[] excludedColumns) throws IOException {
        List<String> variables = new LinkedList<>();

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            int colNum = 0;
            int excludedIndex = 0;
            int numOfExCols = excludedColumns.length;
            boolean hasQuoteChar = false;
            boolean endOfLine = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !endOfLine) {
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
                                    variables.add(value);
                                    if (value.length() == 0) {
                                        errors.add(String.format("Column %d: missing variable name.", colNum));
                                    }
                                }
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        endOfLine = colNum > 0 || dataBuilder.length() > 0;
                    }
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
                    variables.add(value);
                    if (value.length() == 0) {
                        errors.add(String.format("Column %d: missing variable name.", colNum));
                    }
                }
            }

        }

        return variables;
    }

    private List<String> generateVariables(int[] excludedColumns) throws IOException {
        List<String> variables = new LinkedList<>();

        int numOfCols = getNumOfColumns();
        int length = excludedColumns.length;
        int excludedIndex = 0;
        for (int colNum = 1; colNum <= numOfCols; colNum++) {
            if (length > 0 && (excludedIndex < length && colNum == excludedColumns[excludedIndex])) {
                excludedIndex++;
            } else {
                variables.add(String.format("V%d", colNum));
            }
        }

        return variables;
    }

    @Override
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    @Override
    public boolean hasInfos() {
        return !infos.isEmpty();
    }

    @Override
    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    @Override
    public List<String> getErrors() {
        return errors;
    }

    @Override
    public List<String> getInfos() {
        return infos;
    }

    @Override
    public List<String> getWarnings() {
        return warnings;
    }

}
