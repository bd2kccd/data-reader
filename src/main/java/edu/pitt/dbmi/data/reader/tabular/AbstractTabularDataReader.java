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
package edu.pitt.dbmi.data.reader.tabular;

import edu.pitt.dbmi.data.reader.AbstractDataReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * Feb 8, 2017 5:03:00 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataReader extends AbstractDataReader {

    protected boolean hasHeader;

    public AbstractTabularDataReader(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    protected int[] getVariableColumnNumbers(Set<String> variableNames) throws IOException {
        return commentMarker.isEmpty()
                ? getColumnNumbers(variableNames)
                : getColumnNumbers(variableNames, commentMarker);
    }

    private int[] getColumnNumbers(Set<String> variableNames, String comment) throws IOException {
        List<Integer> indexList = new LinkedList<>();
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = comment.getBytes();
            int index = 0;
            int columnCount = 0;
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
                                    columnCount = 0;
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
                                columnCount++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (variableNames.contains(value)) {
                                    indexList.add(columnCount);
                                }
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        endOfLine = columnCount > 0 || dataBuilder.length() > 0;
                    }
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);

            // data at the end of line
            if (columnCount > 0 || dataBuilder.length() > 0) {
                columnCount++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (variableNames.contains(value)) {
                    indexList.add(columnCount);
                }
            }

        }

        int[] indices = new int[indexList.size()];
        if (indices.length > 0) {
            int i = 0;
            for (Integer index : indexList) {
                indices[i++] = index;
            }
        }

        return indices;
    }

    private int[] getColumnNumbers(Set<String> variableNames) throws IOException {
        List<Integer> indexList = new LinkedList<>();
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            int columnCount = 0;
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
                                columnCount++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (variableNames.contains(value)) {
                                    indexList.add(columnCount);
                                }
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        endOfLine = columnCount > 0 || dataBuilder.length() > 0;
                    }
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);

            // data at the end of line
            if (columnCount > 0 || dataBuilder.length() > 0) {
                columnCount++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (variableNames.contains(value)) {
                    indexList.add(columnCount);
                }
            }

        }

        int[] indices = new int[indexList.size()];
        if (indices.length > 0) {
            int i = 0;
            for (Integer index : indexList) {
                indices[i++] = index;
            }
        }

        return indices;
    }

    protected int[] getValidColumnNumbers(int[] columnNumbers) throws IOException {
        Set<Integer> indices = new TreeSet<>();
        int numOfVars = getNumberOfColumns();
        for (int colNum : columnNumbers) {
            if (colNum > 0 && colNum <= numOfVars) {
                indices.add(colNum);
            }
        }

        int[] results = new int[indices.size()];
        int i = 0;
        for (Integer index : indices) {
            results[i++] = index;
        }

        return results;
    }

    public boolean isHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

}
