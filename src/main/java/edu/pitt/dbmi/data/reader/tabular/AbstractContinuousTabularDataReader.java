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

import edu.cmu.tetrad.data.ContinuousVariable;
import edu.cmu.tetrad.graph.Node;
import edu.pitt.dbmi.data.reader.DataReaderException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Feb 8, 2017 5:45:59 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractContinuousTabularDataReader extends AbstractTabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractContinuousTabularDataReader.class);

    public AbstractContinuousTabularDataReader(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    protected List<Node> extractVariablesFromData(int[] excludedColumns) throws IOException {
        if (hasHeader) {
            return (commentMarker == null || commentMarker.trim().isEmpty())
                    ? extractVariables(excludedColumns)
                    : extractVariables(excludedColumns, commentMarker);
        } else {
            return generateVariables(excludedColumns);
        }
    }

    private List<Node> extractVariables(int[] excludedColumns, String comment) throws IOException {
        List<Node> nodes = new LinkedList<>();

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
                                    if (value.length() > 0) {
                                        nodes.add(new ContinuousVariable(value));
                                    } else {
                                        String errMsg = String.format("Missing variable name at column %d.", colNum);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
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

                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                    excludedIndex++;
                } else {
                    if (value.length() > 0) {
                        nodes.add(new ContinuousVariable(value));
                    } else {
                        String errMsg = String.format("Missing variable name at column %d.", colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }
            }

        }

        return nodes;
    }

    private List<Node> extractVariables(int[] excludedColumns) throws IOException {
        List<Node> nodes = new LinkedList<>();

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
                                    if (value.length() > 0) {
                                        nodes.add(new ContinuousVariable(value));
                                    } else {
                                        String errMsg = String.format("Missing variable name at column %d.", colNum);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
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

                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                    excludedIndex++;
                } else {
                    if (value.length() > 0) {
                        nodes.add(new ContinuousVariable(value));
                    } else {
                        String errMsg = String.format("Missing variable name at column %d.", colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }
            }

        }

        return nodes;
    }

    /**
     *
     * @param excludedColumns sorted array of column numbers
     * @return
     * @throws IOException
     */
    private List<Node> generateVariables(int[] excludedColumns) throws IOException {
        List<Node> nodes = new LinkedList<>();

        int numOfCols = getNumOfColumns();
        int length = excludedColumns.length;
        int excludedIndex = 0;
        for (int colNum = 1; colNum <= numOfCols; colNum++) {
            if (length > 0 && (excludedIndex < length && colNum == excludedColumns[excludedIndex])) {
                excludedIndex++;
            } else {
                nodes.add(new ContinuousVariable(String.format("V%d", colNum)));
            }
        }

        return nodes;
    }

}
