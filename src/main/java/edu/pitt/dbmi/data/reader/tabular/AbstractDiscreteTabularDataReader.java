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

import edu.pitt.dbmi.data.reader.DataReaderException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Feb 14, 2017 2:06:43 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractDiscreteTabularDataReader extends AbstractTabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDiscreteTabularDataReader.class);

    public AbstractDiscreteTabularDataReader(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    protected DiscreteVarInfo[] extractVariablesFromFile(int[] excludedColumns) throws IOException {
        if (hasHeader) {
            return commentMarker.isEmpty()
                    ? extractVariables(excludedColumns)
                    : extractVariables(excludedColumns, commentMarker);
        } else {
            return generateVariables(excludedColumns);
        }
    }

    protected void extractVariableDataFromFile(DiscreteVarInfo[] varInfos, int[] excludedColumns) throws IOException {
        if (commentMarker.isEmpty()) {
            extractVariableData(varInfos, excludedColumns);
        } else {
            extractVariableData(varInfos, excludedColumns, commentMarker);
        }
    }

    private void extractVariableData(DiscreteVarInfo[] varInfos, int[] excludedColumns, String comment) throws IOException {
    }

    private void extractVariableData(DiscreteVarInfo[] varInfos, int[] excludedColumns) throws IOException {
        int numCols = getNumOfColumns();
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            boolean skipHeader = hasHeader;
            boolean hasQuoteChar = false;
            boolean skipLine = false;
            boolean endOfLine = false;
            int numOfExCols = excludedColumns.length;
            int lineNumber = 1; // actual row number
            int colNum = 0;  // actual columm number
            int excludedIndex = 0;
            int varInfoIndex = 0;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipHeader) {
                    while (buffer.hasRemaining() && !endOfLine) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                endOfLine = true;
                                lineNumber++;
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            skipLine = true;
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            lineNumber++;
                        }
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
                                    if (value.length() > 0) {
                                        varInfos[varInfoIndex++].setValue(value);
                                    } else {
                                        String errMsg = String.format("Missing data one line %d column %d.", lineNumber, colNum);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
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
                                if (value.length() > 0) {
                                    varInfos[varInfoIndex++].setValue(value);
                                } else {
                                    String errMsg = String.format("Missing data one line %d column %d.", lineNumber, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }
                            }
                        }

                        colNum = 0;
                        excludedIndex = 0;
                        varInfoIndex = 0;
                        lineNumber++;
                    }
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
                    if (value.length() > 0) {
                        varInfos[varInfoIndex++].setValue(value);
                    } else {
                        String errMsg = String.format("Missing data one line %d column %d.", lineNumber, colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }
            }
        }
    }

    private DiscreteVarInfo[] extractVariables(int[] excludedColumns, String comment) throws IOException {
        int numOfCols = getNumOfColumns();
        int numOfExCols = excludedColumns.length;
        int varInfoSize = numOfCols - numOfExCols;
        DiscreteVarInfo[] varInfos = new DiscreteVarInfo[varInfoSize];

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = comment.getBytes();
            int index = 0;
            int colNum = 0;
            int excludedIndex = 0;
            int varInfoIndex = 0;
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
                                        varInfos[varInfoIndex++] = new DiscreteVarInfo(value);
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

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    if (value.length() > 0) {
                        varInfos[varInfoIndex++] = new DiscreteVarInfo(value);
                    } else {
                        String errMsg = String.format("Missing variable name at column %d.", colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }
            }

        }

        return varInfos;
    }

    private DiscreteVarInfo[] extractVariables(int[] excludedColumns) throws IOException {
        int numOfCols = getNumOfColumns();
        int numOfExCols = excludedColumns.length;
        int varInfoSize = numOfCols - numOfExCols;
        DiscreteVarInfo[] varInfos = new DiscreteVarInfo[varInfoSize];

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            int colNum = 0;
            int excludedIndex = 0;
            int varInfoIndex = 0;
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
                                        varInfos[varInfoIndex++] = new DiscreteVarInfo(value);
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

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    if (value.length() > 0) {
                        varInfos[varInfoIndex++] = new DiscreteVarInfo(value);
                    } else {
                        String errMsg = String.format("Missing variable name at column %d.", colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }
            }

        }

        return varInfos;
    }

    /**
     *
     * @param excludedColumns sorted array of column numbers
     * @return
     * @throws IOException
     */
    private DiscreteVarInfo[] generateVariables(int[] excludedColumns) throws IOException {
        int numOfCols = getNumOfColumns();
        int numOfExCols = excludedColumns.length;
        int size = numOfCols - numOfExCols;
        int exColIndex = 0;
        int varInfoIndex = 0;
        DiscreteVarInfo[] varInfos = new DiscreteVarInfo[size];
        for (int colNum = 1; colNum <= numOfCols; colNum++) {
            if (numOfExCols > 0 && (exColIndex < numOfExCols && colNum == excludedColumns[exColIndex])) {
                exColIndex++;
            } else {
                varInfos[varInfoIndex++] = new DiscreteVarInfo(String.format("V%d", colNum));
            }
        }

        return varInfos;
    }

    /**
     * This internal class is used to hold information about discrete variables
     * for discretization.
     */
    public static class DiscreteVarInfo {

        private final String name;
        private final Map<String, Integer> values;

        private final List<String> categories;

        public DiscreteVarInfo(String name) {
            this.name = name;
            this.values = new TreeMap<>();
            this.categories = new ArrayList<>();
        }

        public void recategorize() {
            Set<String> keyset = values.keySet();
            int count = 0;
            for (String key : keyset) {
                values.put(key, count++);
                categories.add(key);
            }
        }

        @Override
        public String toString() {
            return "DiscreteVarInfo{" + "name=" + name + ", values=" + values + ", categories=" + categories + '}';
        }

        public String getName() {
            return name;
        }

        public Integer getEncodeValue(String value) {
            return values.get(value);
        }

        public void setValue(String value) {
            this.values.put(value, null);
        }

        public List<String> getCategories() {
            return categories;
        }

    }

}
