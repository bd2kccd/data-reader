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
package edu.pitt.dbmi.data.reader.tabular;

import edu.pitt.dbmi.data.reader.AbstractDataReader;
import edu.pitt.dbmi.data.reader.DataColumn;
import edu.pitt.dbmi.data.reader.DataReaderException;
import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Nov 5, 2018 2:55:51 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataReader extends AbstractDataReader implements TabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTabularDataReader.class);

    protected boolean hasHeader;

    public AbstractTabularDataReader(Path dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    protected int[] toColumnNumbers(Set<String> columnNames) throws IOException {
        if (columnNames.isEmpty()) {
            return new int[0];
        }

        List<Integer> colNums = new LinkedList<>();
        try (InputStream in = Files.newInputStream(dataFile, StandardOpenOption.READ)) {
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

    public DataColumn[] extractDataColumns(Set<String> excludedVariables) throws IOException {
        Set<String> excludedVars = (excludedVariables == null)
                ? Collections.EMPTY_SET
                : excludedVariables.stream().map(String::trim).collect(Collectors.toSet());

        if (hasHeader) {
            return getDataColumns(toColumnNumbers(excludedVars));
        } else {
            return generateDataColumns(getNumberOfColumns(), toColumnNumbers(excludedVars));
        }
    }

    protected DataColumn[] extractDataColumns(int[] excludedColumns) throws IOException {
        int size = (excludedColumns == null) ? 0 : excludedColumns.length;
        int[] excludedCols = new int[size];
        if (size > 0) {
            System.arraycopy(excludedColumns, 0, excludedCols, 0, size);
            Arrays.sort(excludedCols);
        }

        int numOfCols = getNumberOfColumns();
        int[] validCols = extractValidColumnNumbers(numOfCols, excludedCols);

        return hasHeader ? getDataColumns(validCols) : generateDataColumns(numOfCols, validCols);
    }

    protected DataColumn[] getDataColumns(int[] excludedColumns) throws IOException {
        List<DataColumn> dataColumns = new LinkedList<>();

        try (InputStream in = Files.newInputStream(dataFile, StandardOpenOption.READ)) {
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
                        finished = hasSeenNonblankChar && !skip;
                        if (finished) {
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            colNum++;
                            if (numOfExCols == 0 || exColsIndex >= numOfExCols || colNum != excludedColumns[exColsIndex]) {
                                if (value.length() > 0) {
                                    dataColumns.add(new DataColumn(value, colNum));
                                } else {
                                    String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
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
                                if (numOfExCols > 0 && (exColsIndex < numOfExCols && colNum == excludedColumns[exColsIndex])) {
                                    exColsIndex++;
                                } else {
                                    if (value.length() > 0) {
                                        dataColumns.add(new DataColumn(value, colNum));
                                    } else {
                                        String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
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
                    if (value.length() > 0) {
                        dataColumns.add(new DataColumn(value, colNum));
                    } else {
                        String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }
            }
        }

        return dataColumns.toArray(new DataColumn[dataColumns.size()]);
    }

    /**
     * Save all the column numbers that are between 1 and numOfCols, inclusive.
     *
     * @param numOfCols used as the maximum column number
     * @param cols sorted array of column numbers
     * @return sorted array of valid column numbers
     */
    protected int[] extractValidColumnNumbers(int numOfCols, int[] cols) {
        Set<Integer> colNums = new TreeSet<>();

        for (int col : cols) {
            if (col > 0 && col <= numOfCols) {
                colNums.add(col);
            }
        }

        return colNums.stream().mapToInt(e -> e).toArray();
    }

    /**
     * Generate a list of data column for tabular data that does not have a
     * header.
     *
     * @param numOfCols number of column names to generate
     * @param excludedCols sorted array of column numbers to exclude
     * @return list of column names
     */
    protected DataColumn[] generateDataColumns(int numOfCols, int[] excludedCols) {
        List<DataColumn> dataColumns = new LinkedList<>();

        String prefix = "X";
        int len = excludedCols.length;
        int index = 0;
        boolean checkForExcludedCols = len > 0;
        for (int col = 1; col <= numOfCols && !Thread.currentThread().isInterrupted(); col++) {
            if (checkForExcludedCols && (index < len && col == excludedCols[index])) {
                index++;
            } else {
                dataColumns.add(new DataColumn(prefix, col));
            }
        }

        return dataColumns.toArray(new DataColumn[dataColumns.size()]);
    }

    @Override
    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    @Override
    public int getNumberOfColumns() throws IOException {
        return countNumberOfColumns();
    }

    @Override
    public int getNumberOfRows() throws IOException {
        return hasHeader
                ? countNumberOfLines() - 1
                : countNumberOfLines();
    }

}
