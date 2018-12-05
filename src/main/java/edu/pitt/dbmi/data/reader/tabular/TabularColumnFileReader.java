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

import edu.pitt.dbmi.data.reader.DataReaderException;
import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Nov 7, 2018 2:24:23 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularColumnFileReader extends AbstractTabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TabularColumnFileReader.class);

    public TabularColumnFileReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    public TabularDataColumn[] readInDataColumns(Set<String> excludedVariables, boolean isDiscrete) throws IOException {
        Set<String> excludedVars = (excludedVariables == null)
                ? Collections.EMPTY_SET
                : excludedVariables.stream().map(String::trim).collect(Collectors.toSet());

        if (hasHeader) {
            return getColumns(toColumnNumbers(excludedVars), isDiscrete);
        } else {
            return generateColumns(getNumberOfColumns(), new int[0], isDiscrete);
        }
    }

    public TabularDataColumn[] readInDataColumns(int[] excludedColumns, boolean isDiscrete) throws IOException {
        int size = (excludedColumns == null) ? 0 : excludedColumns.length;
        int[] excludedCols = new int[size];
        if (size > 0) {
            System.arraycopy(excludedColumns, 0, excludedCols, 0, size);
            Arrays.sort(excludedCols);
        }

        int numOfCols = getNumberOfColumns();
        int[] validCols = extractValidColumnNumbers(numOfCols, excludedCols);

        return hasHeader ? getColumns(validCols, isDiscrete) : generateColumns(numOfCols, validCols, isDiscrete);
    }

    public TabularDataColumn[] readInDataColumns(boolean isDiscrete) throws IOException {
        return readInDataColumns(Collections.EMPTY_SET, isDiscrete);
    }

    /**
     * Analyze the column data to determine if it contains discrete data based
     * on the number of categories. If the number of categories of a column is
     * equal to or less than the given number of categories, it will be
     * considered to have discrete data. Else, it is considered to have
     * continuous data.
     *
     * @param columns
     * @param numOfCategories maximum number of categories to be consider
     * discrete
     * @throws IOException
     */
    public void determineDiscreteDataColumns(TabularDataColumn[] columns, int numOfCategories) throws IOException {
        int numOfCols = columns.length;
        Set<String>[] columnCategories = new Set[numOfCols];
        for (int i = 0; i < numOfCols; i++) {
            columnCategories[i] = new HashSet<>();
        }
        try (InputStream in = Files.newInputStream(dataFile.toPath(), StandardOpenOption.READ)) {
            boolean skipHeader = hasHeader;
            boolean skip = false;
            boolean hasSeenNonblankChar = false;
            boolean hasQuoteChar = false;

            byte delimChar = delimiter.getByteValue();

            // comment marker check
            byte[] comment = commentMarker.getBytes();
            int cmntIndex = 0;
            boolean checkForComment = comment.length > 0;

            int colNum = 0;
            int lineNum = 1;

            int columnIndex = 0;

            int maxCategoryToAdd = numOfCategories + 1;

            StringBuilder dataBuilder = new StringBuilder();
            byte prevChar = -1;
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1 && !Thread.currentThread().isInterrupted()) {
                int i = 0; // buffer array index

                if (skipHeader) {
                    boolean finished = false;
                    for (; i < len && !finished && !Thread.currentThread().isInterrupted(); i++) {
                        byte currChar = buffer[i];

                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                                prevChar = currChar;
                                continue;
                            }

                            finished = hasSeenNonblankChar && !skip;
                            if (finished) {
                                skipHeader = false;
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
                        }

                        prevChar = currChar;
                    }
                }

                for (; i < len && !Thread.currentThread().isInterrupted(); i++) {
                    byte currChar = buffer[i];

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            prevChar = currChar;
                            continue;
                        }

                        if (hasSeenNonblankChar && !skip) {
                            colNum++;

                            // ensure we don't go out of bound
                            if (columnIndex < numOfCols) {
                                TabularDataColumn dataColumn = columns[columnIndex];
                                if (dataColumn.getColumnNumber() == colNum) {
                                    String value = dataBuilder.toString().trim();
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        Set<String> categories = columnCategories[columnIndex];
                                        if (categories.size() < maxCategoryToAdd) {
                                            categories.add(value);
                                        }
                                    }

                                    columnIndex++;
                                }

                                // ensure we have enough data
                                if (columnIndex < numOfCols) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, columnIndex, numOfCols);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }
                            } else {
                                String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfCols + 1, numOfCols);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }
                        }

                        lineNum++;

                        // clear data
                        dataBuilder.delete(0, dataBuilder.length());

                        // reset states
                        skip = false;
                        hasSeenNonblankChar = false;
                        cmntIndex = 0;
                        checkForComment = comment.length > 0;
                        columnIndex = 0;
                        colNum = 0;
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
                        } else {
                            if (hasQuoteChar) {
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
                                    colNum++;

                                    // ensure we don't go out of bound
                                    if (columnIndex < numOfCols) {
                                        TabularDataColumn column = columns[columnIndex];
                                        if (column.getColumnNumber() == colNum) {
                                            String value = dataBuilder.toString().trim();
                                            if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                                Set<String> categories = columnCategories[columnIndex];
                                                if (categories.size() < maxCategoryToAdd) {
                                                    categories.add(value);
                                                }
                                            }

                                            columnIndex++;
                                        }
                                    } else {
                                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfCols + 1, numOfCols);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
                                    }

                                    // clear data
                                    dataBuilder.delete(0, dataBuilder.length());
                                } else {
                                    dataBuilder.append((char) currChar);
                                }
                            }
                        }
                    }

                    prevChar = currChar;
                }
            }

            if (!skipHeader && hasSeenNonblankChar && !skip) {
                colNum++;

                // ensure we don't go out of bound
                if (columnIndex < numOfCols) {
                    TabularDataColumn dataColumn = columns[columnIndex];
                    if (dataColumn.getColumnNumber() == colNum) {
                        String value = dataBuilder.toString().trim();
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            Set<String> categories = columnCategories[columnIndex];
                            if (categories.size() < maxCategoryToAdd) {
                                categories.add(value);
                            }
                        }

                        columnIndex++;
                    }

                    // ensure we have enough data
                    if (columnIndex < numOfCols) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, columnIndex, numOfCols);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                } else {
                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfCols + 1, numOfCols);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        for (int i = 0; i < numOfCols; i++) {
            columns[i].setDiscrete(columnCategories[i].size() <= numOfCategories);
        }
    }

    protected TabularDataColumn[] getColumns(int[] excludedColumns, boolean isDiscrete) throws IOException {
        List<TabularDataColumn> columns = new LinkedList<>();

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
                        finished = hasSeenNonblankChar && !skip;
                        if (finished) {
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            colNum++;
                            if (numOfExCols == 0 || exColsIndex >= numOfExCols || colNum != excludedColumns[exColsIndex]) {
                                if (value.isEmpty()) {
                                    String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                } else {
                                    columns.add(new TabularDataColumn(value, colNum, isDiscrete));
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
                        } else {
                            if (hasQuoteChar) {
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
                                        if (value.isEmpty()) {
                                            String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                            LOGGER.error(errMsg);
                                            throw new DataReaderException(errMsg);
                                        } else {
                                            columns.add(new TabularDataColumn(value, colNum, isDiscrete));
                                        }
                                    }

                                } else {
                                    dataBuilder.append((char) currChar);
                                }
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
                    if (value.isEmpty()) {
                        String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    } else {
                        columns.add(new TabularDataColumn(value, colNum, isDiscrete));
                    }
                }
            }
        }

        return columns.toArray(new TabularDataColumn[columns.size()]);
    }

    /**
     * Generate columns for tabular data that does not have a header.
     *
     * @param numOfCols maximum number of columns to generate
     * @param excludedCols list of columns to exclude
     * @param isDiscrete true if the column contains discrete data
     * @return
     */
    protected TabularDataColumn[] generateColumns(int numOfCols, int[] excludedCols, boolean isDiscrete) {
        List<TabularDataColumn> columns = new LinkedList<>();

        String prefix = "V";
        int exclColIndex = 0;
        for (int col = 1; col <= numOfCols && !Thread.currentThread().isInterrupted(); col++) {
            if (exclColIndex < excludedCols.length && col == excludedCols[exclColIndex]) {
                exclColIndex++;
            } else {
                columns.add(new TabularDataColumn(prefix + col, col, isDiscrete));
            }
        }

        return columns.toArray(new TabularDataColumn[columns.size()]);
    }

    public final class TabularDataColumn {

        private final String name;
        private final int columnNumber;
        private boolean discrete;

        private TabularDataColumn(String name, int columnNumber) {
            this.name = name;
            this.columnNumber = columnNumber;
        }

        private TabularDataColumn(String name, int columnNumber, boolean discrete) {
            this(name, columnNumber);
            this.discrete = discrete;
        }

        @Override
        public String toString() {
            return "TabularDataColumn{" + "name=" + name + ", columnNumber=" + columnNumber + ", discrete=" + discrete + '}';
        }

        public String getName() {
            return name;
        }

        public int getColumnNumber() {
            return columnNumber;
        }

        public boolean isDiscrete() {
            return discrete;
        }

        public void setDiscrete(boolean discrete) {
            this.discrete = discrete;
        }

    }

}
