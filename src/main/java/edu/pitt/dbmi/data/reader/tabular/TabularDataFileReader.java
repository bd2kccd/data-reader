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

import edu.pitt.dbmi.data.reader.AbstractDataFileReader;
import edu.pitt.dbmi.data.reader.ContinuousData;
import edu.pitt.dbmi.data.reader.Data;
import edu.pitt.dbmi.data.reader.DataColumn;
import edu.pitt.dbmi.data.reader.DataReaderException;
import edu.pitt.dbmi.data.reader.Delimiter;
import edu.pitt.dbmi.data.reader.DiscreteDataColumn;
import edu.pitt.dbmi.data.reader.MixedTabularData;
import edu.pitt.dbmi.data.reader.VerticalDiscreteData;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Nov 15, 2018 5:22:50 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularDataFileReader extends AbstractDataFileReader implements TabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TabularDataFileReader.class);

    public TabularDataFileReader(Path dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    public Data readInData(DataColumn[] dataColumns, boolean hasHeader) throws IOException {
        if (dataColumns == null) {
            return null;
        }

        boolean isDiscrete = false;
        boolean isContinuous = false;
        for (DataColumn dataColumn : dataColumns) {
            if (dataColumn.isDiscrete()) {
                isDiscrete = true;
            } else {
                isContinuous = true;
            }

            if (isDiscrete && isContinuous) {
                break;
            }
        }

        if (isDiscrete && isContinuous) {
            return readInMixedData(dataColumns, hasHeader);
        } else if (isContinuous) {
            return readInContinuousData(dataColumns, hasHeader);
        } else if (isDiscrete) {
            return readInDiscreteData(dataColumns, hasHeader);
        } else {
            return null;
        }
    }

    private Data readInMixedData(DataColumn[] dataColumns, boolean hasHeader) throws IOException {
        int numOfCols = dataColumns.length;
        int numOfRows = hasHeader ? countNumberOfLines() - 1 : countNumberOfLines();

        DiscreteDataColumn[] discreteDataColumns = new DiscreteDataColumn[numOfCols];
        double[][] continuousData = new double[numOfCols][];
        int[][] discreteData = new int[numOfCols][];
        for (int i = 0; i < numOfCols; i++) {
            DataColumn dataColumn = dataColumns[i];

            // initialize data
            if (dataColumn.isDiscrete()) {
                discreteData[i] = new int[numOfRows];
            } else {
                continuousData[i] = new double[numOfRows];
            }

            // initialize columns
            discreteDataColumns[i] = new MixedTabularFileDataColumn(dataColumn);
        }

        readInDiscreteCategorizes(discreteDataColumns, hasHeader);
        readInMixedData(discreteDataColumns, hasHeader, continuousData, discreteData);

        return new MixedTabularFileData(numOfRows, discreteDataColumns, continuousData, discreteData);
    }

    protected void readInMixedData(DiscreteDataColumn[] dataColumns, boolean hasHeader, double[][] continuousData, int[][] discreteData) throws IOException {
        int numOfCols = dataColumns.length;
        try (InputStream in = Files.newInputStream(dataFile, StandardOpenOption.READ)) {
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

            int row = 0;  // array row number
            int col = 0;  // array column number

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

                            DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                            DataColumn dataColumn = discreteDataColumn.getDataColumn();
                            if (dataColumn.getColumnNumber() == colNum) {
                                String value = dataBuilder.toString().trim();
                                if (dataColumn.isDiscrete()) {
                                    if (value.isEmpty() || value.equals(missingDataMarker)) {
                                        discreteData[col++][row] = DISCRETE_MISSING_VALUE;
                                    } else {
                                        discreteData[col++][row] = discreteDataColumn.getEncodeValue(value);
                                    }
                                } else {
                                    if (value.isEmpty() || value.equals(missingDataMarker)) {
                                        continuousData[col++][row] = CONTINUOUS_MISSING_VALUE;
                                    } else {
                                        try {
                                            continuousData[col++][row] = Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                            LOGGER.error(errMsg, exception);
                                            throw new DataReaderException(errMsg);
                                        }
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

                            row++;
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
                        col = 0;
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

                                    DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                                    DataColumn dataColumn = discreteDataColumn.getDataColumn();
                                    if (dataColumn.getColumnNumber() == colNum) {
                                        String value = dataBuilder.toString().trim();
                                        if (dataColumn.isDiscrete()) {
                                            if (value.isEmpty() || value.equals(missingDataMarker)) {
                                                discreteData[col++][row] = DISCRETE_MISSING_VALUE;
                                            } else {
                                                discreteData[col++][row] = discreteDataColumn.getEncodeValue(value);
                                            }
                                        } else {
                                            if (value.isEmpty() || value.equals(missingDataMarker)) {
                                                continuousData[col++][row] = CONTINUOUS_MISSING_VALUE;
                                            } else {
                                                try {
                                                    continuousData[col++][row] = Double.parseDouble(value);
                                                } catch (NumberFormatException exception) {
                                                    String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                                    LOGGER.error(errMsg, exception);
                                                    throw new DataReaderException(errMsg);
                                                }
                                            }
                                        }

                                        columnIndex++;
                                        if (columnIndex == numOfCols) {
                                            row++;
                                            skip = true;
                                        }
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

                DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                DataColumn dataColumn = discreteDataColumn.getDataColumn();
                if (dataColumn.getColumnNumber() == colNum) {
                    String value = dataBuilder.toString().trim();
                    if (dataColumn.isDiscrete()) {
                        if (value.isEmpty() || value.equals(missingDataMarker)) {
                            discreteData[col++][row] = DISCRETE_MISSING_VALUE;
                        } else {
                            discreteData[col++][row] = discreteDataColumn.getEncodeValue(value);
                        }
                    } else {
                        if (value.isEmpty() || value.equals(missingDataMarker)) {
                            continuousData[col++][row] = CONTINUOUS_MISSING_VALUE;
                        } else {
                            try {
                                continuousData[col++][row] = Double.parseDouble(value);
                            } catch (NumberFormatException exception) {
                                String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                LOGGER.error(errMsg, exception);
                                throw new DataReaderException(errMsg);
                            }
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
            }
        }
    }

    @Override
    public void determineDiscreteDataColumns(DataColumn[] dataColumns, int numberOfCategories, boolean hasHeader) throws IOException {
        int numOfCols = dataColumns.length;
        Set<String>[] columnCategories = new Set[numOfCols];
        for (int i = 0; i < numOfCols; i++) {
            columnCategories[i] = new HashSet<>();
        }

        try (InputStream in = Files.newInputStream(dataFile, StandardOpenOption.READ)) {
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

            int maxCategoryToAdd = numberOfCategories + 1;

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

                            DataColumn dataColumn = dataColumns[columnIndex];
                            if (dataColumn.getColumnNumber() == colNum) {
                                String value = dataBuilder.toString().trim();
                                if (!(value.isEmpty() || value.equals(missingDataMarker))) {
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

                                    DataColumn dataColumn = dataColumns[columnIndex];
                                    if (dataColumn.getColumnNumber() == colNum) {
                                        String value = dataBuilder.toString().trim();
                                        if (!(value.isEmpty() || value.equals(missingDataMarker))) {
                                            Set<String> categories = columnCategories[columnIndex];
                                            if (categories.size() < maxCategoryToAdd) {
                                                categories.add(value);
                                            }
                                        }

                                        columnIndex++;
                                        if (columnIndex == numOfCols) {
                                            skip = true;
                                        }
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

                DataColumn dataColumn = dataColumns[columnIndex];
                if (dataColumn.getColumnNumber() == colNum) {
                    String value = dataBuilder.toString().trim();
                    if (!(value.isEmpty() || value.equals(missingDataMarker))) {
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
            }
        }

        for (int i = 0; i < numOfCols; i++) {
            dataColumns[i].setDiscrete(columnCategories[i].size() <= numberOfCategories);
        }
    }

    private Data readInDiscreteData(DataColumn[] dataColumns, boolean hasHeader) throws IOException {
        DiscreteDataColumn[] discreteDataColumns = Arrays.stream(dataColumns)
                .map(DiscreteTabularFileDataColumn::new)
                .toArray(DiscreteDataColumn[]::new);

        readInDiscreteCategorizes(discreteDataColumns, hasHeader);

        int[][] data = readInDiscreteData(discreteDataColumns, hasHeader);

        return new VerticalDiscreteTabularFileData(discreteDataColumns, data);
    }

    private Data readInContinuousData(DataColumn[] dataColumns, boolean hasHeader) throws IOException {
        int numOfCols = dataColumns.length;
        int numOfRows = hasHeader ? countNumberOfLines() - 1 : countNumberOfLines();
        double[][] data = new double[numOfRows][numOfCols];

        try (InputStream in = Files.newInputStream(dataFile, StandardOpenOption.READ)) {
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

            int row = 0;  // array row number
            int col = 0;  // array column number

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

                            DataColumn dataColumn = dataColumns[columnIndex];
                            if (dataColumn.getColumnNumber() == colNum) {
                                String value = dataBuilder.toString().trim();
                                if (value.isEmpty() || value.equals(missingDataMarker)) {
                                    data[row][col++] = CONTINUOUS_MISSING_VALUE;
                                } else {
                                    try {
                                        data[row][col++] = Double.parseDouble(value);
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Non-continuous number %s on line %d at column %d.", value, lineNum, colNum);
                                        LOGGER.error(errMsg, exception);
                                        throw new DataReaderException(errMsg);
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

                            row++;
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
                        col = 0;
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

                                    DataColumn dataColumn = dataColumns[columnIndex];
                                    if (dataColumn.getColumnNumber() == colNum) {
                                        String value = dataBuilder.toString().trim();
                                        if (value.isEmpty() || value.equals(missingDataMarker)) {
                                            data[row][col++] = CONTINUOUS_MISSING_VALUE;
                                        } else {
                                            try {
                                                data[row][col++] = Double.parseDouble(value);
                                            } catch (NumberFormatException exception) {
                                                String errMsg = String.format("Non-continuous number %s on line %d at column %d.", value, lineNum, colNum);
                                                LOGGER.error(errMsg, exception);
                                                throw new DataReaderException(errMsg);
                                            }
                                        }

                                        columnIndex++;
                                        if (columnIndex == numOfCols) {
                                            row++;
                                            skip = true;
                                        }
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

                DataColumn dataColumn = dataColumns[columnIndex];
                if (dataColumn.getColumnNumber() == colNum) {
                    String value = dataBuilder.toString().trim();
                    if (value.isEmpty() || value.equals(missingDataMarker)) {
                        data[row][col++] = CONTINUOUS_MISSING_VALUE;
                    } else {
                        try {
                            data[row][col++] = Double.parseDouble(value);
                        } catch (NumberFormatException exception) {
                            String errMsg = String.format("Non-continuous number %s on line %d at column %d.", value, lineNum, colNum);
                            LOGGER.error(errMsg, exception);
                            throw new DataReaderException(errMsg);
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
            }
        }

        return new ContinuousTabularFileData(dataColumns, data);
    }

    protected int[][] readInDiscreteData(DiscreteDataColumn[] dataColumns, boolean hasHeader) throws IOException {
        int numOfCols = dataColumns.length;
        int numOfRows = hasHeader ? countNumberOfLines() - 1 : countNumberOfLines();
        int[][] data = new int[numOfCols][numOfRows];

        try (InputStream in = Files.newInputStream(dataFile, StandardOpenOption.READ)) {
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

            int row = 0;  // array row number
            int col = 0;  // array column number

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

                            DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                            DataColumn dataColumn = discreteDataColumn.getDataColumn();
                            if (dataColumn.getColumnNumber() == colNum) {
                                String value = dataBuilder.toString().trim();
                                if (value.isEmpty() || value.equals(missingDataMarker)) {
                                    data[col++][row] = DISCRETE_MISSING_VALUE;
                                } else {
                                    data[col++][row] = discreteDataColumn.getEncodeValue(value);
                                }

                                columnIndex++;
                            }

                            // ensure we have enough data
                            if (columnIndex < numOfCols) {
                                String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, columnIndex, numOfCols);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }

                            row++;
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
                        col = 0;
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

                                    DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                                    DataColumn dataColumn = discreteDataColumn.getDataColumn();
                                    if (dataColumn.getColumnNumber() == colNum) {
                                        String value = dataBuilder.toString().trim();
                                        if (value.isEmpty() || value.equals(missingDataMarker)) {
                                            data[col++][row] = DISCRETE_MISSING_VALUE;
                                        } else {
                                            data[col++][row] = discreteDataColumn.getEncodeValue(value);
                                        }

                                        columnIndex++;
                                        if (columnIndex == numOfCols) {
                                            row++;
                                            skip = true;
                                        }
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

                DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                DataColumn dataColumn = discreteDataColumn.getDataColumn();
                if (dataColumn.getColumnNumber() == colNum) {
                    String value = dataBuilder.toString().trim();
                    if (value.isEmpty() || value.equals(missingDataMarker)) {
                        data[col++][row] = DISCRETE_MISSING_VALUE;
                    } else {
                        data[col++][row] = discreteDataColumn.getEncodeValue(value);
                    }

                    columnIndex++;
                }

                // ensure we have enough data
                if (columnIndex < numOfCols) {
                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, columnIndex, numOfCols);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        return data;
    }

    protected void readInDiscreteCategorizes(DiscreteDataColumn[] dataColumns, boolean hasHeader) throws IOException {
        int numOfCols = dataColumns.length;
        try (InputStream in = Files.newInputStream(dataFile, StandardOpenOption.READ)) {
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

                            DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                            DataColumn dataColumn = discreteDataColumn.getDataColumn();
                            if (dataColumn.getColumnNumber() == colNum) {
                                if (dataColumn.isDiscrete()) {
                                    String value = dataBuilder.toString().trim();
                                    if (value.length() > 0 && !value.equals(missingDataMarker)) {
                                        discreteDataColumn.setValue(value);
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

                                    DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                                    DataColumn dataColumn = discreteDataColumn.getDataColumn();
                                    if (dataColumn.getColumnNumber() == colNum) {
                                        if (dataColumn.isDiscrete()) {
                                            String value = dataBuilder.toString().trim();
                                            if (value.length() > 0 && !value.equals(missingDataMarker)) {
                                                discreteDataColumn.setValue(value);
                                            }
                                        }

                                        columnIndex++;
                                        if (columnIndex == numOfCols) {
                                            skip = true;
                                        }
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

                DiscreteDataColumn discreteDataColumn = dataColumns[columnIndex];
                DataColumn dataColumn = discreteDataColumn.getDataColumn();
                if (dataColumn.getColumnNumber() == colNum) {
                    if (dataColumn.isDiscrete()) {
                        String value = dataBuilder.toString().trim();
                        if (value.length() > 0 && !value.equals(missingDataMarker)) {
                            discreteDataColumn.setValue(value);
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
            }
        }

        // recategorize values
        for (DiscreteDataColumn discreteDataColumn : dataColumns) {
            discreteDataColumn.recategorize();
        }
    }

    private final class MixedTabularFileDataColumn implements DiscreteDataColumn {

        private final DataColumn dataColumn;
        private final Map<String, Integer> values;
        private List<String> categories;

        public MixedTabularFileDataColumn(DataColumn dataColumn) {
            this.dataColumn = dataColumn;
            this.values = dataColumn.isDiscrete() ? new TreeMap<>() : null;
        }

        @Override
        public String toString() {
            return "MixedTabularFileDataColumn{" + "dataColumn=" + dataColumn + ", values=" + values + ", categories=" + categories + '}';
        }

        @Override
        public Integer getEncodeValue(String value) {
            return (values == null)
                    ? DISCRETE_MISSING_VALUE
                    : values.get(value);
        }

        @Override
        public void recategorize() {
            if (values != null) {
                Set<String> keyset = values.keySet();
                categories = new ArrayList<>(keyset.size());
                int count = 0;
                for (String key : keyset) {
                    values.put(key, count++);
                    categories.add(key);
                }
            }
        }

        @Override
        public List<String> getCategories() {
            return (categories == null) ? Collections.EMPTY_LIST : categories;
        }

        @Override
        public DataColumn getDataColumn() {
            return dataColumn;
        }

        @Override
        public void setValue(String value) {
            if (this.values != null) {
                this.values.put(value, null);
            }
        }

    }

    private final class DiscreteTabularFileDataColumn implements DiscreteDataColumn {

        private final DataColumn dataColumn;
        private final Map<String, Integer> values;
        private List<String> categories;

        public DiscreteTabularFileDataColumn(DataColumn dataColumn) {
            this.dataColumn = dataColumn;
            this.values = new TreeMap<>();
        }

        @Override
        public String toString() {
            return "DiscreteTabularFileDataColumn{" + "dataColumn=" + dataColumn + ", values=" + values + ", categories=" + categories + '}';
        }

        @Override
        public void recategorize() {
            Set<String> keyset = values.keySet();
            categories = new ArrayList<>(keyset.size());
            int count = 0;
            for (String key : keyset) {
                values.put(key, count++);
                categories.add(key);
            }
        }

        @Override
        public DataColumn getDataColumn() {
            return dataColumn;
        }

        @Override
        public List<String> getCategories() {
            return (categories == null)
                    ? Collections.EMPTY_LIST
                    : categories;
        }

        @Override
        public Integer getEncodeValue(String value) {
            return values.get(value);
        }

        @Override
        public void setValue(String value) {
            this.values.put(value, null);
        }

    }

    private final class MixedTabularFileData implements MixedTabularData {

        private final int numOfRows;
        private final DiscreteDataColumn[] dataColumns;
        private final double[][] continuousData;
        private final int[][] discreteData;

        public MixedTabularFileData(int numOfRows, DiscreteDataColumn[] dataColumns, double[][] continuousData, int[][] discreteData) {
            this.numOfRows = numOfRows;
            this.dataColumns = dataColumns;
            this.continuousData = continuousData;
            this.discreteData = discreteData;
        }

        @Override
        public int getNumOfRows() {
            return numOfRows;
        }

        @Override
        public DiscreteDataColumn[] getDataColumns() {
            return dataColumns;
        }

        @Override
        public double[][] getContinuousData() {
            return continuousData;
        }

        @Override
        public int[][] getDiscreteData() {
            return discreteData;
        }

    }

    private final class VerticalDiscreteTabularFileData implements VerticalDiscreteData {

        private final DiscreteDataColumn[] dataColumns;
        private final int[][] data;

        public VerticalDiscreteTabularFileData(DiscreteDataColumn[] dataColumns, int[][] data) {
            this.dataColumns = dataColumns;
            this.data = data;
        }

        @Override
        public DiscreteDataColumn[] getDataColumns() {
            return dataColumns;
        }

        @Override
        public int[][] getData() {
            return data;
        }

    }

    private final class ContinuousTabularFileData implements ContinuousData {

        private final DataColumn[] dataColumns;
        private final double[][] data;

        public ContinuousTabularFileData(DataColumn[] dataColumns, double[][] data) {
            this.dataColumns = dataColumns;
            this.data = data;
        }

        @Override
        public DataColumn[] getDataColumns() {
            return dataColumns;
        }

        @Override
        public double[][] getData() {
            return data;
        }

    }

}
