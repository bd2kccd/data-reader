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
import edu.pitt.dbmi.data.reader.tabular.TabularColumnFileReader.TabularDataColumn;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Nov 7, 2018 2:34:40 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularDataFileReader extends AbstractTabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TabularDataFileReader.class);

    private static final double CONTINUOUS_MISSING_VALUE = Double.NaN;
    private static final int DISCRETE_MISSING_VALUE = -99;

    public TabularDataFileReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    public TabularData readInData(TabularDataColumn[] columns) throws IOException {
        boolean isDiscrete = false;
        boolean isContinuous = false;
        for (TabularDataColumn column : columns) {
            if (column.isDiscrete()) {
                isDiscrete = true;
            } else {
                isContinuous = true;
            }

            if (isDiscrete && isContinuous) {
                break;
            }
        }

        if (isDiscrete && isContinuous) {
            return readInMixedData(columns);
        } else if (isContinuous) {
            return readInContinuousData(columns);
        } else {
            return readInDiscreteData(columns);
        }
    }

    public TabularData readInMixedData(TabularDataColumn[] columns) throws IOException {
        int numOfCols = columns.length;
        int numOfRows = getNumberOfRows();

        MixedDataColumn[] mixedDataColumns = new MixedDataColumn[numOfCols];
        double[][] continuousData = new double[numOfCols][];
        int[][] discreteData = new int[numOfCols][];
        for (int i = 0; i < numOfCols; i++) {
            TabularDataColumn column = columns[i];

            // initialize data
            if (column.isDiscrete()) {
                discreteData[i] = new int[numOfRows];
            } else {
                continuousData[i] = new double[numOfRows];
            }

            // initialize columns
            mixedDataColumns[i] = new MixedDataColumn(column);
        }

        readInDiscreteCategorizes(mixedDataColumns);
        readInMixedData(mixedDataColumns, continuousData, discreteData);

        return new MixedTabularDataset(numOfRows, mixedDataColumns, continuousData, discreteData);
    }

    public TabularData readInDiscreteData(TabularDataColumn[] columns) throws IOException {
        // convert data columns to discrete columns
        DiscreteDataColumn[] discreteDataColumns = Arrays.stream(columns)
                .map(DiscreteDataColumn::new)
                .toArray(DiscreteDataColumn[]::new);

        readInDiscreteCategorizes(discreteDataColumns);

        int[][] data = readInDiscreteData(discreteDataColumns);

        return new VerticalDiscreteTabularDataset(discreteDataColumns, data);
    }

    protected void readInMixedData(DiscreteColumn[] columns, double[][] continuousData, int[][] discreteData) throws IOException {
        int numOfCols = columns.length;
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

                            // ensure we don't go out of bound
                            if (columnIndex < numOfCols) {
                                DiscreteColumn column = columns[columnIndex];
                                if (column.getColumnNumber() == colNum) {
                                    columnIndex++;

                                    String value = dataBuilder.toString().trim();
                                    if (column.isDiscrete()) {
                                        if (value.length() == 0 || value.equals(missingValueMarker)) {
                                            discreteData[col][row] = DISCRETE_MISSING_VALUE;
                                        } else {
                                            discreteData[col][row] = column.getEncodeValue(value);
                                        }
                                    } else {
                                        if (value.length() == 0 || value.equals(missingValueMarker)) {
                                            continuousData[col][row] = CONTINUOUS_MISSING_VALUE;
                                        } else {
                                            try {
                                                continuousData[col][row] = Double.parseDouble(value);
                                            } catch (NumberFormatException exception) {
                                                String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                                LOGGER.error(errMsg, exception);
                                                throw new DataReaderException(errMsg);
                                            }
                                        }
                                    }
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

                                    // ensure we don't go out of bound
                                    if (columnIndex < numOfCols) {
                                        DiscreteColumn column = columns[columnIndex];
                                        if (column.getColumnNumber() == colNum) {
                                            columnIndex++;

                                            String value = dataBuilder.toString().trim();
                                            if (column.isDiscrete()) {
                                                if (value.length() == 0 || value.equals(missingValueMarker)) {
                                                    discreteData[col][row] = DISCRETE_MISSING_VALUE;
                                                } else {
                                                    discreteData[col][row] = column.getEncodeValue(value);
                                                }
                                            } else {
                                                if (value.length() == 0 || value.equals(missingValueMarker)) {
                                                    continuousData[col][row] = CONTINUOUS_MISSING_VALUE;
                                                } else {
                                                    try {
                                                        continuousData[col][row] = Double.parseDouble(value);
                                                    } catch (NumberFormatException exception) {
                                                        String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                                        LOGGER.error(errMsg, exception);
                                                        throw new DataReaderException(errMsg);
                                                    }
                                                }
                                            }

                                            col++;
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
                    DiscreteColumn column = columns[columnIndex];
                    if (column.getColumnNumber() == colNum) {
                        columnIndex++;

                        String value = dataBuilder.toString().trim();
                        if (column.isDiscrete()) {
                            if (value.length() == 0 || value.equals(missingValueMarker)) {
                                discreteData[col][row] = DISCRETE_MISSING_VALUE;
                            } else {
                                discreteData[col][row] = column.getEncodeValue(value);
                            }
                        } else {
                            if (value.length() == 0 || value.equals(missingValueMarker)) {
                                continuousData[col][row] = CONTINUOUS_MISSING_VALUE;
                            } else {
                                try {
                                    continuousData[col][row] = Double.parseDouble(value);
                                } catch (NumberFormatException exception) {
                                    String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                    LOGGER.error(errMsg, exception);
                                    throw new DataReaderException(errMsg);
                                }
                            }
                        }
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
    }

    protected int[][] readInDiscreteData(DiscreteColumn[] columns) throws IOException {
        int numOfCols = columns.length;
        int numOfRows = getNumberOfRows();
        int[][] data = new int[numOfCols][numOfRows];

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

                            // ensure we don't go out of bound
                            if (columnIndex < numOfCols) {
                                DiscreteColumn column = columns[columnIndex];
                                if (column.getColumnNumber() == colNum) {
                                    columnIndex++;

                                    String value = dataBuilder.toString().trim();
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        data[col++][row] = column.getEncodeValue(value);
                                    } else {
                                        data[col++][row] = DISCRETE_MISSING_VALUE;
                                    }
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

                                    // ensure we don't go out of bound
                                    if (columnIndex < numOfCols) {
                                        DiscreteColumn column = columns[columnIndex];
                                        if (column.getColumnNumber() == colNum) {
                                            columnIndex++;

                                            String value = dataBuilder.toString().trim();
                                            if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                                data[col++][row] = column.getEncodeValue(value);
                                            } else {
                                                data[col++][row] = DISCRETE_MISSING_VALUE;
                                            }
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
                    DiscreteColumn column = columns[columnIndex];
                    if (column.getColumnNumber() == colNum) {
                        columnIndex++;

                        String value = dataBuilder.toString().trim();
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            data[col++][row] = column.getEncodeValue(value);
                        } else {
                            data[col++][row] = DISCRETE_MISSING_VALUE;
                        }
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

        return data;
    }

    protected void readInDiscreteCategorizes(DiscreteColumn[] columns) throws IOException {
        int numOfCols = columns.length;
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
                                DiscreteColumn column = columns[columnIndex];
                                if (column.getColumnNumber() == colNum) {
                                    columnIndex++;

                                    if (column.isDiscrete()) {
                                        String value = dataBuilder.toString().trim();
                                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                            column.setValue(value);
                                        }
                                    }
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
                                        DiscreteColumn column = columns[columnIndex];
                                        if (column.getColumnNumber() == colNum) {
                                            columnIndex++;

                                            if (column.isDiscrete()) {
                                                String value = dataBuilder.toString().trim();
                                                if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                                    column.setValue(value);
                                                }
                                            }
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
                    DiscreteColumn column = columns[columnIndex];
                    if (column.getColumnNumber() == colNum) {
                        columnIndex++;

                        if (column.isDiscrete()) {
                            String value = dataBuilder.toString().trim();
                            if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                column.setValue(value);
                            }
                        }
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

        // recategorize values
        for (DiscreteColumn column : columns) {
            column.recategorize();
        }
    }

    public TabularData readInContinuousData(TabularDataColumn[] columns) throws IOException {
        int numOfCols = columns.length;
        int numOfRows = getNumberOfRows();
        double[][] data = new double[numOfRows][numOfCols];

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

                            // ensure we don't go out of bound
                            if (columnIndex < numOfCols) {
                                TabularDataColumn column = columns[columnIndex];
                                if (column.getColumnNumber() == colNum) {
                                    columnIndex++;

                                    String value = dataBuilder.toString().trim();
                                    if (value.length() == 0 || value.equals(missingValueMarker)) {
                                        data[row][col++] = CONTINUOUS_MISSING_VALUE;
                                    } else {
                                        try {
                                            data[row][col++] = Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                            LOGGER.error(errMsg, exception);
                                            throw new DataReaderException(errMsg);
                                        }
                                    }
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

                                    // ensure we don't go out of bound
                                    if (columnIndex < numOfCols) {
                                        TabularDataColumn column = columns[columnIndex];
                                        if (column.getColumnNumber() == colNum) {
                                            columnIndex++;

                                            String value = dataBuilder.toString().trim();
                                            if (value.length() == 0 || value.equals(missingValueMarker)) {
                                                data[row][col++] = CONTINUOUS_MISSING_VALUE;
                                            } else {
                                                try {
                                                    data[row][col++] = Double.parseDouble(value);
                                                } catch (NumberFormatException exception) {
                                                    String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                                    LOGGER.error(errMsg, exception);
                                                    throw new DataReaderException(errMsg);
                                                }
                                            }
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
                    TabularDataColumn column = columns[columnIndex];
                    if (column.getColumnNumber() == colNum) {
                        columnIndex++;

                        String value = dataBuilder.toString().trim();
                        if (value.length() == 0 || value.equals(missingValueMarker)) {
                            data[row][col++] = CONTINUOUS_MISSING_VALUE;
                        } else {
                            try {
                                data[row][col++] = Double.parseDouble(value);
                            } catch (NumberFormatException exception) {
                                String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                LOGGER.error(errMsg, exception);
                                throw new DataReaderException(errMsg);
                            }
                        }
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

        return new ContinuousTabularDataset(columns, data);
    }

    public interface DiscreteColumn {

        public boolean isDiscrete();

        public void setValue(String value);

        public void recategorize();

        public List<String> getCategories();

        public Integer getEncodeValue(String value);

        public int getColumnNumber();

        public String getName();

    }

    public class MixedDataColumn implements DiscreteColumn {

        private final TabularDataColumn column;
        private final Map<String, Integer> values;
        private List<String> categories;

        private MixedDataColumn(TabularDataColumn column) {
            if (column == null) {
                throw new IllegalArgumentException("Column cannot be null.");
            }
            this.column = column;
            this.values = column.isDiscrete() ? new TreeMap<>() : null;
        }

        @Override
        public String toString() {
            return "MixedDataColumn{" + "column=" + column + ", values=" + values + ", categories=" + categories + '}';
        }

        @Override
        public boolean isDiscrete() {
            return column.isDiscrete();
        }

        @Override
        public void setValue(String value) {
            this.values.put(value, null);
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
        public Integer getEncodeValue(String value) {
            return (values == null)
                    ? DISCRETE_MISSING_VALUE
                    : values.get(value);
        }

        @Override
        public int getColumnNumber() {
            return column.getColumnNumber();
        }

        @Override
        public String getName() {
            return column.getName();
        }

        public TabularDataColumn getColumn() {
            return column;
        }

    }

    public class DiscreteDataColumn implements DiscreteColumn {

        private final TabularDataColumn column;
        private final Map<String, Integer> values;
        private List<String> categories;

        private DiscreteDataColumn(TabularDataColumn column) {
            if (column == null) {
                throw new IllegalArgumentException("Column cannot be null.");
            }
            this.column = column;
            this.values = new TreeMap<>();
        }

        @Override
        public String toString() {
            return "DiscreteDataColumn{" + "column=" + column + ", values=" + values + ", categories=" + categories + '}';
        }

        @Override
        public boolean isDiscrete() {
            return column.isDiscrete();
        }

        @Override
        public void setValue(String value) {
            this.values.put(value, null);
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
        public List<String> getCategories() {
            return (categories == null) ? Collections.EMPTY_LIST : categories;
        }

        @Override
        public Integer getEncodeValue(String value) {
            return values.get(value);
        }

        @Override
        public int getColumnNumber() {
            return column.getColumnNumber();
        }

        @Override
        public String getName() {
            return column.getName();
        }

        public TabularDataColumn getColumn() {
            return column;
        }

    }

    public class MixedTabularDataset implements TabularData {

        private final int numOfRows;
        private final MixedDataColumn[] columns;

        private final double[][] continuousData;
        private final int[][] discreteData;

        private MixedTabularDataset(int numOfRows, MixedDataColumn[] columns, double[][] continuousData, int[][] discreteData) {
            this.numOfRows = numOfRows;
            this.columns = columns;
            this.continuousData = continuousData;
            this.discreteData = discreteData;
        }

        public int getNumOfRows() {
            return numOfRows;
        }

        public MixedDataColumn[] getColumns() {
            return columns;
        }

        public double[][] getContinuousData() {
            return continuousData;
        }

        public int[][] getDiscreteData() {
            return discreteData;
        }

    }

    public class VerticalDiscreteTabularDataset implements TabularData {

        private final DiscreteDataColumn[] columns;
        private final int[][] data;

        private VerticalDiscreteTabularDataset(DiscreteDataColumn[] columns, int[][] data) {
            this.columns = columns;
            this.data = data;
        }

        public DiscreteDataColumn[] getColumns() {
            return columns;
        }

        public int[][] getData() {
            return data;
        }

    }

    public class ContinuousTabularDataset implements TabularData {

        private final TabularDataColumn[] columns;
        private final double[][] data;

        private ContinuousTabularDataset(TabularDataColumn[] columns, double[][] data) {
            this.columns = columns;
            this.data = data;
        }

        public TabularDataColumn[] getColumns() {
            return columns;
        }

        public double[][] getData() {
            return data;
        }

    }

}
