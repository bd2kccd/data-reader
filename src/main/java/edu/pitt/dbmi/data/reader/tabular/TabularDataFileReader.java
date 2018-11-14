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
import edu.pitt.dbmi.data.reader.tabular.TabularColumnFileReader.DataColumn;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public TabularDataFileReader(Path dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    public TabularData readInData(DataColumn[] dataColumns) throws IOException {
        boolean isDiscrete = false;
        boolean isContinuous = false;
        for (DataColumn dataColumn : dataColumns) {
            if (dataColumn.isContinuous()) {
                isContinuous = true;
            } else {
                isDiscrete = true;
            }

            if (isDiscrete && isContinuous) {
                break;
            }
        }

        if (isDiscrete && isContinuous) {
            return readInMixedData(dataColumns);
        } else if (isContinuous) {
            return readInContinuousData(dataColumns);
        } else {
            return readInDiscreteData(dataColumns);
        }
    }

    public TabularData readInContinuousData(DataColumn[] dataColumns) throws IOException {
        int numOfCols = dataColumns.length;
        int numOfRows = getNumberOfRows();
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

            int dataColsIndex = 0;

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
                            if (dataColsIndex < numOfCols) {
                                DataColumn dataColumn = dataColumns[dataColsIndex];
                                if (dataColumn.getColumnNumber() == colNum) {
                                    dataColsIndex++;

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
                                if (dataColsIndex < numOfCols) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColsIndex, numOfCols);
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
                        dataColsIndex = 0;
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
                                colNum++;

                                // ensure we don't go out of bound
                                if (dataColsIndex < numOfCols) {
                                    DataColumn dataColumn = dataColumns[dataColsIndex];
                                    if (dataColumn.getColumnNumber() == colNum) {
                                        dataColsIndex++;

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

                    prevChar = currChar;
                }
            }

            if (!skipHeader && hasSeenNonblankChar && !skip) {
                colNum++;

                // ensure we don't go out of bound
                if (dataColsIndex < numOfCols) {
                    DataColumn dataColumn = dataColumns[dataColsIndex];
                    if (dataColumn.getColumnNumber() == colNum) {
                        dataColsIndex++;

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
                    if (dataColsIndex < numOfCols) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColsIndex, numOfCols);
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

        return new ContinuousTabularDataset(dataColumns, data);
    }

    public TabularData readInDiscreteData(DataColumn[] dataColumns) throws IOException {
        DiscreteDataColumn[] discreteDataColumns = readInDiscreteCategorizes(dataColumns);
        int[][] data = readInDiscreteData(discreteDataColumns);

        return new VerticalDiscreteTabularDataset(discreteDataColumns, data);
    }

    public TabularData readInMixedData(DataColumn[] dataColumns) throws IOException {
        MixedDataColumn[] mixedDataColumns = Arrays.stream(dataColumns)
                .map(MixedDataColumn::new)
                .toArray(MixedDataColumn[]::new);
        readInDiscreteCategorizes(mixedDataColumns);

        return null;
    }

    protected int[][] readInDiscreteData(DiscreteDataColumn[] discreteDataColumns) throws IOException {
        int numOfCols = discreteDataColumns.length;
        int numOfRows = getNumberOfRows();
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

            int dataColumnIndex = 0;

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
                            if (dataColumnIndex < numOfCols) {
                                DiscreteDataColumn varInfo = discreteDataColumns[dataColumnIndex];
                                if (varInfo.getDataColumn().getColumnNumber() == colNum) {
                                    dataColumnIndex++;

                                    String value = dataBuilder.toString().trim();
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        data[col++][row] = varInfo.getEncodeValue(value);
                                    } else {
                                        data[col++][row] = DISCRETE_MISSING_VALUE;
                                    }
                                }

                                // ensure we have enough data
                                if (dataColumnIndex < numOfCols) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColumnIndex, numOfCols);
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
                        dataColumnIndex = 0;
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
                                colNum++;

                                // ensure we don't go out of bound
                                if (dataColumnIndex < numOfCols) {
                                    DiscreteDataColumn varInfo = discreteDataColumns[dataColumnIndex];
                                    if (varInfo.getDataColumn().getColumnNumber() == colNum) {
                                        dataColumnIndex++;

                                        String value = dataBuilder.toString().trim();
                                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                            data[col++][row] = varInfo.getEncodeValue(value);
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

                    prevChar = currChar;
                }
            }

            if (!skipHeader && hasSeenNonblankChar && !skip) {
                colNum++;

                // ensure we don't go out of bound
                if (dataColumnIndex < numOfCols) {
                    DiscreteDataColumn varInfo = discreteDataColumns[dataColumnIndex];
                    if (varInfo.getDataColumn().getColumnNumber() == colNum) {
                        dataColumnIndex++;

                        String value = dataBuilder.toString().trim();
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            data[col++][row] = varInfo.getEncodeValue(value);
                        } else {
                            data[col++][row] = DISCRETE_MISSING_VALUE;
                        }
                    }

                    // ensure we have enough data
                    if (dataColumnIndex < numOfCols) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColumnIndex, numOfCols);
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

    protected void readInDiscreteCategorizes(DiscreteColumn[] dataColumns) throws IOException {
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

            int dataColumnIndex = 0;

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
                            if (dataColumnIndex < numOfCols) {
                                DiscreteColumn column = dataColumns[dataColumnIndex];
                                if (column.getColumnNumber() == colNum) {
                                    String value = dataBuilder.toString().trim();
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        column.setValue(value);
                                    }

                                    dataColumnIndex++;
                                }

                                // ensure we have enough data
                                if (dataColumnIndex < numOfCols) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColumnIndex, numOfCols);
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
                        dataColumnIndex = 0;
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
                                colNum++;

                                // ensure we don't go out of bound
                                if (dataColumnIndex < numOfCols) {
                                    DiscreteColumn column = dataColumns[dataColumnIndex];
                                    if (column.getColumnNumber() == colNum) {
                                        String value = dataBuilder.toString().trim();
                                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                            column.setValue(value);
                                        }

                                        dataColumnIndex++;
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

                    prevChar = currChar;
                }
            }

            if (!skipHeader && hasSeenNonblankChar && !skip) {
                colNum++;

                // ensure we don't go out of bound
                if (dataColumnIndex < numOfCols) {
                    DiscreteColumn column = dataColumns[dataColumnIndex];
                    if (column.getColumnNumber() == colNum) {
                        String value = dataBuilder.toString().trim();
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            column.setValue(value);
                        }

                        dataColumnIndex++;
                    }

                    // ensure we have enough data
                    if (dataColumnIndex < numOfCols) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColumnIndex, numOfCols);
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
        for (DiscreteColumn dataColumn : dataColumns) {
            dataColumn.recategorize();
        }
    }

    protected DiscreteDataColumn[] readInDiscreteCategorizes(DataColumn[] dataColumns) throws IOException {
        // convert data columns to discrete columns
        DiscreteDataColumn[] discreteDataColumns = Arrays.stream(dataColumns)
                .map(DiscreteDataColumn::new)
                .toArray(DiscreteDataColumn[]::new);

        int numOfVars = discreteDataColumns.length;
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

            int dataColumnIndex = 0;

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
                            if (dataColumnIndex < numOfVars) {
                                DiscreteDataColumn dataColumn = discreteDataColumns[dataColumnIndex];
                                if (dataColumn.getDataColumn().getColumnNumber() == colNum) {
                                    String value = dataBuilder.toString().trim();
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        dataColumn.setValue(value);
                                    }

                                    dataColumnIndex++;
                                }

                                // ensure we have enough data
                                if (dataColumnIndex < numOfVars) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColumnIndex, numOfVars);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }
                            } else {
                                String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfVars + 1, numOfVars);
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
                        dataColumnIndex = 0;
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
                                colNum++;

                                // ensure we don't go out of bound
                                if (dataColumnIndex < numOfVars) {
                                    DiscreteDataColumn dataColumn = discreteDataColumns[dataColumnIndex];
                                    if (dataColumn.getDataColumn().getColumnNumber() == colNum) {
                                        String value = dataBuilder.toString().trim();
                                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                            dataColumn.setValue(value);
                                        }

                                        dataColumnIndex++;
                                    }
                                } else {
                                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfVars + 1, numOfVars);
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

                    prevChar = currChar;
                }
            }

            if (!skipHeader && hasSeenNonblankChar && !skip) {
                colNum++;

                // ensure we don't go out of bound
                if (dataColumnIndex < numOfVars) {
                    DiscreteDataColumn dataColumn = discreteDataColumns[dataColumnIndex];
                    if (dataColumn.getDataColumn().getColumnNumber() == colNum) {
                        String value = dataBuilder.toString().trim();
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            dataColumn.setValue(value);
                        }

                        dataColumnIndex++;
                    }

                    // ensure we have enough data
                    if (dataColumnIndex < numOfVars) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, dataColumnIndex, numOfVars);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                } else {
                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfVars + 1, numOfVars);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        // recategorize values
        for (DiscreteDataColumn dataColumn : discreteDataColumns) {
            dataColumn.recategorize();
        }

        return discreteDataColumns;
    }

    public interface DiscreteColumn {

        public boolean isDiscrete();

        public void setValue(String value);

        public void recategorize();

        public List<String> getCategories();

        public Integer getEncodeValue(String value);

        public int getColumnNumber();

    }

    public class DiscreteDataColumn implements DiscreteColumn {

        protected final DataColumn dataColumn;
        protected final Map<String, Integer> values;
        protected List<String> categories;

        private DiscreteDataColumn(DataColumn dataColumn) {
            if (dataColumn == null) {
                throw new IllegalArgumentException("Data column cannot be null.");
            }
            this.dataColumn = dataColumn;
            this.values = new TreeMap<>();
        }

        @Override
        public String toString() {
            return "DiscreteDataColumn{" + "dataColumn=" + dataColumn + ", values=" + values + ", categories=" + categories + '}';
        }

        @Override
        public boolean isDiscrete() {
            return !dataColumn.isContinuous();
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
            return dataColumn.getColumnNumber();
        }

        public DataColumn getDataColumn() {
            return dataColumn;
        }

    }

    public class MixedDataColumn implements DiscreteColumn {

        private List<String> categories;

        private final DataColumn dataColumn;
        private final Map<String, Integer> values;

        public MixedDataColumn(DataColumn dataColumn) {
            if (dataColumn == null) {
                throw new IllegalArgumentException("Data column cannot be null.");
            }
            this.dataColumn = dataColumn;
            this.values = dataColumn.isContinuous() ? null : new TreeMap<>();
        }

        @Override
        public boolean isDiscrete() {
            return !dataColumn.isContinuous();
        }

        @Override
        public void setValue(String value) {
            if (values != null) {
                values.put(value, null);
            }
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
            return dataColumn.getColumnNumber();
        }

        public DataColumn getDataColumn() {
            return dataColumn;
        }

    }

    public class MixedTabularDataset implements TabularData {

        private final int numOfRows;
        private final MixedDataColumn[] mixedDataColumns;

        private final double[][] continuousData;
        private final int[][] discreteData;

        public MixedTabularDataset(int numOfRows, MixedDataColumn[] mixedDataColumns, double[][] continuousData, int[][] discreteData) {
            this.numOfRows = numOfRows;
            this.mixedDataColumns = mixedDataColumns;
            this.continuousData = continuousData;
            this.discreteData = discreteData;
        }

        public int getNumOfRows() {
            return numOfRows;
        }

        public MixedDataColumn[] getMixedDataColumns() {
            return mixedDataColumns;
        }

        public double[][] getContinuousData() {
            return continuousData;
        }

        public int[][] getDiscreteData() {
            return discreteData;
        }

    }

    public class VerticalDiscreteTabularDataset implements TabularData {

        private final DiscreteDataColumn[] discreteDataColumns;
        private final int[][] data;

        private VerticalDiscreteTabularDataset(DiscreteDataColumn[] discreteDataColumns, int[][] data) {
            this.discreteDataColumns = discreteDataColumns;
            this.data = data;
        }

        public DiscreteDataColumn[] getDiscreteDataColumns() {
            return discreteDataColumns;
        }

        public int[][] getData() {
            return data;
        }

    }

    public class ContinuousTabularDataset implements TabularData {

        private final DataColumn[] dataColumns;
        private final double[][] data;

        private ContinuousTabularDataset(DataColumn[] dataColumns, double[][] data) {
            this.dataColumns = dataColumns;
            this.data = data;
        }

        public DataColumn[] getDataColumns() {
            return dataColumns;
        }

        public double[][] getData() {
            return data;
        }

    }

}
