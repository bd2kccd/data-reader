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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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

        List<String> variables = Arrays.stream(dataColumns)
                .map(DataColumn::getName)
                .collect(Collectors.toList());

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

        return new ContinuousTabularDataset(variables, data);
    }

    public TabularData readInDiscreteData(DataColumn[] dataColumns) throws IOException {
        return null;
    }

    public TabularData readInMixedData(DataColumn[] dataColumns) throws IOException {
        return null;
    }

}
