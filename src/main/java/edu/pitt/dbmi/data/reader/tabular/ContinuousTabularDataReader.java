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

import edu.pitt.dbmi.data.ContinuousTabularDataset;
import edu.pitt.dbmi.data.Dataset;
import edu.pitt.dbmi.data.reader.DataReaderException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Feb 8, 2017 10:01:35 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ContinuousTabularDataReader extends AbstractContinuousTabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousTabularDataReader.class);

    public ContinuousTabularDataReader(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    protected Dataset readInDataFromFile(int[] excludedColumns) throws IOException {
        List<String> variables = extractVariablesFromData(excludedColumns);
        double[][] data = extractDataFromFile(variables, excludedColumns);

        return new ContinuousTabularDataset(variables, data);
    }

    protected double[][] extractDataFromFile(List<String> variables, int[] excludedColumns) throws IOException {
        return commentMarker.isEmpty()
                ? extractData(variables, excludedColumns)
                : extractData(variables, excludedColumns, commentMarker);
    }

    protected double[][] extractData(List<String> variables, int[] excludedColumns, String comment) throws IOException {
        int numCols = variables.size();
        int numRows = (hasHeader) ? getNumOfRows() - 1 : getNumOfRows();

        double[][] data = new double[numRows][numCols];
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = comment.getBytes();
            int index = 0;
            int numOfExCols = excludedColumns.length;
            int excludedIndex = 0;
            int colNum = 0;
            int row = 0;  // data row number
            int col = 0;  // data column number
            int lineNumber = 1; // actual row number
            boolean isHeader = false;
            boolean skipLine = false;
            boolean checkRequired = true;  // require check for comment
            boolean hasQuoteChar = false;
            boolean endOfLine = false;
            boolean skipHeader = hasHeader;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipHeader) {
                    while (buffer.hasRemaining() && !endOfLine) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                lineNumber++;
                                if (isHeader) {
                                    endOfLine = true;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            if (currentChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    skipLine = true;
                                    index = 0;
                                }
                            } else {
                                skipLine = true;
                                index = 0;
                                isHeader = true;
                            }
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            if (index > 0) {
                                endOfLine = true;
                            }
                            index = 0;
                            lineNumber++;
                        }
                    }
                    skipHeader = false;
                }

                while (buffer.hasRemaining()) {
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
                                    // ensure we don't go out of bound
                                    if (col >= numCols) {
                                        String errMsg = String.format("Excess data on line %d.  Expect %d.", lineNumber, numCols);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
                                    }

                                    if (value.length() > 0) {
                                        try {
                                            data[row][col++] = Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Invalid number %s on line %d column %d.", value, lineNumber, colNum);
                                            LOGGER.error(errMsg, exception);
                                            throw new DataReaderException(errMsg);
                                        }
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
                                // ensure number of data don't exceed
                                if (col >= numCols) {
                                    String errMsg = String.format("Excess data on line %d.  Expect %d.", lineNumber, numCols);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }

                                if (value.length() > 0) {
                                    try {
                                        data[row][col++] = Double.parseDouble(value);
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Invalid number %s on line %d column %d.", value, lineNumber, colNum);
                                        LOGGER.error(errMsg, exception);
                                        throw new DataReaderException(errMsg);
                                    }
                                } else {
                                    String errMsg = String.format("Missing data one line %d column %d.", lineNumber, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }
                            }

                            // ensure there sufficient number of data
                            if (col < numCols) {
                                String errMsg = String.format("Insufficient data on line %d.  Expect %d but found %d.", lineNumber, numCols, col);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }

                            row++;
                        }

                        colNum = 0;
                        col = 0;
                        excludedIndex = 0;
                        lineNumber++;
                        checkRequired = true;
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
                    // ensure number of data don't exceed
                    if (col >= numCols) {
                        String errMsg = String.format("Excess data on line %d.  Expect %d.", lineNumber, numCols);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }

                    if (value.length() > 0) {
                        try {
                            data[row][col++] = Double.parseDouble(value);
                        } catch (NumberFormatException exception) {
                            String errMsg = String.format("Invalid number %s on line %d column %d.", value, lineNumber, colNum);
                            LOGGER.error(errMsg, exception);
                            throw new DataReaderException(errMsg);
                        }
                    } else {
                        String errMsg = String.format("Missing data one line %d column %d.", lineNumber, colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }

                // ensure there sufficient number of data
                if (col < numCols) {
                    String errMsg = String.format("Insufficient data on line %d.  Expect %d but found %d.", lineNumber, numCols, col);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        return data;
    }

    protected double[][] extractData(List<String> variables, int[] excludedColumns) throws IOException {
        int numCols = variables.size();
        int numRows = (hasHeader) ? getNumOfRows() - 1 : getNumOfRows();

        double[][] data = new double[numRows][numCols];
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
            int row = 0;  // data row number
            int col = 0;  // data column number
            int lineNumber = 1; // actual row number
            int colNum = 0;  // actual columm number
            int excludedIndex = 0;
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
                                    // ensure we don't go out of bound
                                    if (col >= numCols) {
                                        String errMsg = String.format("Excess data on line %d.  Expect %d.", lineNumber, numCols);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
                                    }

                                    if (value.length() > 0) {
                                        try {
                                            data[row][col++] = Double.parseDouble(value);
                                        } catch (NumberFormatException exception) {
                                            String errMsg = String.format("Invalid number %s on line %d column %d.", value, lineNumber, colNum);
                                            LOGGER.error(errMsg, exception);
                                            throw new DataReaderException(errMsg);
                                        }
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
                                // ensure number of data don't exceed
                                if (col >= numCols) {
                                    String errMsg = String.format("Excess data on line %d.  Expect %d.", lineNumber, numCols);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }

                                if (value.length() > 0) {
                                    try {
                                        data[row][col++] = Double.parseDouble(value);
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Invalid number %s on line %d column %d.", value, lineNumber, colNum);
                                        LOGGER.error(errMsg, exception);
                                        throw new DataReaderException(errMsg);
                                    }
                                } else {
                                    String errMsg = String.format("Missing data one line %d column %d.", lineNumber, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }
                            }

                            // ensure there sufficient number of data
                            if (col < numCols) {
                                String errMsg = String.format("Insufficient data on line %d.  Expect %d but found %d.", lineNumber, numCols, col);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }

                            row++;
                        }

                        colNum = 0;
                        col = 0;
                        excludedIndex = 0;
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
                    // ensure number of data don't exceed
                    if (col >= numCols) {
                        String errMsg = String.format("Excess data on line %d.  Expect %d.", lineNumber, numCols);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }

                    if (value.length() > 0) {
                        try {
                            data[row][col++] = Double.parseDouble(value);
                        } catch (NumberFormatException exception) {
                            String errMsg = String.format("Invalid number %s on line %d column %d.", value, lineNumber, colNum);
                            LOGGER.error(errMsg, exception);
                            throw new DataReaderException(errMsg);
                        }
                    } else {
                        String errMsg = String.format("Missing data one line %d column %d.", lineNumber, colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }

                // ensure there sufficient number of data
                if (col < numCols) {
                    String errMsg = String.format("Insufficient data on line %d.  Expect %d but found %d.", lineNumber, numCols, col);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        return data;
    }

}
