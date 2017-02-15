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

    protected DiscreteVarInfo[] extractVariableDataFromFile(DiscreteVarInfo[] varInfos, int[] excludedColumns) throws IOException {
        return commentMarker.isEmpty()
                ? extractVariableData(varInfos, excludedColumns)
                : extractVariableData(varInfos, excludedColumns, commentMarker);
    }

    private DiscreteVarInfo[] extractVariableData(DiscreteVarInfo[] varInfos, int[] excludedColumns, String comment) throws IOException {
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = comment.getBytes();
            int index = 0;
            int numOfExCols = excludedColumns.length;
            int actualNumOfCols = varInfos.length;
            int excludedIndex = 0;
            int varInfoIndex = 0;
            int colNum = 0;
            int dataColNum = 0;
            int lineNumber = 1; // actual row number
            boolean isHeader = false;
            boolean skipLine = false;
            boolean checkRequired = true;  // require check for comment
            boolean hasQuoteChar = false;
            boolean endOfLine = false;
            boolean skipHeader = hasHeader;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipHeader) {
                    while (buffer.hasRemaining() && !endOfLine) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                if (isHeader) {
                                    endOfLine = true;
                                }

                                lineNumber++;
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
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
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
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
                            previousChar = currentChar;
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

                                    previousChar = currentChar;
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
                                    dataColNum++;

                                    // ensure we don't go out of bound
                                    if (dataColNum > actualNumOfCols) {
                                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
                                    }

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
                                dataColNum++;

                                // ensure the data is within bound
                                if (dataColNum > actualNumOfCols) {
                                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                } else if (dataColNum < actualNumOfCols) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
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
                        }

                        colNum = 0;
                        dataColNum = 0;
                        excludedIndex = 0;
                        varInfoIndex = 0;
                        checkRequired = true;

                        lineNumber++;
                        if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                            lineNumber--;
                        }
                    }

                    previousChar = currentChar;
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
                    dataColNum++;

                    // ensure the data is within bound
                    if (dataColNum > actualNumOfCols) {
                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    } else if (dataColNum < actualNumOfCols) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
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
            }
        }

        return varInfos;
    }

    private DiscreteVarInfo[] extractVariableData(DiscreteVarInfo[] varInfos, int[] excludedColumns) throws IOException {
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
            int actualNumOfCols = varInfos.length;
            int lineNumber = 1; // actual row number
            int colNum = 0;  // actual columm number
            int dataColNum = 0;
            int excludedIndex = 0;
            int varInfoIndex = 0;
            byte previousChar = -1;
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
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            skipLine = true;
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
                    }
                    skipHeader = false;
                }

                while (buffer.hasRemaining()) {
                    byte currentChar = buffer.get();

                    if (currentChar >= SPACE || currentChar == delimiter) {
                        // case where line starts with spaces
                        if (currentChar == SPACE && currentChar != delimiter && dataBuilder.length() == 0) {
                            previousChar = currentChar;
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
                                    dataColNum++;

                                    // ensure we don't go out of bound
                                    if (dataColNum > actualNumOfCols) {
                                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
                                    }

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
                                dataColNum++;

                                // ensure the data is within bound
                                if (dataColNum > actualNumOfCols) {
                                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                } else if (dataColNum < actualNumOfCols) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
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
                        }

                        colNum = 0;
                        dataColNum = 0;
                        excludedIndex = 0;
                        varInfoIndex = 0;

                        lineNumber++;
                        if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                            lineNumber--;
                        }
                    }

                    previousChar = currentChar;
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
                    dataColNum++;

                    // ensure the data is within bound
                    if (dataColNum > actualNumOfCols) {
                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    } else if (dataColNum < actualNumOfCols) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, dataColNum, actualNumOfCols);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
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
            }
        }

        return varInfos;
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
            byte previousChar = -1;
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
                            previousChar = currentChar;
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

                                    previousChar = currentChar;
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

                    previousChar = currentChar;
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
            int lineNumber = 1; // actual row number
            boolean hasQuoteChar = false;
            boolean endOfLine = false;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !endOfLine) {
                    byte currentChar = buffer.get();

                    if (currentChar >= SPACE || currentChar == delimiter) {
                        // case where line starts with spaces
                        if (currentChar == SPACE && currentChar != delimiter && dataBuilder.length() == 0) {
                            previousChar = currentChar;
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
                                        String errMsg = String.format("Missing variable name on line %d column %d.", lineNumber, colNum);
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

                        lineNumber++;
                        if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                            lineNumber--;
                        }
                    }

                    previousChar = currentChar;
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
                        String errMsg = String.format("Missing variable name on line %d column %d.", lineNumber, colNum);
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

}
