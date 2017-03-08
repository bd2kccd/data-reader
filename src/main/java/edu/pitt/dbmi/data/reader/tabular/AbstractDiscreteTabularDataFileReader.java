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

import edu.pitt.dbmi.data.Delimiter;
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
public abstract class AbstractDiscreteTabularDataFileReader extends AbstractTabularDataFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDiscreteTabularDataFileReader.class);

    private static final int DISCRETE_MISSING_VALUE = -99;

    public AbstractDiscreteTabularDataFileReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    protected int[][] extractAndEncodeData(DiscreteVarInfo[] varInfos, int[] excludedColumns) throws IOException {
        int numOfColumns = varInfos.length;
        int numOfRows = (hasHeader) ? getNumberOfLines() - 1 : getNumberOfLines();
        int[][] data = new int[numOfColumns][numOfRows];

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
            int lineNum = 1;
            int row = 0;  // array row number
            int col = 0;  // array column number
            int numOfData = 0; // number of data read in per column
            int numOfExCols = excludedColumns.length;
            int excludedIndex = 0;
            int varInfoIndex = 0;
            boolean skipHeader = hasHeader;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean finished = false;
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                // skip header, if any
                if (skipHeader) {
                    while (buffer.hasRemaining() && skipHeader) {
                        byte currChar = buffer.get();
                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                prevNonBlankChar = SPACE_CHAR;
                                skipHeader = false;
                            }

                            lineNum++;
                            if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                                lineNum--;
                            }
                        } else if (!skipLine) {
                            if (currChar > SPACE_CHAR) {
                                prevNonBlankChar = currChar;
                            }

                            if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                                if (currChar == prefix[index]) {
                                    index++;
                                    if (index == prefix.length) {
                                        index = 0;
                                        skipLine = true;
                                        prevNonBlankChar = SPACE_CHAR;
                                    }
                                } else {
                                    index = 0;
                                    skipLine = true;
                                }
                            }
                        }

                        prevChar = currChar;
                    }
                }

                while (buffer.hasRemaining()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                excludedIndex++;
                            } else {
                                numOfData++;

                                // ensure we don't go out of bound
                                if (numOfData > numOfColumns) {
                                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                } else if (numOfData < numOfColumns) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                } else {
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        data[col++][row] = varInfos[varInfoIndex++].getEncodeValue(value);
                                    } else {
                                        data[col++][row] = DISCRETE_MISSING_VALUE;
                                    }
                                }
                            }

                            row++;
                        }

                        col = 0;
                        colNum = 0;
                        numOfData = 0;
                        excludedIndex = 0;
                        varInfoIndex = 0;
                        reqCheck = true;
                        prevNonBlankChar = SPACE_CHAR;
                        skipLine = false;

                        lineNum++;
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            lineNum--;
                        }
                    } else if (!skipLine) {
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                            if (currChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    prevNonBlankChar = SPACE_CHAR;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                reqCheck = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        } else {
                            boolean isDelimiter;
                            switch (delimiter) {
                                case WHITESPACE:
                                    isDelimiter = (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR);
                                    break;
                                default:
                                    isDelimiter = (currChar == delimChar);
                            }

                            if (isDelimiter) {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    numOfData++;

                                    // ensure we don't go out of bound
                                    if (numOfData > numOfColumns) {
                                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
                                    } else {
                                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                            data[col++][row] = varInfos[varInfoIndex++].getEncodeValue(value);
                                        } else {
                                            data[col++][row] = DISCRETE_MISSING_VALUE;
                                        }
                                    }
                                }
                            } else {
                                dataBuilder.append((char) currChar);
                            }
                        }
                    }

                    prevChar = currChar;
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

                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                    excludedIndex++;
                } else {
                    numOfData++;

                    // ensure we don't go out of bound
                    if (numOfData > numOfColumns) {
                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    } else if (numOfData < numOfColumns) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    } else {
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            data[col++][row] = varInfos[varInfoIndex++].getEncodeValue(value);
                        } else {
                            data[col++][row] = DISCRETE_MISSING_VALUE;
                        }
                    }
                }
            }
        }

        return data;
    }

    protected DiscreteVarInfo[] extractVariableData(DiscreteVarInfo[] varInfos, int[] excludedColumns) throws IOException {
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
            int lineNum = 1;
            int numOfData = 0; // number of data read in per column
            int numOfExCols = excludedColumns.length;
            int numOfColumns = varInfos.length;
            int excludedIndex = 0;
            int varInfoIndex = 0;
            boolean skipHeader = hasHeader;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                // skip header, if any
                if (skipHeader) {
                    while (buffer.hasRemaining() && skipHeader) {
                        byte currChar = buffer.get();
                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                prevNonBlankChar = SPACE_CHAR;
                                skipHeader = false;
                            }

                            lineNum++;
                            if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                                lineNum--;
                            }
                        } else if (!skipLine) {
                            if (currChar > SPACE_CHAR) {
                                prevNonBlankChar = currChar;
                            }

                            if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                                if (currChar == prefix[index]) {
                                    index++;
                                    if (index == prefix.length) {
                                        index = 0;
                                        skipLine = true;
                                        prevNonBlankChar = SPACE_CHAR;
                                    }
                                } else {
                                    index = 0;
                                    skipLine = true;
                                }
                            }
                        }

                        prevChar = currChar;
                    }
                }

                // read in data
                while (buffer.hasRemaining()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                                numOfData++;

                                // ensure we don't go out of bound
                                if (numOfData > numOfColumns) {
                                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                } else if (numOfData < numOfColumns) {
                                    String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                } else {
                                    if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                        varInfos[varInfoIndex++].setValue(value);
                                    }
                                }
                            }
                        }

                        skipLine = false;
                        colNum = 0;
                        numOfData = 0;
                        excludedIndex = 0;
                        varInfoIndex = 0;
                        reqCheck = true;
                        prevNonBlankChar = SPACE_CHAR;

                        lineNum++;
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            lineNum--;
                        }
                    } else if (!skipLine) {
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                            if (currChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    prevNonBlankChar = SPACE_CHAR;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                reqCheck = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        } else {
                            boolean isDelimiter;
                            switch (delimiter) {
                                case WHITESPACE:
                                    isDelimiter = (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR);
                                    break;
                                default:
                                    isDelimiter = (currChar == delimChar);
                            }

                            if (isDelimiter) {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    numOfData++;

                                    // ensure we don't go out of bound
                                    if (numOfData > numOfColumns) {
                                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                                        LOGGER.error(errMsg);
                                        throw new DataReaderException(errMsg);
                                    } else {
                                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                                            varInfos[varInfoIndex++].setValue(value);
                                        }
                                    }
                                }
                            } else {
                                dataBuilder.append((char) currChar);
                            }
                        }
                    }

                    prevChar = currChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);

            // case when no newline at end of file
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    numOfData++;

                    // ensure we don't go out of bound
                    if (numOfData > numOfColumns) {
                        String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    } else if (numOfData < numOfColumns) {
                        String errMsg = String.format("Insufficient data on line %d.  Extracted %d value(s) but expected %d.", lineNum, numOfData, numOfColumns);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    } else {
                        if (value.length() > 0 && !value.equals(missingValueMarker)) {
                            varInfos[varInfoIndex++].setValue(value);
                        }
                    }
                }
            }
        }

        return varInfos;
    }

    protected DiscreteVarInfo[] extractVariables(int[] excludedColumns) throws IOException {
        int numOfCols = getNumberOfColumns();
        int numOfExCols = excludedColumns.length;
        int numOfVars = numOfCols - numOfExCols;
        DiscreteVarInfo[] varInfos = new DiscreteVarInfo[numOfVars];

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
            int lineNum = 1;
            int excludedIndex = 0;
            int varInfoIndex = 0;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean taskFinished = false;
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !taskFinished) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        skipLine = false;
                        if (prevNonBlankChar > SPACE_CHAR) {
                            taskFinished = true;
                        } else {
                            lineNum++;
                            if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                                lineNum--;
                            }
                        }
                    } else if (!skipLine) {
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        if (reqCheck && prevNonBlankChar > SPACE_CHAR) {
                            if (currChar == prefix[index]) {
                                index++;

                                // all the comment chars are matched
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    colNum = 0;
                                    prevNonBlankChar = SPACE_CHAR;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                reqCheck = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else if (hasQuoteChar) {
                            dataBuilder.append((char) currChar);
                        } else {
                            boolean isDelimiter;
                            switch (delimiter) {
                                case WHITESPACE:
                                    isDelimiter = (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR);
                                    break;
                                default:
                                    isDelimiter = (currChar == delimChar);
                            }

                            if (isDelimiter) {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols > 0 && (excludedIndex < numOfExCols && colNum == excludedColumns[excludedIndex])) {
                                    excludedIndex++;
                                } else {
                                    if (value.length() > 0) {
                                        varInfos[varInfoIndex++] = new DiscreteVarInfo(value);
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

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize && !taskFinished);

            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    switch (delimiter) {
                        case WHITESPACE:
                            if (value.length() > 0) {
                                varInfos[varInfoIndex++] = new DiscreteVarInfo(value);
                            }
                            break;
                        default:
                            if (value.length() > 0) {
                                varInfos[varInfoIndex++] = new DiscreteVarInfo(value);
                            } else {
                                String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }
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
    protected DiscreteVarInfo[] generateDiscreteVariables(int[] excludedColumns) throws IOException {
        int numOfCols = getNumberOfColumns();
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
