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

import edu.pitt.dbmi.data.Dataset;
import edu.pitt.dbmi.data.Delimiter;
import edu.pitt.dbmi.data.MixedTabularDataset;
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
 * Apr 4, 2017 5:24:07 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedTabularDataFileReader extends AbstractTabularDataFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MixedTabularDataFileReader.class);

    private static final double CONTINUOUS_MISSING_VALUE = Double.NaN;
    private static final int DISCRETE_MISSING_VALUE = -99;

    private final int numberOfDiscreteCategories;

    public MixedTabularDataFileReader(int numberOfDiscreteCategories, File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
        this.numberOfDiscreteCategories = numberOfDiscreteCategories;
    }

    @Override
    protected Dataset readInDataFromFile(int[] excludedColumns) throws IOException {
        MixedVarInfo[] mixedVarInfos = hasHeader ? extractMixedVariables(excludedColumns) : generateMixedVariables(excludedColumns);
        mixedVarInfos = analysMixedVariables(mixedVarInfos, excludedColumns);

        return extractMixedData(mixedVarInfos, excludedColumns);
    }

    private MixedTabularDataset extractMixedData(MixedVarInfo[] mixedVarInfos, int[] excludedColumns) throws IOException {
        int numOfColumns = mixedVarInfos.length;
        int numOfRows = (hasHeader) ? getNumberOfLines() - 1 : getNumberOfLines();

        double[][] continuousData = new double[numOfColumns][];
        int[][] discreteData = new int[numOfColumns][];

        int mixedVarInfoIndex = 0;
        for (MixedVarInfo mixedVarInfo : mixedVarInfos) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }

            if (mixedVarInfo.isContinuous()) {
                mixedVarInfo.clearValues();
                continuousData[mixedVarInfoIndex++] = new double[numOfRows];
            } else {
                mixedVarInfo.recategorize();
                discreteData[mixedVarInfoIndex++] = new int[numOfRows];
            }
        }

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
            boolean skipHeader = hasHeader;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            mixedVarInfoIndex = 0;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                // skip header, if any
                if (skipHeader) {
                    while (buffer.hasRemaining() && skipHeader && !Thread.currentThread().isInterrupted()) {
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
                while (buffer.hasRemaining() && !Thread.currentThread().isInterrupted()) {
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
                                    MixedVarInfo mixedVarInfo = mixedVarInfos[col];
                                    if (mixedVarInfo.isContinuous()) {
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
                                    } else {
                                        if (value.length() == 0 || value.equals(missingValueMarker)) {
                                            discreteData[col][row] = DISCRETE_MISSING_VALUE;
                                        } else {
                                            discreteData[col][row] = mixedVarInfos[mixedVarInfoIndex].getEncodeValue(value);
                                        }
                                    }
                                    col++;
                                    mixedVarInfoIndex++;
                                }
                            }

                            row++;
                        }

                        col = 0;
                        colNum = 0;
                        numOfData = 0;
                        excludedIndex = 0;
                        mixedVarInfoIndex = 0;
                        reqCheck = prefix.length > 0;
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
                                        MixedVarInfo mixedVarInfo = mixedVarInfos[col];
                                        if (mixedVarInfo.isContinuous()) {
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
                                        } else {
                                            if (value.length() == 0 || value.equals(missingValueMarker)) {
                                                discreteData[col][row] = DISCRETE_MISSING_VALUE;
                                            } else {
                                                discreteData[col][row] = mixedVarInfos[mixedVarInfoIndex].getEncodeValue(value);
                                            }
                                        }
                                        col++;
                                        mixedVarInfoIndex++;
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
            } while (position < fileSize && !Thread.currentThread().isInterrupted());

            // case when no newline char at the end of the file
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
                        MixedVarInfo mixedVarInfo = mixedVarInfos[col];
                        if (mixedVarInfo.isContinuous()) {
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
                        } else {
                            if (value.length() == 0 || value.equals(missingValueMarker)) {
                                discreteData[col][row] = DISCRETE_MISSING_VALUE;
                            } else {
                                discreteData[col][row] = mixedVarInfos[mixedVarInfoIndex].getEncodeValue(value);
                            }
                        }
                        col++;
                        mixedVarInfoIndex++;
                    }
                }
            }
        }

        return new MixedTabularDataset(numOfRows, mixedVarInfos, continuousData, discreteData);
    }

    private MixedVarInfo[] analysMixedVariables(MixedVarInfo[] mixedVarInfos, int[] excludedColumns) throws IOException {
        int numOfColumns = mixedVarInfos.length;
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
                    while (buffer.hasRemaining() && skipHeader && !Thread.currentThread().isInterrupted()) {
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
                while (buffer.hasRemaining() && !Thread.currentThread().isInterrupted()) {
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
                                        MixedVarInfo mixedVarInfo = mixedVarInfos[varInfoIndex];
                                        if (!mixedVarInfo.isContinuous()) {
                                            mixedVarInfo.setValue(value);
                                            if (mixedVarInfo.getNumberOfValues() > numberOfDiscreteCategories) {
                                                mixedVarInfo.setContinuous(true);
                                            }
                                        }
                                    }
                                    varInfoIndex++;
                                }
                            }
                        }

                        skipLine = false;
                        colNum = 0;
                        numOfData = 0;
                        excludedIndex = 0;
                        varInfoIndex = 0;
                        reqCheck = prefix.length > 0;
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
                                            MixedVarInfo mixedVarInfo = mixedVarInfos[varInfoIndex];
                                            if (!mixedVarInfo.isContinuous()) {
                                                mixedVarInfo.setValue(value);
                                                if (mixedVarInfo.getNumberOfValues() > numberOfDiscreteCategories) {
                                                    mixedVarInfo.setContinuous(true);
                                                }
                                            }
                                        }
                                        varInfoIndex++;
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
            } while (position < fileSize && !Thread.currentThread().isInterrupted());

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
                            MixedVarInfo mixedVarInfo = mixedVarInfos[varInfoIndex];
                            if (!mixedVarInfo.isContinuous()) {
                                mixedVarInfo.setValue(value);
                                if (mixedVarInfo.getNumberOfValues() > numberOfDiscreteCategories) {
                                    mixedVarInfo.setContinuous(true);
                                }
                            }
                        }
                        varInfoIndex++;
                    }
                }
            }
        }

        return mixedVarInfos;
    }

    private MixedVarInfo[] extractMixedVariables(int[] excludedColumns) throws IOException {
        int numOfCols = getNumberOfColumns();
        int numOfExCols = excludedColumns.length;
        int numOfVars = numOfCols - numOfExCols;
        MixedVarInfo[] mixedVars = new MixedVarInfo[numOfVars];

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
                while (buffer.hasRemaining() && !taskFinished && !Thread.currentThread().isInterrupted()) {
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
                                        mixedVars[varInfoIndex++] = new MixedVarInfo(value);
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
            } while (position < fileSize && !taskFinished && !Thread.currentThread().isInterrupted());

            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                    switch (delimiter) {
                        case WHITESPACE:
                            if (value.length() > 0) {
                                mixedVars[varInfoIndex++] = new MixedVarInfo(value);
                            }
                            break;
                        default:
                            if (value.length() > 0) {
                                mixedVars[varInfoIndex++] = new MixedVarInfo(value);
                            } else {
                                String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }
                    }
                }
            }
        }

        return mixedVars;
    }

    /**
     *
     * @param excludedColumns sorted array of column numbers
     * @return
     * @throws IOException
     */
    protected MixedVarInfo[] generateMixedVariables(int[] excludedColumns) throws IOException {
        int numOfCols = getNumberOfColumns();
        int numOfExCols = excludedColumns.length;
        int size = numOfCols - numOfExCols;
        int exColIndex = 0;
        int varInfoIndex = 0;
        MixedVarInfo[] varInfos = new MixedVarInfo[size];
        for (int colNum = 1; colNum <= numOfCols && !Thread.currentThread().isInterrupted(); colNum++) {
            if (numOfExCols > 0 && (exColIndex < numOfExCols && colNum == excludedColumns[exColIndex])) {
                exColIndex++;
            } else {
                varInfos[varInfoIndex++] = new MixedVarInfo(String.format("V%d", colNum));
            }
        }

        return varInfos;
    }

}
