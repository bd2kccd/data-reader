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
package edu.pitt.dbmi.data.reader.covariance;

import edu.pitt.dbmi.data.CovarianceDataset;
import edu.pitt.dbmi.data.Dataset;
import edu.pitt.dbmi.data.Delimiter;
import edu.pitt.dbmi.data.reader.AbstractDataFileReader;
import edu.pitt.dbmi.data.reader.DataReaderException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Feb 22, 2017 2:42:13 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class LowerCovarianceDataReader extends AbstractDataFileReader implements CovarianceDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowerCovarianceDataReader.class);

    public LowerCovarianceDataReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    public Dataset readInData() throws IOException {
        Dataset dataset;
        try {
            int numberOfCases = getNumberOfCases();
            List<String> variables = extractVariables();
            double[][] data = extractCovarianceData(variables.size());

            dataset = new CovarianceDataset(numberOfCases, variables, data);
        } catch (ClosedByInterruptException exception) {
            dataset = null;
            LOGGER.error("", exception);
        }

        return dataset;
    }

    private double[][] extractCovarianceData(int matrixSize) throws IOException {
        double[][] data = new double[matrixSize][matrixSize];

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNum = 1;
            int colNum = 0;
            int col = 0;
            int row = 0;
            boolean skipLine = false;
            boolean skipToData = true;
            int skipLineCount = 0;
            boolean reqCheck = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipToData) {
                    while (buffer.hasRemaining() && skipToData && !Thread.currentThread().isInterrupted()) {
                        byte currChar = buffer.get();

                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                skipLineCount++;
                                skipToData = !(skipLineCount == 2);
                                prevNonBlankChar = SPACE_CHAR;
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

                while (buffer.hasRemaining() && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (col > row) {
                                String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, col + 1, row + 1);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            } else if (col < row) {
                                String errMsg = String.format("Insufficent data on line %d.  Extracted %d value(s) but expected %d.", lineNum, col + 1, row + 1);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            } else {
                                if (value.length() > 0) {
                                    try {
                                        double covariance = Double.parseDouble(value);
                                        data[row][col] = covariance;
                                        data[col][row] = covariance;
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                        LOGGER.error(errMsg, exception);
                                        throw new DataReaderException(errMsg);
                                    }
                                } else {
                                    String errMsg = String.format("Missing data on line %d at column %d.", lineNum, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }
                            }

                            row++;
                        }

                        skipLine = false;
                        col = 0;
                        colNum = 0;
                        reqCheck = prefix.length > 0;

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

                                if (col > row) {
                                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, col + 1, row + 1);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }

                                if (value.length() > 0) {
                                    try {
                                        double covariance = Double.parseDouble(value);
                                        data[row][col] = covariance;
                                        data[col][row] = covariance;
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                                        LOGGER.error(errMsg, exception);
                                        throw new DataReaderException(errMsg);
                                    }
                                } else {
                                    String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }

                                col++;
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

            // case where no newline at end of file
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (col > row) {
                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNum, col + 1, row + 1);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                } else if (col < row) {
                    String errMsg = String.format("Insufficent data on line %d.  Extracted %d value(s) but expected %d.", lineNum, col + 1, row + 1);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                } else {
                    if (value.length() > 0) {
                        try {
                            double covariance = Double.parseDouble(value);
                            data[row][col] = covariance;
                            data[col][row] = covariance;
                        } catch (NumberFormatException exception) {
                            String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNum, colNum);
                            LOGGER.error(errMsg, exception);
                            throw new DataReaderException(errMsg);
                        }
                    } else {
                        String errMsg = String.format("Missing data on line %d at column %d.", lineNum, colNum);
                        LOGGER.error(errMsg);
                        throw new DataReaderException(errMsg);
                    }
                }
            }
        }

        return data;
    }

    private List<String> extractVariables() throws IOException {
        List<String> variables = new LinkedList<>();

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNum = 1;
            int colNum = 0;
            boolean skipLine = false;
            boolean skipCaseNum = true;
            boolean doneExtractVars = false;
            boolean reqCheck = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipCaseNum) {
                    while (buffer.hasRemaining() && skipCaseNum && !Thread.currentThread().isInterrupted()) {
                        byte currChar = buffer.get();

                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                            if (prevNonBlankChar > SPACE_CHAR) {
                                skipCaseNum = false;
                                prevNonBlankChar = SPACE_CHAR;
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

                while (buffer.hasRemaining() && !doneExtractVars && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        skipLine = false;
                        if (prevNonBlankChar > SPACE_CHAR) {
                            doneExtractVars = true;
                            prevNonBlankChar = SPACE_CHAR;
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

                                if (value.length() > 0) {
                                    variables.add(value);
                                } else {
                                    String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
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
            } while (position < fileSize && !doneExtractVars && !Thread.currentThread().isInterrupted());

            // data at the end of line
            if (colNum > 0 || dataBuilder.length() > 0) {
                colNum++;
                String value = dataBuilder.toString().trim();
                dataBuilder.delete(0, dataBuilder.length());

                if (value.length() > 0) {
                    variables.add(value);
                } else {
                    String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        return variables;
    }

    public int getNumberOfCases() throws IOException {
        int numOfCases = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNum = 1;
            boolean finished = false;
            boolean skipLine = false;
            boolean reqCheck = prefix.length > 0;  // require check for comment
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !finished && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();

                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        skipLine = false;

                        if (prevNonBlankChar > SPACE_CHAR) {
                            finished = true;
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

                        dataBuilder.append((char) currChar);
                    }

                    prevChar = currChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while ((position < fileSize) && !finished && !Thread.currentThread().isInterrupted());

            if (dataBuilder.length() > 0) {
                String value = dataBuilder.toString().trim();
                try {
                    numOfCases += Integer.parseInt(value);
                } catch (NumberFormatException exception) {
                    String errMsg = String.format("Invalid number %s on line %d.", value, lineNum);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        return numOfCases;
    }

}
