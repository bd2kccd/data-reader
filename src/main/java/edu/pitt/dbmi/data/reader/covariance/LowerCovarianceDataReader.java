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
import edu.pitt.dbmi.data.reader.AbstractDataReader;
import edu.pitt.dbmi.data.reader.DataReaderException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
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
public class LowerCovarianceDataReader extends AbstractDataReader implements CovarianceDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowerCovarianceDataReader.class);

    public LowerCovarianceDataReader(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    public Dataset readInData() throws IOException {
        int numberOfCases = getNumberOfCases();
        List<String> variables = extractVariables();
        double[][] data = extractCovarianceData(variables.size());

        return new CovarianceDataset(numberOfCases, variables, data);
    }

    private double[][] extractCovarianceData(int matrixSize) throws IOException {
        double[][] data = new double[matrixSize][matrixSize];

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNumber = 1;
            int colNum = 0;
            int numOfLineData = 0;
            int col = 0;
            int row = 0;
            boolean done = false;
            boolean skipLine = false;
            boolean skipToData = true;
            boolean checkRequired = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipToData) {
                    while (buffer.hasRemaining() && !done) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                done = (numOfLineData == 2);

                                lineNumber++;
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            if (prefix.length > 0 && currentChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    skipLine = true;
                                    index = 0;
                                }
                            } else {
                                skipLine = true;
                                index = 0;
                                numOfLineData++;
                            }
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            if (index > 0) {
                                numOfLineData++;
                                done = (numOfLineData == 2);
                            }
                            index = 0;

                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
                    }

                    skipToData = false;
                    done = false;
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

                                if (col > row) {
                                    String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, col + 1, row + 1);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }

                                if (value.length() > 0) {
                                    try {
                                        double covariance = Double.parseDouble(value);
                                        data[row][col] = covariance;
                                        data[col][row] = covariance;
                                    } catch (NumberFormatException exception) {
                                        String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNumber, colNum);
                                        LOGGER.error(errMsg, exception);
                                        throw new DataReaderException(errMsg);
                                    }
                                } else {
                                    String errMsg = String.format("Missing data on line %d at column %d.", lineNumber, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }

                                col++;
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        if (colNum > 0 || dataBuilder.length() > 0) {
                            colNum++;
                            String value = dataBuilder.toString().trim();
                            dataBuilder.delete(0, dataBuilder.length());

                            if (col > row) {
                                String errMsg = String.format("Excess data on line %d.  Extracted %d value(s) but expected %d.", lineNumber, col + 1, row + 1);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }

                            if (value.length() > 0) {
                                try {
                                    double covariance = Double.parseDouble(value);
                                    data[row][col] = covariance;
                                    data[col][row] = covariance;
                                } catch (NumberFormatException exception) {
                                    String errMsg = String.format("Invalid number %s on line %d at column %d.", value, lineNumber, colNum);
                                    LOGGER.error(errMsg, exception);
                                    throw new DataReaderException(errMsg);
                                }
                            } else {
                                String errMsg = String.format("Missing data on line %d at column %d.", lineNumber, colNum);
                                LOGGER.error(errMsg);
                                throw new DataReaderException(errMsg);
                            }

                            row++;
                        }

                        col = 0;
                        colNum = 0;
                        checkRequired = prefix.length > 0;

                        lineNumber++;
                        if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                            lineNumber--;
                        }
                    }
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);
        }

        return data;
    }

    private List<String> extractVariables() throws IOException {
        List<String> variables = new LinkedList<>();

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNumber = 1;
            int colNum = 0;
            boolean done = false;
            boolean skipLine = false;
            boolean skipToData = true;
            boolean isLineData = false;
            boolean checkRequired = prefix.length > 0;  // require check for comment
            boolean hasQuoteChar = false;
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                if (skipToData) {
                    while (buffer.hasRemaining() && !done) {
                        byte currentChar = buffer.get();

                        if (skipLine) {
                            if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                                skipLine = false;
                                if (isLineData) {
                                    done = true;
                                }

                                lineNumber++;
                                if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                    lineNumber--;
                                }
                            }
                        } else if (currentChar > SPACE || currentChar == delimiter) {
                            if (prefix.length > 0 && currentChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    skipLine = true;
                                    index = 0;
                                }
                            } else {
                                skipLine = true;
                                index = 0;
                                isLineData = true;
                            }
                        } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            if (index > 0) {
                                done = true;
                            }
                            index = 0;

                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }

                        previousChar = currentChar;
                    }
                    skipToData = false;
                    done = false;
                }

                while (buffer.hasRemaining() && !done) {
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

                                if (value.length() > 0) {
                                    variables.add(value);
                                } else {
                                    String errMsg = String.format("Missing variable name on line %d at column %d.", lineNumber, colNum);
                                    LOGGER.error(errMsg);
                                    throw new DataReaderException(errMsg);
                                }
                            }
                        } else {
                            dataBuilder.append((char) currentChar);
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        done = colNum > 0 || dataBuilder.length() > 0;

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

                if (value.length() > 0) {
                    variables.add(value);
                } else {
                    String errMsg = String.format("Missing variable name on line %d at column %d.", lineNumber, colNum);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        return variables;
    }

    public int getNumberOfCases() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int lineNumber = 1;
            boolean done = false;
            boolean skipLine = false;
            boolean checkRequired = prefix.length > 0;  // require check for comment
            byte previousChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !done) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currentChar > SPACE) {
                        if (checkRequired) {
                            if (currentChar == prefix[index]) {
                                index++;

                                // all the comment chars are matched
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    dataBuilder.delete(0, dataBuilder.length());

                                    previousChar = currentChar;
                                    continue;
                                }
                            } else {
                                index = 0;
                                checkRequired = false;
                            }
                        }

                        dataBuilder.append((char) currentChar);
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        if (dataBuilder.length() > 0) {
                            done = true;
                        } else {
                            lineNumber++;
                            if (currentChar == LINE_FEED && previousChar == CARRIAGE_RETURN) {
                                lineNumber--;
                            }
                        }
                    }

                    previousChar = currentChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while ((position < fileSize) && !done);

            if (dataBuilder.length() > 0) {
                String value = dataBuilder.toString().trim();
                try {
                    count = Integer.parseInt(value);
                } catch (NumberFormatException exception) {
                    String errMsg = String.format("Invalid number %s on line %d.", value, lineNumber);
                    LOGGER.error(errMsg);
                    throw new DataReaderException(errMsg);
                }
            }
        }

        return count;
    }

}
