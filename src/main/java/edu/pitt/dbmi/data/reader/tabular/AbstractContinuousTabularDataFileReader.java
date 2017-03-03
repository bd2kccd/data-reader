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
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Mar 2, 2017 1:35:57 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractContinuousTabularDataFileReader extends AbstractTabularDataFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractContinuousTabularDataFileReader.class);

    public AbstractContinuousTabularDataFileReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    protected List<String> extractVariables(int[] excludedColumns) throws IOException {
        List<String> variables = new LinkedList<>();

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
            int numOfExCols = excludedColumns.length;
            int excludedIndex = 0;
            boolean requireCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean finished = false;
            boolean hasQuoteChar = false;
            byte prevNonBlankChar = SPACE_CHAR;
            byte prevChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !finished) {
                    byte currChar = buffer.get();

                    if (skipLine) {
                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (prevNonBlankChar > SPACE_CHAR) {
                            finished = true;

                            // data at the end of line
                            if (colNum > 0 || dataBuilder.length() > 0) {
                                colNum++;
                                String value = dataBuilder.toString().trim();
                                dataBuilder.delete(0, dataBuilder.length());

                                if (numOfExCols == 0 || excludedIndex >= numOfExCols || colNum != excludedColumns[excludedIndex]) {
                                    switch (delimiter) {
                                        case WHITESPACE:
                                            if (value.length() > 0) {
                                                variables.add(value);
                                            }
                                            break;
                                        default:
                                            if (value.length() > 0) {
                                                variables.add(value);
                                            } else {
                                                String errMsg = String.format("Missing variable name on line %d at column %d.", lineNum, colNum);
                                                LOGGER.error(errMsg);
                                                throw new DataReaderException(errMsg);
                                            }
                                    }
                                }
                            }
                        }

                        lineNum++;
                        if (currChar == LINE_FEED && prevChar == CARRIAGE_RETURN) {
                            lineNum--;
                        }
                    } else {
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        if (requireCheck && prevNonBlankChar > SPACE_CHAR) {
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
                                requireCheck = false;
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
                                        variables.add(value);
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
            } while (position < fileSize);
        }

        return variables;
    }

}
