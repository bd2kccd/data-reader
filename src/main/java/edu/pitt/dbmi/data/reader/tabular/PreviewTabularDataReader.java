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

import edu.pitt.dbmi.data.reader.AbstractDataReader;
import edu.pitt.dbmi.data.reader.PreviewDataReader;
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
 * Feb 10, 2017 4:47:33 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class PreviewTabularDataReader extends AbstractDataReader implements PreviewDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreviewTabularDataReader.class);

    public PreviewTabularDataReader(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    public List<String> getPreviews(int fromRow, int toRow, int fromColumn, int toColumn) throws IOException {
        if (toRow < fromRow) {
            throw new IllegalArgumentException("Parameter toRow must be greater than or equal to fromRow.");
        }
        if (toColumn < fromColumn) {
            throw new IllegalArgumentException("Parameter toColumn must be greater than or equal to fromColumn.");
        }

        List<String> previews = new LinkedList<>();

        char delim = (char) delimiter;
        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            StringBuilder lineBuilder = new StringBuilder();
            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
            int rowNum = 1;
            boolean skipLine = false;
            boolean checkRequired = true;  // require check for comment
            boolean hasQuoteChar = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);

                while (buffer.hasRemaining()) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (rowNum >= fromRow && rowNum <= toRow) {
                        if (currentChar >= SPACE || currentChar == delimiter) {
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
                                dataBuilder.append((char) currentChar);
                            } else if (currentChar == delimiter) {
                                if (hasQuoteChar) {
                                    dataBuilder.append((char) currentChar);
                                } else {
                                    colNum++;
                                    String value = dataBuilder.toString().trim();
                                    dataBuilder.delete(0, dataBuilder.length());

                                    if (colNum >= fromColumn && colNum <= toColumn) {
                                        lineBuilder.append(value);
                                        lineBuilder.append(delim);
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

                                if (colNum >= fromColumn && colNum <= toColumn) {
                                    lineBuilder.append(value);
                                    lineBuilder.append(delim);
                                }

                                rowNum++;
                            }

                            lineBuilder.deleteCharAt(lineBuilder.length() - 1);
                            String line = lineBuilder.toString().trim();
                            lineBuilder.delete(0, lineBuilder.length());
                            if (line.length() > 0) {
                                previews.add(line);
                            }

                            colNum = 0;
                            checkRequired = true;
                        }
                    } else {
                        skipLine = true;
                    }
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);
        }

        return previews;
    }

}
