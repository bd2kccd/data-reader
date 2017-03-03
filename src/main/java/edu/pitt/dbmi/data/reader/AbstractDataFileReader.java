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
package edu.pitt.dbmi.data.reader;

import edu.pitt.dbmi.data.Delimiter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * Feb 24, 2017 3:24:28 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class AbstractDataFileReader {

    protected static final byte LINE_FEED = '\n';
    protected static final byte CARRIAGE_RETURN = '\r';

    protected static final byte SPACE_CHAR = ' ';

    protected byte quoteCharacter;
    protected String missingValueMarker;
    protected String commentMarker;

    private int numberOfLines;
    private int numberOfColumns;

    protected final File dataFile;
    protected final Delimiter delimiter;

    public AbstractDataFileReader(File dataFile, Delimiter delimiter) {
        this.dataFile = dataFile;
        this.delimiter = delimiter;

        this.quoteCharacter = -1;
        this.commentMarker = "";

        this.numberOfLines = -1;
        this.numberOfColumns = -1;
    }

    /**
     * Count number of columns of the first non-blank line.
     *
     * @return number of columns
     * @throws IOException
     */
    private int countNumberOfColumns() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            boolean reqCheck = prefix.length > 0;
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

                            switch (delimiter) {
                                case WHITESPACE:
                                    if (prevChar > SPACE_CHAR) {
                                        count++;
                                    }
                                    break;
                                default:
                                    if (prevNonBlankChar > SPACE_CHAR) {
                                        count++;
                                    }
                            }
                        }
                    } else {
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
                                    count = 0;
                                    prevNonBlankChar = SPACE_CHAR;

                                    prevChar = currChar;
                                    continue;
                                }
                            } else {
                                reqCheck = false;
                            }
                        }

                        if (currChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        } else {
                            switch (delimiter) {
                                case WHITESPACE:
                                    if (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR) {
                                        if (!hasQuoteChar) {
                                            count++;
                                        }
                                    }
                                    break;
                                default:
                                    if (currChar == delimChar) {
                                        if (!hasQuoteChar) {
                                            count++;
                                        }
                                    }
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

        return count;
    }

    /**
     * Count number of non-blank lines.
     *
     * @return number of lines
     * @throws IOException
     */
    private int countNumberOfLines() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            boolean reqCheck = prefix.length > 0;
            boolean skipLine = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining()) {
                    byte currChar = buffer.get();

                    if (skipLine) {
                        if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if (index > 0) {
                            index = 0;
                            count++;
                        }
                    } else {
                        // case where line starts with spaces
                        if (currChar <= SPACE_CHAR && index == 0) {
                            continue;
                        }

                        if (reqCheck) {
                            if (currChar == prefix[index]) {
                                index++;

                                // all the comment chars are matched
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                }
                            } else {
                                index = 0;
                                skipLine = true;
                                count++;
                            }
                        } else {
                            // have seen at least a non-blank char
                            skipLine = true;
                            count++;
                        }
                    }
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);

            // case when there's no newline at the end of the file
            if (index > 0) {
                index = 0;
                count++;
            }
        }

        return count;
    }

    public char getQuoteCharacter() {
        return (char) quoteCharacter;
    }

    public void setQuoteCharacter(char quoteCharacter) {
        this.quoteCharacter = (byte) quoteCharacter;
    }

    public String getMissingValueMarker() {
        return missingValueMarker;
    }

    public void setMissingValueMarker(String missingValueMarker) {
        this.missingValueMarker = (missingValueMarker) == null
                ? missingValueMarker
                : missingValueMarker.trim();
    }

    public String getCommentMarker() {
        return commentMarker;
    }

    public void setCommentMarker(String commentMarker) {
        if (commentMarker != null) {
            this.commentMarker = commentMarker.trim();
        }
    }

    public int getNumberOfLines() throws IOException {
        if (numberOfLines == -1) {
            numberOfLines = countNumberOfLines();
        }

        return numberOfLines;
    }

    public int getNumberOfColumns() throws IOException {
        if (numberOfColumns == -1) {
            numberOfColumns = countNumberOfColumns();
        }

        return numberOfColumns;
    }

}
