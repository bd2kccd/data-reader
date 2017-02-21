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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * Feb 8, 2017 1:46:38 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractDataReader {

    protected static final byte LINE_FEED = '\n';
    protected static final byte CARRIAGE_RETURN = '\r';

    protected static final byte SPACE = ' ';

    protected byte quoteCharacter;
    protected String commentMarker;

    private int numberOfRows;
    private int numberOfColumns;

    protected final File dataFile;
    protected final byte delimiter;

    public AbstractDataReader(File dataFile, char delimiter) {
        this.dataFile = dataFile;
        this.delimiter = (byte) delimiter;

        // set default values
        this.quoteCharacter = -1;
        this.commentMarker = "";
        this.numberOfRows = -1;
        this.numberOfColumns = -1;
    }

    /**
     * Count number columns from the first non-empty line, ignoring lines that
     * begin with comment markers. Comment marker must not contain any white
     * space characters.
     *
     * @param comment comment marker
     * @return number of columns
     * @throws IOException when unable to read file
     */
    private int countColumns(String comment) throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte[] prefix = comment.getBytes();
            int index = 0;
            boolean checkRequired = true;  // require check for comment
            boolean skipLine = false;
            boolean hasQuoteChar = false;
            boolean endOfLine = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !endOfLine) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currentChar > SPACE || currentChar == delimiter) {
                        if (checkRequired) {
                            if (currentChar == prefix[index]) {
                                index++;

                                // all the comment chars are matched
                                if (index == prefix.length) {
                                    index = 0;
                                    skipLine = true;
                                    count = 0;
                                    continue;
                                }
                            } else {
                                index = 0;
                                checkRequired = false;
                            }
                        }

                        if (count == 0) {
                            count++;
                        }

                        if (currentChar == delimiter) {
                            if (!hasQuoteChar) {
                                count++;
                            }
                        } else if (currentChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        endOfLine = count > 0;
                    }
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
     * Count number columns from the first non-empty line.
     *
     * @return number of columns
     * @throws IOException when unable to read file
     */
    private int countColumns() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            boolean hasQuoteChar = false;
            boolean endOfLine = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !endOfLine) {
                    byte currentChar = buffer.get();

                    if (currentChar > SPACE || currentChar == delimiter) {
                        if (count == 0) {
                            count++;
                        }

                        if (currentChar == delimiter) {
                            if (!hasQuoteChar) {
                                count++;
                            }
                        } else if (currentChar == quoteCharacter) {
                            hasQuoteChar = !hasQuoteChar;
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        endOfLine = count > 0;
                    }
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
     * Count the number of non-empty lines, ignoring lines that begin with
     * comment markers. Comment marker must not contain any white space
     * characters.
     *
     * @param comment comment marker
     * @return number of non-empty lines
     * @throws IOException when unable to read file
     */
    private int countRows(String comment) throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte[] prefix = comment.getBytes();
            int index = 0;
            boolean skipLine = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining()) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currentChar > SPACE || currentChar == delimiter) {
                        if (currentChar == prefix[index]) {
                            index++;

                            // all the comment chars are matched
                            if (index == prefix.length) {
                                index = 0;
                                skipLine = true;
                            }
                        } else {
                            count++;
                            index = 0;
                            skipLine = true;
                        }
                    } else if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                        if (index > 0) {
                            count++;
                            index = 0;
                        }
                    }
                }

                // case when there's no newline at the end of the file
                if (index > 0) {
                    count++;
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
     * Count the number of non-empty lines.
     *
     * @return number of non-empty lines
     * @throws IOException when unable to read file
     */
    private int countRows() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            boolean skipLine = false;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining()) {
                    byte currentChar = buffer.get();

                    if (skipLine) {
                        if (currentChar == CARRIAGE_RETURN || currentChar == LINE_FEED) {
                            skipLine = false;
                        }
                    } else if (currentChar > SPACE || currentChar == delimiter) {
                        count++;
                        skipLine = true;
                    }
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while (position < fileSize);
        }

        return count;
    }

    public byte getQuoteCharacter() {
        return quoteCharacter;
    }

    public void setQuoteCharacter(char quoteCharacter) {
        this.quoteCharacter = (byte) quoteCharacter;
    }

    public String getCommentMarker() {
        return commentMarker;
    }

    public void setCommentMarker(String commentMarker) {
        if (commentMarker != null) {
            commentMarker = commentMarker.trim();
            if (commentMarker.length() > 0) {
                this.commentMarker = commentMarker;
            }
        }
    }

    public int getNumberOfRows() throws IOException {
        if (numberOfRows == -1) {
            numberOfRows = commentMarker.isEmpty()
                    ? countRows()
                    : countRows(commentMarker);
        }

        return numberOfRows;
    }

    public int getNumberOfColumns() throws IOException {
        if (numberOfColumns == -1) {
            numberOfColumns = commentMarker.isEmpty()
                    ? countColumns()
                    : countColumns(commentMarker);
        }

        return numberOfColumns;
    }

}
