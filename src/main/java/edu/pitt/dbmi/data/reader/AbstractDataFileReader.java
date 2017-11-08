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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

/**
 *
 * Mar 4, 2017 1:14:52 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractDataFileReader {

    protected static final byte LINE_FEED = '\n';
    protected static final byte CARRIAGE_RETURN = '\r';

    protected static final byte SPACE_CHAR = Delimiter.SPACE.getDelimiterChar();
    protected static final String EMPTY_STRING = "";

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

        this.missingValueMarker = EMPTY_STRING;
        this.commentMarker = EMPTY_STRING;
        this.quoteCharacter = -1;
        this.numberOfLines = -1;
        this.numberOfColumns = -1;
    }

    private int countNumberOfColumns() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            boolean reqCmntCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean hasQuoteChar = false;
            boolean finished = false;
            boolean eol = true;
            byte prevChar = -1;
            byte prevNonBlankChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !finished && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();
                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if ((prevChar == CARRIAGE_RETURN || prevChar == LINE_FEED) && prevChar != currChar) {
                            prevChar = currChar;
                            continue;
                        }

                        finished = !skipLine;
                        if (finished) {
                            count++;
                        }

                        skipLine = false;
                        reqCmntCheck = prefix.length > 0;
                        index = 0;
                        prevNonBlankChar = -1;
                        eol = true;
                        hasQuoteChar = false;
                    } else {
                        eol = false;

                        if (!skipLine) {
                            // save any non-blank char encountered
                            if (currChar > SPACE_CHAR) {
                                prevNonBlankChar = currChar;
                            }

                            // skip any blank chars at the begining of the line
                            if (currChar <= SPACE_CHAR && prevNonBlankChar <= SPACE_CHAR) {
                                continue;
                            }

                            if (reqCmntCheck) {
                                if (currChar == prefix[index]) {
                                    index++;
                                    if (index == prefix.length) {
                                        skipLine = true;
                                        prevChar = currChar;
                                        continue;
                                    }
                                } else {
                                    reqCmntCheck = false;
                                }
                            }

                            if (currChar == quoteCharacter) {
                                hasQuoteChar = !hasQuoteChar;
                            } else if (!hasQuoteChar) {
                                switch (delimiter) {
                                    case WHITESPACE:
                                        if (currChar <= SPACE_CHAR && prevChar > SPACE_CHAR) {
                                            count++;
                                        }
                                        break;
                                    default:
                                        if (currChar == delimChar) {
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
            } while ((position < fileSize) && !finished && !Thread.currentThread().isInterrupted());

            if (!(eol || skipLine)) {
                count++;
            }
        }

        return count;
    }

    private int countNumberOfLines() throws IOException {
        int count = 0;

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            boolean reqCmntCheck = prefix.length > 0;
            boolean skipLine = false;
            boolean moveToEOL = false;
            byte prevChar = -1;
            byte prevNonBlankChar = -1;
            do {
                MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, position, size);
                while (buffer.hasRemaining() && !Thread.currentThread().isInterrupted()) {
                    byte currChar = buffer.get();
                    if (currChar == CARRIAGE_RETURN || currChar == LINE_FEED) {
                        if ((prevChar == CARRIAGE_RETURN || prevChar == LINE_FEED) && prevChar != currChar) {
                            continue;
                        }

                        if (!skipLine) {
                            count++;
                        }
                        index = 0;
                        moveToEOL = false;
                        skipLine = false;
                        prevNonBlankChar = -1;
                    } else if (!moveToEOL) {
                        // save any non-blank char encountered
                        if (currChar > SPACE_CHAR) {
                            prevNonBlankChar = currChar;
                        }

                        // skip any blank chars at the begining of the line
                        if (currChar <= SPACE_CHAR && prevNonBlankChar <= SPACE_CHAR) {
                            continue;
                        }

                        if (reqCmntCheck) {
                            if (currChar == prefix[index]) {
                                index++;
                                if (index == prefix.length) {
                                    moveToEOL = true;
                                    skipLine = true;
                                }
                            } else {
                                moveToEOL = true;
                            }
                        } else {
                            moveToEOL = true;
                        }
                    }

                    prevChar = currChar;
                }

                position += size;
                if ((position + size) > fileSize) {
                    size = fileSize - position;
                }
            } while ((position < fileSize) && !Thread.currentThread().isInterrupted());

            if (!(prevChar == CARRIAGE_RETURN || prevChar == LINE_FEED) && !skipLine) {
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
        this.missingValueMarker = (missingValueMarker == null)
                ? EMPTY_STRING
                : missingValueMarker.trim();
    }

    public String getCommentMarker() {
        return commentMarker;
    }

    public void setCommentMarker(String commentMarker) {
        this.commentMarker = (commentMarker == null)
                ? EMPTY_STRING
                : commentMarker.trim();
    }

    public int getNumberOfLines() throws IOException {
        if (numberOfLines == -1) {
            try {
                numberOfLines = countNumberOfLines();
            } catch (ClosedByInterruptException exception) {
                numberOfLines = -1;
            }
        }

        return numberOfLines;
    }

    public int getNumberOfColumns() throws IOException {
        if (numberOfColumns == -1) {
            try {
                numberOfColumns = countNumberOfColumns();
            } catch (ClosedByInterruptException exception) {
                numberOfColumns = -1;
            }
        }

        return numberOfColumns;
    }

}
