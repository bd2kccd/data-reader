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
import edu.pitt.dbmi.data.reader.AbstractDataFileReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * Feb 25, 2017 1:36:46 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataFileReader extends AbstractDataFileReader {

    protected boolean hasHeader;

    public AbstractTabularDataFileReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
        this.hasHeader = true;
    }

    protected int[] getColumnNumbers(Set<String> variables) throws IOException {
        List<Integer> indexList = new LinkedList<>();

        try (FileChannel fc = new RandomAccessFile(dataFile, "r").getChannel()) {
            long fileSize = fc.size();
            long position = 0;
            long size = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : fileSize;

            byte delimChar = delimiter.getDelimiterChar();

            StringBuilder dataBuilder = new StringBuilder();
            byte[] prefix = commentMarker.getBytes();
            int index = 0;
            int colNum = 0;
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
                        taskFinished = prevNonBlankChar > SPACE_CHAR;
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

                                if (variables.contains(value)) {
                                    System.out.println(value);
                                    indexList.add(colNum);
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

                if (variables.contains(value)) {
                    System.out.println(value);
                    indexList.add(colNum);
                }
            }
        }

        int[] indices = new int[indexList.size()];
        if (indices.length > 0) {
            int i = 0;
            for (Integer index : indexList) {
                indices[i++] = index;
            }
        }

        return indices;
    }

    protected int[] filterValidColumnNumbers(int[] columnNumbers) throws IOException {
        Set<Integer> indices = new TreeSet<>();
        int numOfVars = getNumberOfColumns();
        for (int colNum : columnNumbers) {
            if (colNum > 0 && colNum <= numOfVars) {
                indices.add(colNum);
            }
        }

        int[] results = new int[indices.size()];
        int i = 0;
        for (Integer index : indices) {
            results[i++] = index;
        }

        return results;
    }

    /**
     *
     * @param excludedColumns sorted array of column numbers
     * @return
     * @throws IOException
     */
    protected List<String> generateVariables(int[] excludedColumns) throws IOException {
        List<String> nodes = new LinkedList<>();

        int numOfCols = getNumberOfColumns();
        int length = excludedColumns.length;
        int excludedIndex = 0;
        for (int colNum = 1; colNum <= numOfCols; colNum++) {
            if (length > 0 && (excludedIndex < length && colNum == excludedColumns[excludedIndex])) {
                excludedIndex++;
            } else {
                nodes.add(String.format("VAR_%d", colNum));
            }
        }

        return nodes;
    }

    public boolean isHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

}
