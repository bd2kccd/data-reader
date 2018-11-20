/*
 * Copyright (C) 2018 University of Pittsburgh.
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

import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 *
 * Nov 16, 2018 4:45:51 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataFileReader implements TabularDataReader {

    protected final TabularColumnFileReader columnFileReader;
    protected final TabularDataFileReader dataFileReader;

    public AbstractTabularDataFileReader(File dataFile, Delimiter delimiter) {
        this.columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
        this.dataFileReader = new TabularDataFileReader(dataFile, delimiter);
    }

    public abstract TabularData readInData(Set<String> excludedVariables) throws IOException;

    public abstract TabularData readInData(int[] excludedColumns) throws IOException;

    public TabularData readInData() throws IOException {
        return readInData(Collections.EMPTY_SET);
    }

    @Override
    public void setHasHeader(boolean hasHeader) {
        this.columnFileReader.setHasHeader(hasHeader);
        this.dataFileReader.setHasHeader(hasHeader);
    }

    @Override
    public int getNumberOfColumns() throws IOException {
        return this.columnFileReader.getNumberOfColumns();
    }

    @Override
    public int getNumberOfRows() throws IOException {
        return this.columnFileReader.getNumberOfRows();
    }

    @Override
    public void setQuoteCharacter(char quoteCharacter) {
        this.columnFileReader.setQuoteCharacter(quoteCharacter);
        this.dataFileReader.setQuoteCharacter(quoteCharacter);
    }

    @Override
    public void setMissingValueMarker(String missingValueMarker) {
        this.columnFileReader.setMissingValueMarker(missingValueMarker);
        this.dataFileReader.setMissingValueMarker(missingValueMarker);
    }

    @Override
    public void setCommentMarker(String commentMarker) {
        this.columnFileReader.setCommentMarker(commentMarker);
        this.dataFileReader.setCommentMarker(commentMarker);
    }

}
