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

import edu.pitt.dbmi.data.reader.AbstractDataReader;
import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.File;
import java.io.IOException;

/**
 *
 * Nov 5, 2018 2:55:51 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataReader extends AbstractDataReader implements TabularDataReader {

    protected boolean hasHeader;

    public AbstractTabularDataReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    @Override
    public int getNumberOfColumns() throws IOException {
        return countNumberOfColumns();
    }

    @Override
    public int getNumberOfRows() throws IOException {
        return hasHeader
                ? countNumberOfLines() - 1
                : countNumberOfLines();
    }

}
