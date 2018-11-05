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

import edu.pitt.dbmi.data.reader.DataReader;
import java.io.IOException;

/**
 *
 * Nov 5, 2018 2:51:35 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public interface TabularDataReader extends DataReader {

    /**
     * Set true if the dataset has a header on the first line.
     *
     * @param hasHeader
     */
    public void setHasHeader(boolean hasHeader);

    /**
     * Get the number of columns in the dataset. The number of column is based
     * on the column counts of the first line.
     *
     * @return
     * @throws IOException
     */
    public int getNumberOfColumns() throws IOException;

    /**
     * Get the number of rows containing data, exclude the header row.
     *
     * @return
     * @throws IOException
     */
    public int getNumberOfRows() throws IOException;

}
