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

import edu.pitt.dbmi.data.Dataset;
import java.io.IOException;
import java.util.Set;

/**
 * Interface for reading in data.
 *
 * Jan 24, 2017 5:14:40 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public interface TabularDataReader {

    /**
     * Read in dataset.
     *
     * @return
     * @throws IOException whenever unable to read file
     */
    public Dataset readInData() throws IOException;

    /**
     *
     * Read in dataset. Excludes any variables from the given set.
     *
     * @param excludedVariables set of variable names to exclude
     * @return
     * @throws IOException whenever unable to read file
     */
    public Dataset readInData(Set<String> excludedVariables) throws IOException;

    public Dataset readInData(int[] excludedColumns) throws IOException;

    public void setQuoteCharacter(char quoteCharacter);

    public void setCommentMarker(String commentMarker);

    public void setHasHeader(boolean hasHeader);

}
