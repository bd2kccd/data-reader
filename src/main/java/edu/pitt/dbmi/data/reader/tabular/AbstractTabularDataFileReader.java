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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 *
 * Feb 17, 2017 5:09:19 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataFileReader extends AbstractTabularDataReader implements TabularDataReader {

    public AbstractTabularDataFileReader(File dataFile, char delimiter) {
        super(dataFile, delimiter);
    }

    protected abstract Dataset readInDataFromFile(int[] excludedColumns) throws IOException;

    @Override
    public Dataset readInData() throws IOException {
        return readInData(Collections.EMPTY_SET);
    }

    @Override
    public Dataset readInData(Set<String> excludedVariables) throws IOException {
        return readInDataFromFile(getVariableColumnNumbers(excludedVariables));
    }

    @Override
    public Dataset readInData(int[] excludedColumns) throws IOException {
        return readInDataFromFile(getValidColumnNumbers(excludedColumns));
    }

}
