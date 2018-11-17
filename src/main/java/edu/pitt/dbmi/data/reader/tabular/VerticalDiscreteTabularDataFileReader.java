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
import edu.pitt.dbmi.data.reader.tabular.TabularColumnFileReader.TabularDataColumn;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 *
 * Nov 17, 2018 3:30:43 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDataFileReader extends AbstractTabularDataFileReader {

    private final boolean isDiscrete;

    public VerticalDiscreteTabularDataFileReader(Path dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
        this.isDiscrete = true;
    }

    @Override
    public TabularData readInData(Set<String> excludedVariables) throws IOException {
        TabularDataColumn[] columns = columnFileReader.readInDataColumns(excludedVariables, isDiscrete);

        return dataFileReader.readInData(columns);
    }

    @Override
    public TabularData readInData(int[] excludedColumns) throws IOException {
        TabularDataColumn[] columns = columnFileReader.readInDataColumns(excludedColumns, isDiscrete);

        return dataFileReader.readInData(columns);
    }

}
