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
import edu.pitt.dbmi.data.Delimiter;
import edu.pitt.dbmi.data.VerticalDiscreteTabularDataset;
import java.io.File;
import java.io.IOException;

/**
 *
 * Feb 14, 2017 2:12:24 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDataReader extends AbstractDiscreteTabularDataFileReader implements TabularDataReader {

    public VerticalDiscreteTabularDataReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    @Override
    protected Dataset readInDataFromFile(int[] excludedColumns) throws IOException {
        DiscreteVarInfo[] varInfos = hasHeader ? extractVariables(excludedColumns) : generateDiscreteVariables(excludedColumns);
        varInfos = extractVariableData(varInfos, excludedColumns);
        for (DiscreteVarInfo varInfo : varInfos) {
            varInfo.recategorize();
        }

        int[][] data = extractAndEncodeData(varInfos, excludedColumns);

        return new VerticalDiscreteTabularDataset(varInfos, data);
    }

}
