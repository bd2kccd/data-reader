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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Feb 14, 2017 2:12:24 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDataReader extends AbstractDiscreteTabularDataReader implements TabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerticalDiscreteTabularDataReader.class);

    public VerticalDiscreteTabularDataReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    private Dataset readInDataFromFile(int[] excludedColumns) throws IOException {
        DiscreteVarInfo[] varInfos = hasHeader ? extractVariables(excludedColumns) : generateDiscreteVariables(excludedColumns);
        varInfos = extractVariableData(varInfos, excludedColumns);
        for (DiscreteVarInfo varInfo : varInfos) {
            System.out.println(varInfo);
        }
        return null;
    }

    @Override
    public Dataset readInData(Set<String> excludedVariables) throws IOException {
        int[] excludedColumns = hasHeader ? getColumnNumbers(excludedVariables) : new int[0];

        return readInDataFromFile(excludedColumns);
    }

    @Override
    public Dataset readInData(int[] excludedColumns) throws IOException {
        return readInDataFromFile(filterValidColumnNumbers(excludedColumns));
    }

    @Override
    public Dataset readInData() throws IOException {
        return readInData(Collections.EMPTY_SET);
    }

}
