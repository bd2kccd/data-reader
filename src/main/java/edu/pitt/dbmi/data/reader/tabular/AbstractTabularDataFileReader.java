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
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Feb 25, 2017 1:36:46 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataFileReader extends AbstractBasicTabularDataFileReader implements TabularDataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTabularDataFileReader.class);

    public AbstractTabularDataFileReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    protected abstract Dataset readInDataFromFile(int[] excludedColumns) throws IOException;

    @Override
    public Dataset readInData(Set<String> excludedVariables) throws IOException {
        Dataset dataset;
        try {
            int[] excludedColumns = hasHeader ? getColumnNumbers(excludedVariables) : new int[0];
            dataset = readInDataFromFile(excludedColumns);
        } catch (ClosedByInterruptException exception) {
            dataset = null;
            LOGGER.error("", exception);
        }

        return dataset;
    }

    @Override
    public Dataset readInData(int[] excludedColumns) throws IOException {
        Dataset dataset;
        try {
            dataset = readInDataFromFile(filterValidColumnNumbers(excludedColumns));
        } catch (ClosedByInterruptException exception) {
            dataset = null;
            LOGGER.error("", exception);
        }

        return dataset;
    }

    @Override
    public Dataset readInData() throws IOException {
        Dataset dataset;
        try {
            dataset = readInData(Collections.EMPTY_SET);
        } catch (ClosedByInterruptException exception) {
            dataset = null;
            LOGGER.error("", exception);
        }

        return dataset;
    }

}
