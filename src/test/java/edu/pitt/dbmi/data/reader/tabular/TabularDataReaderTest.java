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

import edu.pitt.dbmi.data.ContinuousDataset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Feb 9, 2017 1:04:39 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularDataReaderTest {

    public TabularDataReaderTest() {
    }

    /**
     * Test of readInData method, of class TabularDataReader.
     *
     * @throws Exception
     */
    @Test
    public void testTabDelimited() throws Exception {
        Path dataFile = Paths.get("test", "data", "uci", "small_AirQualityUCI.csv");
        char delimiter = ',';

        TabularDataReader dataReader = new ContinuousTabularDataReader(dataFile.toFile(), delimiter);
        dataReader.setHasHeader(true);

        Set<String> excludeVars = new HashSet<>(Arrays.asList("Date", "Time"));
        ContinuousDataset dataSet = (ContinuousDataset) dataReader.readInData(excludeVars);

        List<String> variableNames = dataSet.getGetVariables();
        long expected = 13;
        long actual = variableNames.size();
        Assert.assertEquals(expected, actual);

        double[][] data = dataSet.getData();
        expected = 14;
        actual = data.length;
        Assert.assertEquals(expected, actual);

        expected = 13;
        actual = data[0].length;
        Assert.assertEquals(expected, actual);
    }

}
