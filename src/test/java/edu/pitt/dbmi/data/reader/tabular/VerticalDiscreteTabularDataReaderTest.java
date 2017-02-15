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
import edu.pitt.dbmi.data.VerticalDiscreteTabularDataset;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Feb 14, 2017 4:09:06 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDataReaderTest {

    public VerticalDiscreteTabularDataReaderTest() {
    }

    /**
     * Test of readInDataset method, of class VerticalDiscreteTabularDataReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInVerticalTabularDiscreteDataset() throws IOException {
        Path dataFile = Paths.get("test", "data", "discrete", "uci_balloon.csv");
        char delimiter = ',';

        String[] variableNames = {
            //            "COLOR",
            "SIZE",
            //            "ACT",
            //            "AGE",
            "INFLATED"
        };
        Set<String> variables = new HashSet<>(Arrays.asList(variableNames));

        TabularDataReader dataReader = new VerticalDiscreteTabularDataReader(dataFile.toFile(), delimiter);
        dataReader.setHasHeader(true);

        Dataset dataset = dataReader.readInData(variables);
        if (dataset instanceof VerticalDiscreteTabularDataset) {
            VerticalDiscreteTabularDataset vDataset = (VerticalDiscreteTabularDataset) dataset;

            DiscreteVarInfo[] variableInfos = vDataset.getVariableInfos();
            int expected = 3;
            int actual = variableInfos.length;
            Assert.assertEquals(expected, actual);

            int[][] data = vDataset.getData();
            expected = 3;
            actual = data.length;
            Assert.assertEquals(expected, actual);

            expected = 20;
            actual = data[0].length;
            Assert.assertEquals(expected, actual);
        }
    }

}
