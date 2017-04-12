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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Mar 6, 2017 5:30:25 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDataReaderTest {

    public VerticalDiscreteTabularDataReaderTest() {
    }

    @Test
    public void testReadInDataWithMissingValues() throws IOException {
        Path dataFile = Paths.get("test", "data", "sim_data", "discrete", "small_discrete_data_missing.prn");
        Delimiter delimiter = Delimiter.WHITESPACE;
        char quoteCharacter = '"';
        String missingValueMarker = "*";
        String commentMarker = "//";

        TabularDataReader reader = new VerticalDiscreteTabularDataReader(dataFile.toFile(), delimiter);
        reader.setQuoteCharacter(quoteCharacter);
        reader.setMissingValueMarker(missingValueMarker);
        reader.setCommentMarker(commentMarker);

        Dataset dataset = reader.readInData();
        validateDataset(dataset, 10, 19);
    }

    /**
     * Test of readInData method, of class VerticalDiscreteTabularDataReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInData() throws IOException {
        Path dataFile = Paths.get("test", "data", "sim_data", "discrete", "small_discrete_data.prn");
        Delimiter delimiter = Delimiter.WHITESPACE;
        char quoteCharacter = '"';
        String missingValueMarker = "*";
        String commentMarker = "//";

        TabularDataReader reader = new VerticalDiscreteTabularDataReader(dataFile.toFile(), delimiter);
        reader.setQuoteCharacter(quoteCharacter);
        reader.setMissingValueMarker(missingValueMarker);
        reader.setCommentMarker(commentMarker);

        Dataset dataset = reader.readInData();
        validateDataset(dataset, 10, 19);
    }

    private void validateDataset(Dataset dataset, long numOfVars, long numOfCases) {
        boolean isVerticalDiscreteTabularDataReader = (dataset instanceof VerticalDiscreteTabularDataset);
        Assert.assertTrue(isVerticalDiscreteTabularDataReader);

        if (isVerticalDiscreteTabularDataReader) {
            VerticalDiscreteTabularDataset discDataset = (VerticalDiscreteTabularDataset) dataset;

            DiscreteVarInfo[] varInfos = discDataset.getVariableInfos();
            long expected = numOfVars;
            long actual = varInfos.length;
            Assert.assertEquals(expected, actual);

            int[][] data = discDataset.getData();
            expected = numOfVars;
            actual = data.length;
            Assert.assertEquals(expected, actual);

            expected = numOfCases;
            actual = data[0].length;
            Assert.assertEquals(expected, actual);
        }
    }

}
