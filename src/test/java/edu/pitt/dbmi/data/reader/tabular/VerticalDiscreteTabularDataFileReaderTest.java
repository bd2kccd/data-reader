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
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.DiscreteDataColumn;
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.VerticalDiscreteTabularDataset;
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
 * Nov 17, 2018 3:41:17 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDataFileReaderTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    private final Set<String> excludeVariables = new HashSet<>(Arrays.asList("X1", "X12", "X3", "X5", "X7", "X9"));

    private final Path[] dataFiles = {
        Paths.get(getClass().getResource("/data/tabular/discrete/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/discrete/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/discrete/sim_test_data.csv").getFile())
    };

    public VerticalDiscreteTabularDataFileReaderTest() {
    }

    /**
     * Test of readInData method, of class VerticalDiscreteTabularDataReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInData() throws IOException {
        for (Path dataFile : dataFiles) {
            VerticalDiscreteTabularDataFileReader dataFileReader = new VerticalDiscreteTabularDataFileReader(dataFile.toFile(), delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabularData = dataFileReader.readInData();
            Assert.assertTrue(tabularData instanceof VerticalDiscreteTabularDataset);

            VerticalDiscreteTabularDataset tabularDataset = (VerticalDiscreteTabularDataset) tabularData;
            DiscreteDataColumn[] columns = tabularDataset.getColumns();
            long expected = 10;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            int[][] data = tabularDataset.getData();

            int numOfCols = data.length;
            expected = 10;
            actual = numOfCols;
            Assert.assertEquals(expected, actual);

            int numOfRows = data[0].length;
            expected = 19;
            actual = numOfRows;
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testReadInSelectedData() throws IOException {
        for (Path dataFile : dataFiles) {
            VerticalDiscreteTabularDataFileReader dataFileReader = new VerticalDiscreteTabularDataFileReader(dataFile.toFile(), delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabularData = dataFileReader.readInData(excludeVariables);
            Assert.assertTrue(tabularData instanceof VerticalDiscreteTabularDataset);

            VerticalDiscreteTabularDataset tabularDataset = (VerticalDiscreteTabularDataset) tabularData;
            DiscreteDataColumn[] columns = tabularDataset.getColumns();
            long expected = 5;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            int[][] data = tabularDataset.getData();

            int numOfCols = data.length;
            expected = 5;
            actual = numOfCols;
            Assert.assertEquals(expected, actual);

            int numOfRows = data[0].length;
            expected = 19;
            actual = numOfRows;
            Assert.assertEquals(expected, actual);
        }
    }

}
