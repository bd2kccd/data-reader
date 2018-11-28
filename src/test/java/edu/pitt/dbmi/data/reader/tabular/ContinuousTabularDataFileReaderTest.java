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
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.ContinuousTabularDataset;
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
 * Nov 16, 2018 5:11:06 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ContinuousTabularDataFileReaderTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    private final Set<String> excludeVariables = new HashSet<>(Arrays.asList("X1", "X12", "X3", "X5", "X7", "X9"));

    private final Path[] dataFiles = {
        Paths.get(getClass().getResource("/data/tabular/continuous/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/sim_test_data.csv").getFile())
    };

    public ContinuousTabularDataFileReaderTest() {
    }

    /**
     * Test of readInData method, of class ContinuousTabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInData() throws IOException {
        for (Path dataFile : dataFiles) {
            ContinuousTabularDataFileReader dataFileReader = new ContinuousTabularDataFileReader(dataFile.toFile(), delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabularData = dataFileReader.readInData();
            Assert.assertTrue(tabularData instanceof ContinuousTabularDataset);

            ContinuousTabularDataset tabularDataset = (ContinuousTabularDataset) tabularData;

            TabularColumnFileReader.TabularDataColumn[] columns = tabularDataset.getColumns();
            long expected = 10;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            double[][] data = tabularDataset.getData();

            int numOfRows = data.length;
            expected = 18;
            actual = numOfRows;
            Assert.assertEquals(expected, actual);

            int numOfCols = data[0].length;
            expected = 10;
            actual = numOfCols;
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testReadInSelectedData() throws IOException {
        for (Path dataFile : dataFiles) {
            ContinuousTabularDataFileReader dataFileReader = new ContinuousTabularDataFileReader(dataFile.toFile(), delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabularData = dataFileReader.readInData(excludeVariables);
            Assert.assertTrue(tabularData instanceof ContinuousTabularDataset);

            ContinuousTabularDataset tabularDataset = (ContinuousTabularDataset) tabularData;
            TabularColumnFileReader.TabularDataColumn[] columns = tabularDataset.getColumns();
            long expected = 5;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            double[][] data = tabularDataset.getData();

            int numOfRows = data.length;
            expected = 18;
            actual = numOfRows;
            Assert.assertEquals(expected, actual);

            int numOfCols = data[0].length;
            expected = 5;
            actual = numOfCols;
            Assert.assertEquals(expected, actual);
        }
    }

}
