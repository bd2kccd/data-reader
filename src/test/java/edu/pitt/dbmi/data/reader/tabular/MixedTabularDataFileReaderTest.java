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
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.MixedDataColumn;
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.MixedTabularDataset;
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
 * Nov 18, 2018 12:58:50 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedTabularDataFileReaderTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    private final int numberOfDiscreteCategories = 4;

    private final Set<String> excludeVariables = new HashSet<>(Arrays.asList("X1", "X12", "X3", "X5", "X7", "X9"));

    private final Path[] dataFiles = {
        Paths.get(getClass().getResource("/data/tabular/mixed/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/quotes_sim_test_data.csv").getFile())
    };

    public MixedTabularDataFileReaderTest() {
    }

    /**
     * Test of readInData method, of class MixedTabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInData() throws IOException {
        for (Path dataFile : dataFiles) {
            MixedTabularDataFileReader dataFileReader = new MixedTabularDataFileReader(numberOfDiscreteCategories, dataFile.toFile(), delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabularData = dataFileReader.readInData();
            Assert.assertTrue(tabularData instanceof MixedTabularDataset);

            MixedTabularDataset tabularDataset = (MixedTabularDataset) tabularData;
            MixedDataColumn[] columns = tabularDataset.getColumns();

            int numOfCols = columns.length;
            long expected = 10;
            long actual = numOfCols;
            Assert.assertEquals(expected, actual);

            int numOfRows = tabularDataset.getNumOfRows();
            expected = 20;
            actual = numOfRows;
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testReadInSelectedData() throws IOException {
        for (Path dataFile : dataFiles) {
            MixedTabularDataFileReader dataFileReader = new MixedTabularDataFileReader(numberOfDiscreteCategories, dataFile.toFile(), delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabularData = dataFileReader.readInData(excludeVariables);
            Assert.assertTrue(tabularData instanceof MixedTabularDataset);

            MixedTabularDataset tabularDataset = (MixedTabularDataset) tabularData;
            MixedDataColumn[] columns = tabularDataset.getColumns();

            int numOfCols = columns.length;
            long expected = 5;
            long actual = numOfCols;
            Assert.assertEquals(expected, actual);

            int numOfRows = tabularDataset.getNumOfRows();
            expected = 20;
            actual = numOfRows;
            Assert.assertEquals(expected, actual);
        }
    }

}
