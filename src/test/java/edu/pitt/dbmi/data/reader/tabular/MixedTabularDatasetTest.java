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
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Nov 14, 2018 4:33:59 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedTabularDatasetTest {

    public MixedTabularDatasetTest() {
    }

    @Test
    public void testReadInAllVariables() throws IOException {
        String[] files = {
            //            "/data/mixed/dos_sim_test_data.csv",
            //            "/data/mixed/mac_sim_test_data.csv",
            "/data/mixed/sim_test_data.csv"
        };

        final Delimiter delimiter = Delimiter.COMMA;
        final char quoteCharacter = '"';
        final String missingValueMarker = "*";
        final String commentMarker = "//";
        final boolean hasHeader = true;

        for (String file : files) {
            String dataFileName = getClass().getResource(file).getFile();
            Path dataFile = Paths.get(dataFileName);

            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            TabularColumnFileReader.DataColumn[] dataColumns = columnFileReader.readInDataColumns(false);

            long expected = 10;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            columnFileReader.determineDiscreteDataColumns(dataColumns, 4);

            TabularDataFileReader dataFileReader = new TabularDataFileReader(dataFile, delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabData = dataFileReader.readInData(dataColumns);

            Assert.assertTrue(tabData instanceof MixedTabularDataset);

            if (tabData instanceof MixedTabularDataset) {
                MixedTabularDataset tabularDataset = (MixedTabularDataset) tabData;
                MixedDataColumn[] columns = tabularDataset.getMixedDataColumns();

                double[][] continuousData = tabularDataset.getContinuousData();
                int[][] discreteData = tabularDataset.getDiscreteData();

                int numOfContinuousCols = 0;
                int numOfDiscreteCols = 0;
                int numOfRows = tabularDataset.getNumOfRows();
                int numOfCols = continuousData.length;
                for (int c = 0; c < numOfCols; c++) {
                    if (continuousData[c] != null) {
                        numOfContinuousCols++;
                    }
                    if (discreteData[c] != null) {
                        numOfDiscreteCols++;
                    }
                }

                expected = 5;
                actual = numOfContinuousCols;
                Assert.assertEquals(expected, actual);

                expected = 5;
                actual = numOfDiscreteCols;
                Assert.assertEquals(expected, actual);
            }
        }
    }

}
