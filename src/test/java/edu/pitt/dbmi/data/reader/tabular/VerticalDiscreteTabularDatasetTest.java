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
import edu.pitt.dbmi.data.reader.tabular.TabularColumnFileReader.DataColumn;
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.DiscreteDataColumn;
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.VerticalDiscreteTabularDataset;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Nov 12, 2018 3:48:05 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class VerticalDiscreteTabularDatasetTest {

    public VerticalDiscreteTabularDatasetTest() {
    }

    @Test
    public void testReadInAllVariables() throws IOException {
        String[] files = {
            "/data/discrete/dos_sim_test_data.csv",
            "/data/discrete/mac_sim_test_data.csv",
            "/data/discrete/sim_test_data.csv"
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

            DataColumn[] dataColumns = columnFileReader.readInDataColumns(false);

            long expected = 10;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            TabularDataFileReader dataFileReader = new TabularDataFileReader(dataFile, delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabData = dataFileReader.readInData(dataColumns);

            Assert.assertTrue(tabData instanceof VerticalDiscreteTabularDataset);

            if (tabData instanceof VerticalDiscreteTabularDataset) {
                VerticalDiscreteTabularDataset dataset = (VerticalDiscreteTabularDataset) tabData;
                int[][] data = dataset.getData();

                expected = 10;
                actual = data.length;
                Assert.assertEquals(expected, actual);

                expected = 19;
                actual = data[0].length;
                Assert.assertEquals(expected, actual);
            }
        }
    }

    private void printData(int[][] data) {
        for (int[] rowData : data) {
            int lastIndex = rowData.length - 1;
            for (int i = 0; i < lastIndex; i++) {
                System.out.printf("%d ", rowData[i]);
            }
            System.out.println(rowData[lastIndex]);
        }
    }

    private void printDataColumns(DiscreteDataColumn[] varInfos) {
        for (DiscreteDataColumn varInfo : varInfos) {
            System.out.println(varInfo);
        }
    }

}
