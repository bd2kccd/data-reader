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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

/**
 *
 * Nov 5, 2018 4:44:39 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class SimpleTabularDataReaderTest {

    public SimpleTabularDataReaderTest() {
    }

    @Test
    public void testSomeMethod() {
        String[] files = {
            "/data/dos_test_data.csv",
            "/data/mac_test_data.csv",
            "/data/test_data.csv"
        };
        String dataFileName = getClass().getResource(files[2]).getFile();
        Path dataFile = Paths.get(dataFileName);

        final Delimiter delimiter = Delimiter.COMMA;
        final char quoteCharacter = '"';
        final String missingValueMarker = "*";
        final String commentMarker = "//";
        final boolean hasHeader = true;

        System.out.println("================================================================================");
        try {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            DataColumn[] dataColumns = columnFileReader.readInDataColumns(new int[]{1, 5});
            for (DataColumn dataColumn : dataColumns) {
                System.out.println(dataColumn);
            }

            System.out.println("--------------------------------------------------------------------------------");

            TabularDataFileReader dataFileReader = new TabularDataFileReader(dataFile, delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabData = dataFileReader.readInData(dataColumns);
            if (tabData instanceof ContinuousTabularDataset) {
                ContinuousTabularDataset contData = (ContinuousTabularDataset) tabData;

                for (double[] rowData : contData.getData()) {
                    int lastIndex = rowData.length - 1;
                    for (int i = 0; i < lastIndex; i++) {
                        System.out.printf("%f\t", rowData[i]);
                    }
                    System.out.println(rowData[lastIndex]);
                }
            }
        } catch (IOException exception) {
            exception.printStackTrace(System.err);
        }
        System.out.println("================================================================================");
    }

}
