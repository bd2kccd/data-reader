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

import edu.pitt.dbmi.data.reader.DataColumn;
import edu.pitt.dbmi.data.reader.Delimiter;
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
        String dataFileName = getClass().getResource("/data/test_data.txt").getFile();
        Path dataFile = Paths.get(dataFileName);

        final Delimiter delimiter = Delimiter.TAB;
        final char quoteCharacter = '"';
        final String missingValueMarker = "*";
        final String commentMarker = "//";
        final boolean hasHeader = true;

        SimpleTabularDataReader dataReader = new SimpleTabularDataReader(dataFile, delimiter);
        dataReader.setCommentMarker(commentMarker);
        dataReader.setMissingValueMarker(missingValueMarker);
        dataReader.setQuoteCharacter(quoteCharacter);
        dataReader.setHasHeader(hasHeader);

        System.out.println("================================================================================");
        try {
            int[] excludedColumns = {0, 5, -2, 10, 8, -5, 9, 100, 1};
            DataColumn[] dataColumns = dataReader.extractDataColumns(excludedColumns);
            for (DataColumn dataColumn : dataColumns) {
                System.out.println(dataColumn);
            }
        } catch (IOException exception) {
            exception.printStackTrace(System.err);
        }
        System.out.println("================================================================================");
    }

}
