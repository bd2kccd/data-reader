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
package edu.pitt.dbmi.data.reader;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

/**
 *
 * Dec 7, 2018 3:51:45 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class AbstractFileReaderTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    private final Path[] dataFiles = {
        Paths.get(getClass().getResource("/data/tabular/continuous/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/quotes_sim_test_data.csv").getFile())
    };

    public AbstractFileReaderTest() {
    }

    /**
     * Test of countNumberOfColumns method, of class AbstractFileReader.
     */
    @Test
    public void testCountNumberOfColumns() throws Exception {
        for (Path dataFile : dataFiles) {
//            AbstractFileReader fileReader = new AbstractFileReader(dataFile, delimiter);
//            fileReader.setCommentMarker(commentMarker);
//            fileReader.setQuoteCharacter(quoteCharacter);
//
//            System.out.println("================================================================================");
//            System.out.printf("Lines: %d%n", fileReader.countNumberOfLines());
//            System.out.printf("Columns: %d%n", fileReader.countNumberOfColumns());
//            System.out.println("================================================================================");
        }
    }

}
