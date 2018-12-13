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
package edu.pitt.dbmi.data.reader.utils;

import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Dec 9, 2018 12:55:56 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularFileUtilsTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final byte quoteCharacter = '"';
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    private final Path[] dataFiles = {
        Paths.get(getClass().getResource("/data/tabular/mixed/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/quotes_sim_test_data.csv").getFile())
    };

    public TabularFileUtilsTest() {
    }

    /**
     * Test of stripCharacter method, of class TabularFileUtils.
     */
    @Test
    public void testStripCharacter() {
        String word = "\"X3\"";
        byte character = quoteCharacter;

        String expected = "X3";
        String actual = TabularFileUtils.stripCharacter(word, character);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Test of toColumnNumbers method, of class TabularFileUtils.
     *
     * @throws IOException
     */
    @Test
    public void testToColumnNumbers() throws IOException {
        Set<String> columnNames = new HashSet<>(Arrays.asList("X1", "\"X3\"", "X5", "X7", "X9", "X10"));
        if (hasHeader) {
            columnNames = columnNames.stream()
                    .map(e -> TabularFileUtils.stripCharacter(e, quoteCharacter))
                    .collect(Collectors.toSet());
        }

//        for (Path dataFile : dataFiles) {
//            int[] columnNumbers = TabularFileUtils.toColumnNumbers(dataFile, delimiter, commentMarker, quoteCharacter, columnNames);
//
//            long expected = 6;
//            long actual = columnNumbers.length;
//            Assert.assertEquals(expected, actual);
//        }
    }

}
