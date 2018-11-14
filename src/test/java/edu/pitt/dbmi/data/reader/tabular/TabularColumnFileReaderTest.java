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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Nov 14, 2018 11:03:13 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularColumnFileReaderTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    public TabularColumnFileReaderTest() {
    }

    @Test
    public void testMixedVariables() throws Exception {
        String[] files = {
            "/data/mixed/dos_sim_test_data.csv",
            "/data/mixed/mac_sim_test_data.csv",
            "/data/mixed/sim_test_data.csv"
        };

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

            columnFileReader.determineDiscreteDataColumns(dataColumns, 4);
            int numOfContinuous = 0;
            int numOfDiscrete = 0;
            for (DataColumn dataColumn : dataColumns) {
                if (dataColumn.isContinuous()) {
                    numOfContinuous++;
                } else {
                    numOfDiscrete++;
                }
            }
            expected = 5;
            Assert.assertEquals(expected, numOfContinuous);
            Assert.assertEquals(expected, numOfDiscrete);

            final Set<String> excludeVars = new HashSet<>(Arrays.asList("X1", "X3", "X5", "X7", "X9"));
            dataColumns = columnFileReader.readInDataColumns(excludeVars, false);
            expected = 5;
            actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            columnFileReader.determineDiscreteDataColumns(dataColumns, 4);
            numOfContinuous = 0;
            numOfDiscrete = 0;
            for (DataColumn dataColumn : dataColumns) {
                if (dataColumn.isContinuous()) {
                    numOfContinuous++;
                } else {
                    numOfDiscrete++;
                }
            }
            expected = 2;
            Assert.assertEquals(expected, numOfContinuous);
            expected = 3;
            Assert.assertEquals(expected, numOfDiscrete);
        }
    }

    @Test
    public void testDiscreteVariables() throws Exception {
        String[] files = {
            "/data/discrete/dos_sim_test_data.csv",
            "/data/discrete/mac_sim_test_data.csv",
            "/data/discrete/sim_test_data.csv"
        };

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

            final Set<String> excludeVars = new HashSet<>(Arrays.asList("X1", "X3", "X5", "X7", "X9"));
            dataColumns = columnFileReader.readInDataColumns(excludeVars, false);
            expected = 5;
            actual = dataColumns.length;
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testContinuousVariables() throws Exception {
        String[] files = {
            "/data/continuous/dos_sim_test_data.csv",
            "/data/continuous/mac_sim_test_data.csv",
            "/data/continuous/sim_test_data.csv"
        };

        for (String file : files) {
            String dataFileName = getClass().getResource(file).getFile();
            Path dataFile = Paths.get(dataFileName);

            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            DataColumn[] dataColumns = columnFileReader.readInDataColumns(true);
            long expected = 10;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            final Set<String> excludeVars = new HashSet<>(Arrays.asList("X1", "X3", "X5", "X7", "X9"));
            dataColumns = columnFileReader.readInDataColumns(excludeVars, true);
            expected = 5;
            actual = dataColumns.length;
            Assert.assertEquals(expected, actual);
        }
    }

}
