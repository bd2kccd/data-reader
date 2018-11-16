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
import edu.pitt.dbmi.data.reader.tabular.TabularColumnFileReader.TabularDataColumn;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
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

    private final Path[] dataFiles = {
        Paths.get(getClass().getResource("/data/mixed/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/mixed/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/mixed/sim_test_data.csv").getFile())
    };

    public TabularColumnFileReaderTest() {
    }

    /**
     * Test of readInDataColumns method, of class TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataColumns() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            boolean isDiscrete = false;
            TabularDataColumn[] columns = columnFileReader.readInDataColumns(isDiscrete);
            long expected = 10;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of readInDataColumns method, of class TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataColumns1() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            Set<String> excludedVariables = new HashSet<>(Arrays.asList("X2", "X10", "X3"));
            boolean isDiscrete = false;
            TabularDataColumn[] columns = columnFileReader.readInDataColumns(excludedVariables, isDiscrete);
            long expected = 7;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of readInDataColumns method, of class TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataColumns2() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            int[] excludedColumns = {2, 1, 12, 10, 8};
            boolean isDiscrete = false;
            TabularDataColumn[] columns = columnFileReader.readInDataColumns(excludedColumns, isDiscrete);
            long expected = 6;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of determineDiscreteDataColumns method, of class
     * TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testDetermineDiscreteDataColumns() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            boolean isDiscrete = false;
            TabularDataColumn[] columns = columnFileReader.readInDataColumns(isDiscrete);
            long expected = 10;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            int numOfCategories = 4;
            columnFileReader.determineDiscreteDataColumns(columns, numOfCategories);
            expected = 5;
            actual = Arrays.stream(columns).filter(TabularDataColumn::isDiscrete).count();
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of getColumns method, of class TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testGetColumns() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            int[] excludedColumns = new int[0];
            boolean isDiscrete = false;
            TabularDataColumn[] columns = columnFileReader.getColumns(excludedColumns, isDiscrete);
            long expected = 10;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            excludedColumns = new int[]{1, 3, 5, 7};
            columns = columnFileReader.getColumns(excludedColumns, isDiscrete);
            expected = 6;
            actual = columns.length;
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of toColumnNumbers method, of class TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testToColumnNumbers() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            int[] columns = columnFileReader.toColumnNumbers(Collections.EMPTY_SET);
            long expected = 0;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            columns = columnFileReader.toColumnNumbers(new HashSet<>(Arrays.asList("X1", "X10", "X5", "X11")));
            expected = 3;
            actual = columns.length;
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of extractValidColumnNumbers method, of class
     * TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testExtractValidColumnNumbers() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            int numOfCols = 10;
            int[] cols = new int[0];
            int[] columns = columnFileReader.extractValidColumnNumbers(numOfCols, cols);
            long expected = 0;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            cols = new int[]{1, 1, 3, 5, 5, 7};
            columns = columnFileReader.extractValidColumnNumbers(numOfCols, cols);
            expected = 4;
            actual = columns.length;
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of generateColumns method, of class TabularColumnFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testGenerateColumns() throws IOException {
        for (Path dataFile : dataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);

            int numOfCols = 10;
            int[] excludedColumns = new int[0];
            boolean isDiscrete = false;
            TabularDataColumn[] columns = columnFileReader.generateColumns(numOfCols, excludedColumns, isDiscrete);
            long expected = 10;
            long actual = columns.length;
            Assert.assertEquals(expected, actual);

            excludedColumns = new int[]{1, 3, 5, 7};
            columns = columnFileReader.generateColumns(numOfCols, excludedColumns, isDiscrete);
            expected = 6;
            actual = columns.length;
            Assert.assertEquals(expected, actual);
        }
    }

}
