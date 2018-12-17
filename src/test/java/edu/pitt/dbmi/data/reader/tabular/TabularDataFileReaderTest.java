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

import edu.pitt.dbmi.data.reader.ContinuousData;
import edu.pitt.dbmi.data.reader.Data;
import edu.pitt.dbmi.data.reader.DataColumn;
import edu.pitt.dbmi.data.reader.Delimiter;
import edu.pitt.dbmi.data.reader.DiscreteDataColumn;
import edu.pitt.dbmi.data.reader.MixedTabularData;
import edu.pitt.dbmi.data.reader.VerticalDiscreteData;
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
 * Nov 15, 2018 5:22:50 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularDataFileReaderTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    private final Path[] continuousDataFiles = {
        Paths.get(getClass().getResource("/data/tabular/continuous/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/continuous/quotes_sim_test_data.csv").getFile())
    };

    private final Path[] discreteDataFiles = {
        Paths.get(getClass().getResource("/data/tabular/discrete/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/discrete/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/discrete/sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/discrete/quotes_sim_test_data.csv").getFile())
    };

    private final Path[] mixedDataFiles = {
        Paths.get(getClass().getResource("/data/tabular/mixed/dos_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/tabular/mixed/quotes_sim_test_data.csv").getFile())
    };

    public TabularDataFileReaderTest() {
    }

    /**
     * Test of readInData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataContinuous() throws IOException {
        for (Path dataFile : continuousDataFiles) {
            TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
            columnReader.setCommentMarker(commentMarker);
            columnReader.setQuoteCharacter(quoteCharacter);

            boolean isDiscrete = false;
            DataColumn[] dataColumns = columnReader.readInDataColumns(isDiscrete);

            long expected = 10;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
            dataReader.setCommentMarker(commentMarker);
            dataReader.setQuoteCharacter(quoteCharacter);
            dataReader.setMissingDataMarker(missingValueMarker);

            Data data = dataReader.readInData(dataColumns, hasHeader);
            Assert.assertTrue(data instanceof ContinuousData);

            if (data instanceof ContinuousData) {
                ContinuousData continuousData = (ContinuousData) data;
                double[][] contData = continuousData.getData();

                expected = 18;
                actual = contData.length;
                Assert.assertEquals(expected, actual);

                expected = 10;
                actual = contData[0].length;
                Assert.assertEquals(expected, actual);
            }
        }
    }

    /**
     * Test of readInData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataContinuousWithExcludeColumns() throws IOException {
        Set<String> excludedColumns = new HashSet<>(Arrays.asList("X1", "X3", "X4", "X6", "X8", "X10"));
        for (Path dataFile : continuousDataFiles) {
            TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
            columnReader.setCommentMarker(commentMarker);
            columnReader.setQuoteCharacter(quoteCharacter);

            boolean isDiscrete = false;
            DataColumn[] dataColumns = columnReader.readInDataColumns(excludedColumns, isDiscrete);

            long expected = 4;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
            dataReader.setCommentMarker(commentMarker);
            dataReader.setQuoteCharacter(quoteCharacter);
            dataReader.setMissingDataMarker(missingValueMarker);

            Data data = dataReader.readInData(dataColumns, hasHeader);
            Assert.assertTrue(data instanceof ContinuousData);

            if (data instanceof ContinuousData) {
                ContinuousData continuousData = (ContinuousData) data;
                double[][] contData = continuousData.getData();

                expected = 18;
                actual = contData.length;
                Assert.assertEquals(expected, actual);

                expected = 4;
                actual = contData[0].length;
                Assert.assertEquals(expected, actual);
            }
        }
    }

    /**
     * Test of readInData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataDiscrete() throws IOException {
        for (Path dataFile : discreteDataFiles) {
            TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
            columnReader.setCommentMarker(commentMarker);
            columnReader.setQuoteCharacter(quoteCharacter);

            boolean isDiscrete = true;
            DataColumn[] dataColumns = columnReader.readInDataColumns(isDiscrete);

            long expected = 10;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
            dataReader.setCommentMarker(commentMarker);
            dataReader.setQuoteCharacter(quoteCharacter);
            dataReader.setMissingDataMarker(missingValueMarker);

            Data data = dataReader.readInData(dataColumns, hasHeader);
            Assert.assertTrue(data instanceof VerticalDiscreteData);

            if (data instanceof VerticalDiscreteData) {
                VerticalDiscreteData verticalDiscreteData = (VerticalDiscreteData) data;

                DiscreteDataColumn[] columns = verticalDiscreteData.getDataColumns();
                expected = 10;
                actual = columns.length;
                Assert.assertEquals(expected, actual);

                int[][] discreteData = verticalDiscreteData.getData();

                expected = 10;
                actual = discreteData.length;
                Assert.assertEquals(expected, actual);

                expected = 19;
                actual = discreteData[0].length;
                Assert.assertEquals(expected, actual);
            }
        }
    }

    /**
     * Test of readInData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataDiscreteWithExcludeColumns() throws IOException {
        Set<String> excludedColumns = new HashSet<>(Arrays.asList("X1", "X3", "X4", "X6", "X8", "X10"));
        for (Path dataFile : discreteDataFiles) {
            TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
            columnReader.setCommentMarker(commentMarker);
            columnReader.setQuoteCharacter(quoteCharacter);

            boolean isDiscrete = true;
            DataColumn[] dataColumns = columnReader.readInDataColumns(excludedColumns, isDiscrete);

            long expected = 4;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
            dataReader.setCommentMarker(commentMarker);
            dataReader.setQuoteCharacter(quoteCharacter);
            dataReader.setMissingDataMarker(missingValueMarker);

            Data data = dataReader.readInData(dataColumns, hasHeader);
            Assert.assertTrue(data instanceof VerticalDiscreteData);

            if (data instanceof VerticalDiscreteData) {
                VerticalDiscreteData verticalDiscreteData = (VerticalDiscreteData) data;

                DiscreteDataColumn[] columns = verticalDiscreteData.getDataColumns();
                expected = 4;
                actual = columns.length;
                Assert.assertEquals(expected, actual);

                int[][] discreteData = verticalDiscreteData.getData();

                expected = 4;
                actual = discreteData.length;
                Assert.assertEquals(expected, actual);

                expected = 19;
                actual = discreteData[0].length;
                Assert.assertEquals(expected, actual);
            }
        }
    }

    /**
     * Test of readInData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataMixed() throws IOException {
        for (Path dataFile : mixedDataFiles) {
            TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
            columnReader.setCommentMarker(commentMarker);
            columnReader.setQuoteCharacter(quoteCharacter);

            boolean isDiscrete = true;
            DataColumn[] dataColumns = columnReader.readInDataColumns(isDiscrete);

            long expected = 10;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
            dataReader.setCommentMarker(commentMarker);
            dataReader.setQuoteCharacter(quoteCharacter);
            dataReader.setMissingDataMarker(missingValueMarker);

            int numberOfCategories = 4;
            dataReader.determineDiscreteDataColumns(dataColumns, numberOfCategories, hasHeader);

            Data data = dataReader.readInData(dataColumns, hasHeader);
            Assert.assertTrue(data instanceof MixedTabularData);

            if (data instanceof MixedTabularData) {
                MixedTabularData mixedTabularData = (MixedTabularData) data;

                int numOfRows = mixedTabularData.getNumOfRows();
                int numOfCols = dataColumns.length;
                DiscreteDataColumn[] discreteDataColumns = mixedTabularData.getDataColumns();
                double[][] continuousData = mixedTabularData.getContinuousData();
                int[][] discreteData = mixedTabularData.getDiscreteData();

                expected = 20;
                actual = numOfRows;
                Assert.assertEquals(expected, actual);

                expected = 10;
                actual = discreteDataColumns.length;
                Assert.assertEquals(expected, actual);

                int numOfContinuous = 0;
                int numOfDiscrete = 0;
                for (int i = 0; i < numOfCols; i++) {
                    if (continuousData[i] != null) {
                        numOfContinuous++;
                    }
                    if (discreteData[i] != null) {
                        numOfDiscrete++;
                    }
                }

                expected = 5;
                actual = numOfContinuous;
                Assert.assertEquals(expected, actual);

                expected = 5;
                actual = numOfDiscrete;
                Assert.assertEquals(expected, actual);
            }
        }
    }

    /**
     * Test of readInData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataMixedWithExcludeColumns() throws IOException {
        Set<String> excludedColumns = new HashSet<>(Arrays.asList("X1", "X3", "X4", "X6", "X8", "X10"));
        for (Path dataFile : mixedDataFiles) {
            TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
            columnReader.setCommentMarker(commentMarker);
            columnReader.setQuoteCharacter(quoteCharacter);

            boolean isDiscrete = true;
            DataColumn[] dataColumns = columnReader.readInDataColumns(excludedColumns, isDiscrete);

            long expected = 4;
            long actual = dataColumns.length;
            Assert.assertEquals(expected, actual);

            TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
            dataReader.setCommentMarker(commentMarker);
            dataReader.setQuoteCharacter(quoteCharacter);
            dataReader.setMissingDataMarker(missingValueMarker);

            int numberOfCategories = 4;
            dataReader.determineDiscreteDataColumns(dataColumns, numberOfCategories, hasHeader);

            Data data = dataReader.readInData(dataColumns, hasHeader);
            Assert.assertTrue(data instanceof MixedTabularData);

            if (data instanceof MixedTabularData) {
                MixedTabularData mixedTabularData = (MixedTabularData) data;

                int numOfRows = mixedTabularData.getNumOfRows();
                int numOfCols = dataColumns.length;
                DiscreteDataColumn[] discreteDataColumns = mixedTabularData.getDataColumns();
                double[][] continuousData = mixedTabularData.getContinuousData();
                int[][] discreteData = mixedTabularData.getDiscreteData();

                expected = 20;
                actual = numOfRows;
                Assert.assertEquals(expected, actual);

                expected = 4;
                actual = discreteDataColumns.length;
                Assert.assertEquals(expected, actual);

                int numOfContinuous = 0;
                int numOfDiscrete = 0;
                for (int i = 0; i < numOfCols; i++) {
                    if (continuousData[i] != null) {
                        numOfContinuous++;
                    }
                    if (discreteData[i] != null) {
                        numOfDiscrete++;
                    }
                }

                expected = 1;
                actual = numOfContinuous;
                Assert.assertEquals(expected, actual);

                expected = 3;
                actual = numOfDiscrete;
                Assert.assertEquals(expected, actual);
            }
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
        for (Path dataFile : mixedDataFiles) {
            TabularColumnReader fileReader = new TabularColumnFileReader(dataFile, delimiter);
            fileReader.setCommentMarker(commentMarker);
            fileReader.setQuoteCharacter(quoteCharacter);

            boolean isDiscrete = false;
            DataColumn[] dataColumns = fileReader.readInDataColumns(isDiscrete);

            TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
            dataReader.setCommentMarker(commentMarker);
            dataReader.setQuoteCharacter(quoteCharacter);
            dataReader.setMissingDataMarker(missingValueMarker);

            dataReader.determineDiscreteDataColumns(dataColumns, 4, hasHeader);
            long numOfDiscrete = Arrays.stream(dataColumns)
                    .filter(DataColumn::isDiscrete)
                    .count();

            long expected = 5;
            long actual = numOfDiscrete;
            Assert.assertEquals(expected, actual);
        }
    }

    /**
     * Test of readInData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInData() throws IOException {
    }

    /**
     * Test of readInDiscreteData method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDiscreteData() throws IOException {
    }

    /**
     * Test of readInDiscreteCategorizes method, of class TabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDiscreteCategorizes() throws IOException {
    }

}
