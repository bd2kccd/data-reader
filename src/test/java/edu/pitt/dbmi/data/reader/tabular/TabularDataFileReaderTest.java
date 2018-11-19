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
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.ContinuousTabularDataset;
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.MixedDataColumn;
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.MixedTabularDataset;
import edu.pitt.dbmi.data.reader.tabular.TabularDataFileReader.VerticalDiscreteTabularDataset;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.junit.Ignore;
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

    private final String[] excludeVariables = {
        "X1", "X3", "X5", "X7", "X9"
    };

    private final Path[] continuousDataFiles = {
        //        Paths.get(getClass().getResource("/data/continuous/dos_sim_test_data.csv").getFile()),
        //        Paths.get(getClass().getResource("/data/continuous/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/continuous/sim_test_data.csv").getFile())
    };

    private final Path[] discreteDataFiles = {
        //        Paths.get(getClass().getResource("/data/discrete/dos_sim_test_data.csv").getFile()),
        //        Paths.get(getClass().getResource("/data/discrete/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/discrete/sim_test_data.csv").getFile())
    };

    private final Path[] mixedDataFiles = {
        //        Paths.get(getClass().getResource("/data/mixed/dos_sim_test_data.csv").getFile()),
        //        Paths.get(getClass().getResource("/data/mixed/mac_sim_test_data.csv").getFile()),
        Paths.get(getClass().getResource("/data/mixed/sim_test_data.csv").getFile())
    };

    public TabularDataFileReaderTest() {
    }

    @Ignore
    @Test
    public void testReadInData() throws IOException {
        for (Path dataFile : mixedDataFiles) {
            TabularColumnFileReader columnFileReader = new TabularColumnFileReader(dataFile, delimiter);
            columnFileReader.setCommentMarker(commentMarker);
            columnFileReader.setMissingValueMarker(missingValueMarker);
            columnFileReader.setQuoteCharacter(quoteCharacter);
            columnFileReader.setHasHeader(hasHeader);

            boolean isDiscrete = false;
            TabularDataColumn[] columns = columnFileReader.readInDataColumns(isDiscrete);
//            columns[1].setDiscrete(false);
//            Arrays.stream(columns).forEach(System.out::println);
            int numOfCategories = 4;
            columnFileReader.determineDiscreteDataColumns(columns, numOfCategories);
            columns[1].setDiscrete(false);

            TabularDataFileReader dataFileReader = new TabularDataFileReader(dataFile, delimiter);
            dataFileReader.setCommentMarker(commentMarker);
            dataFileReader.setMissingValueMarker(missingValueMarker);
            dataFileReader.setQuoteCharacter(quoteCharacter);
            dataFileReader.setHasHeader(hasHeader);

            TabularData tabData = dataFileReader.readInData(columns);
            if (tabData instanceof ContinuousTabularDataset) {
                ContinuousTabularDataset tabularDataset = (ContinuousTabularDataset) tabData;

                System.out.println("================================================================================");
                Arrays.stream(tabularDataset.getColumns()).forEach(System.out::println);
                System.out.println("--------------------------------------------------------------------------------");
                double[][] data = tabularDataset.getData();
                for (double[] rowData : data) {
                    int lastIndex = rowData.length - 1;
                    for (int i = 0; i < lastIndex; i++) {
                        System.out.printf("%f\t", rowData[i]);
                    }
                    System.out.printf("%f%n", rowData[lastIndex]);
                }
                System.out.println("================================================================================");
            } else if (tabData instanceof VerticalDiscreteTabularDataset) {
                VerticalDiscreteTabularDataset tabularDataset = (VerticalDiscreteTabularDataset) tabData;
                System.out.println("================================================================================");
                Arrays.stream(tabularDataset.getColumns()).forEach(System.out::println);
                System.out.println("--------------------------------------------------------------------------------");
                int[][] data = tabularDataset.getData();
                int numOfCols = data.length;
                int numOfRows = data[0].length;
                for (int row = 0; row < numOfRows; row++) {
                    for (int col = 0; col < numOfCols; col++) {
                        System.out.printf("%d\t", data[col][row]);
                    }
                    System.out.println();
                }
                System.out.println("================================================================================");
            } else if (tabData instanceof MixedTabularDataset) {
                MixedTabularDataset tabularDataset = (MixedTabularDataset) tabData;

                MixedDataColumn[] mixedCols = tabularDataset.getColumns();
                System.out.println("================================================================================");
                Arrays.stream(tabularDataset.getColumns()).forEach(System.out::println);
                System.out.println("--------------------------------------------------------------------------------");
                double[][] continuousData = tabularDataset.getContinuousData();
                int[][] discreteData = tabularDataset.getDiscreteData();
                int numOfRows = tabularDataset.getNumOfRows();
                int numOfCols = mixedCols.length;
                for (int row = 0; row < numOfRows; row++) {
                    for (int col = 0; col < numOfCols; col++) {
                        if (continuousData[col] != null) {
                            System.out.printf("%f\t", continuousData[col][row]);
                        }
                        if (discreteData[col] != null) {
                            System.out.printf("%d\t", discreteData[col][row]);
                        }
                    }
                    System.out.println();
                }
                System.out.println("================================================================================");
            }
        }
    }

}
