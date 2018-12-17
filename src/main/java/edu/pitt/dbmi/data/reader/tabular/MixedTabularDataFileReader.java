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

import edu.pitt.dbmi.data.reader.AbstractDataFileReader;
import edu.pitt.dbmi.data.reader.ContinuousData;
import edu.pitt.dbmi.data.reader.Data;
import edu.pitt.dbmi.data.reader.DataColumn;
import edu.pitt.dbmi.data.reader.Delimiter;
import edu.pitt.dbmi.data.reader.DiscreteDataColumn;
import edu.pitt.dbmi.data.reader.MixedTabularData;
import edu.pitt.dbmi.data.reader.VerticalDiscreteData;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * Dec 14, 2018 1:54:31 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedTabularDataFileReader extends AbstractDataFileReader implements MixedTabularDataReader {

    private final int numberOfDiscreteCategories;
    private boolean hasHeader;
    private char quoteChar;

    public MixedTabularDataFileReader(int numberOfDiscreteCategories, Path dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
        this.numberOfDiscreteCategories = 4;
        this.hasHeader = hasHeader = true;
        this.quoteChar = '"';
    }

    @Override
    public Data readInData(Set<String> excludedColumns) throws IOException {
        TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
        columnReader.setCommentMarker(commentMarker);
        columnReader.setQuoteCharacter(quoteChar);

        boolean isDiscrete = false;
        DataColumn[] dataColumns = hasHeader
                ? columnReader.readInDataColumns(excludedColumns, isDiscrete)
                : columnReader.generateColumns(new int[0], isDiscrete);

        TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
        dataReader.setCommentMarker(commentMarker);
        dataReader.setQuoteCharacter(quoteChar);
        dataReader.setMissingDataMarker(missingDataMarker);

        dataReader.determineDiscreteDataColumns(dataColumns, numberOfDiscreteCategories, hasHeader);

        return toMixedData(dataReader.readInData(dataColumns, hasHeader));
    }

    @Override
    public Data readInData(int[] excludedColumns) throws IOException {
        TabularColumnReader columnReader = new TabularColumnFileReader(dataFile, delimiter);
        columnReader.setCommentMarker(commentMarker);
        columnReader.setQuoteCharacter(quoteChar);

        boolean isDiscrete = false;
        DataColumn[] dataColumns = hasHeader
                ? columnReader.readInDataColumns(excludedColumns, isDiscrete)
                : columnReader.generateColumns(excludedColumns, isDiscrete);

        TabularDataReader dataReader = new TabularDataFileReader(dataFile, delimiter);
        dataReader.setCommentMarker(commentMarker);
        dataReader.setQuoteCharacter(quoteChar);
        dataReader.setMissingDataMarker(missingDataMarker);

        dataReader.determineDiscreteDataColumns(dataColumns, numberOfDiscreteCategories, hasHeader);

        return toMixedData(dataReader.readInData(dataColumns, hasHeader));
    }

    private Data toMixedData(Data data) {
        if (data instanceof ContinuousData) {
            ContinuousData continuousData = (ContinuousData) data;
            double[][] contData = continuousData.getData();
            int numOfRows = contData.length;
            int numOfCols = contData[0].length;

            // convert to mixed variables
            DiscreteDataColumn[] columns = Arrays.stream(continuousData.getDataColumns())
                    .map(MixedTabularFileDataColumn::new)
                    .toArray(DiscreteDataColumn[]::new);

            // transpose the data
            double[][] vertContData = new double[numOfCols][numOfRows];
            for (int row = 0; row < numOfRows; row++) {
                for (int col = 0; col < numOfCols; col++) {
                    vertContData[col][row] = contData[row][col];
                }
            }

            return new MixedTabularFileData(numOfRows, columns, vertContData, new int[0][0]);
        } else if (data instanceof VerticalDiscreteData) {
            VerticalDiscreteData verticalDiscreteData = (VerticalDiscreteData) data;
            int[][] discreteData = verticalDiscreteData.getData();
            int numOfRows = discreteData[0].length;

            // convert to mixed variables
            DiscreteDataColumn[] columns = Arrays.stream(verticalDiscreteData.getDataColumns())
                    .map(e -> {
                        DiscreteDataColumn column = new MixedTabularFileDataColumn(e.getDataColumn());
                        e.getCategories().forEach(v -> column.setValue(v));
                        e.recategorize();

                        return column;
                    }).toArray(DiscreteDataColumn[]::new);

            return new MixedTabularFileData(numOfRows, columns, new double[0][0], discreteData);
        } else if (data instanceof MixedTabularData) {
            MixedTabularData mixedTabularData = (MixedTabularData) data;
            DiscreteDataColumn[] columns = mixedTabularData.getDataColumns();
            double[][] continuousData = mixedTabularData.getContinuousData();
            int[][] discreteData = mixedTabularData.getDiscreteData();
            int numOfRows = mixedTabularData.getNumOfRows();

            return new MixedTabularFileData(numOfRows, columns, continuousData, discreteData);
        } else {
            return null;
        }
    }

    @Override
    public Data readInData() throws IOException {
        return readInData(Collections.EMPTY_SET);
    }

    @Override
    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    @Override
    public void setQuoteCharacter(char quoteCharacter) {
        this.quoteChar = quoteCharacter;
    }

    private final class MixedTabularFileDataColumn implements DiscreteDataColumn {

        private final DataColumn dataColumn;
        private final Map<String, Integer> values;
        private List<String> categories;

        public MixedTabularFileDataColumn(DataColumn dataColumn) {
            this.dataColumn = dataColumn;
            this.values = dataColumn.isDiscrete() ? new TreeMap<>() : null;
        }

        @Override
        public String toString() {
            return "MixedTabularFileDataColumn{" + "dataColumn=" + dataColumn + ", values=" + values + ", categories=" + categories + '}';
        }

        @Override
        public Integer getEncodeValue(String value) {
            return (values == null)
                    ? DISCRETE_MISSING_VALUE
                    : values.get(value);
        }

        @Override
        public void recategorize() {
            if (values != null) {
                Set<String> keyset = values.keySet();
                categories = new ArrayList<>(keyset.size());
                int count = 0;
                for (String key : keyset) {
                    values.put(key, count++);
                    categories.add(key);
                }
            }
        }

        @Override
        public List<String> getCategories() {
            return (categories == null) ? Collections.EMPTY_LIST : categories;
        }

        @Override
        public DataColumn getDataColumn() {
            return dataColumn;
        }

        @Override
        public void setValue(String value) {
            if (this.values != null) {
                this.values.put(value, null);
            }
        }

    }

    private final class MixedTabularFileData implements MixedTabularData {

        private final int numOfRows;
        private final DiscreteDataColumn[] dataColumns;
        private final double[][] continuousData;
        private final int[][] discreteData;

        public MixedTabularFileData(int numOfRows, DiscreteDataColumn[] dataColumns, double[][] continuousData, int[][] discreteData) {
            this.numOfRows = numOfRows;
            this.dataColumns = dataColumns;
            this.continuousData = continuousData;
            this.discreteData = discreteData;
        }

        @Override
        public int getNumOfRows() {
            return numOfRows;
        }

        @Override
        public DiscreteDataColumn[] getDataColumns() {
            return dataColumns;
        }

        @Override
        public double[][] getContinuousData() {
            return continuousData;
        }

        @Override
        public int[][] getDiscreteData() {
            return discreteData;
        }

    }

}
