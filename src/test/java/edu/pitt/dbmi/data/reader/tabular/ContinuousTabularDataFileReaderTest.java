/*
 * Copyright (C) 2017 University of Pittsburgh.
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

import edu.pitt.dbmi.data.ContinuousTabularDataset;
import edu.pitt.dbmi.data.Dataset;
import edu.pitt.dbmi.data.Delimiter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Mar 6, 2017 11:08:38 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ContinuousTabularDataFileReaderTest {

    public ContinuousTabularDataFileReaderTest() {
    }

    @Test
    public void testReadInData() throws IOException {
        Path dataFile = Paths.get("test", "data", "sim_data", "continuous", "small_data.csv");
        Delimiter delimiter = Delimiter.COMMA;
        char quoteCharacter = '"';
        String missingValueMarker = "*";
        String commentMarker = "//";

        TabularDataReader reader = new ContinuousTabularDataFileReader(dataFile.toFile(), delimiter);
        reader.setQuoteCharacter(quoteCharacter);
        reader.setMissingValueMarker(missingValueMarker);
        reader.setCommentMarker(commentMarker);

        Dataset dataset = reader.readInData();
        validateDataset(dataset, 10, 18);
    }

    @Test
    public void testReadInDataWithVariableExclusions() throws IOException {
        Path dataFile = Paths.get("test", "data", "sim_data", "continuous", "small_data.prn");
        Delimiter delimiter = Delimiter.WHITESPACE;
        char quoteCharacter = '"';
        String missingValueMarker = "*";
        String commentMarker = "//";

        String[] variables = {
            "X1",
            "X3",
            "X7",
            "X10"
        };
        Set<String> excludedVariables = new HashSet<>(Arrays.asList(variables));

        TabularDataReader reader = new ContinuousTabularDataFileReader(dataFile.toFile(), delimiter);
        reader.setQuoteCharacter(quoteCharacter);
        reader.setMissingValueMarker(missingValueMarker);
        reader.setCommentMarker(commentMarker);

        Dataset dataset = reader.readInData(excludedVariables);
        validateDataset(dataset, 6, 18);
    }

    private void validateDataset(Dataset dataset, long numOfVars, long numOfCases) {
        boolean isContinuousTabularDataset = (dataset instanceof ContinuousTabularDataset);
        Assert.assertTrue(isContinuousTabularDataset);

        if (isContinuousTabularDataset) {
            ContinuousTabularDataset contDataset = (ContinuousTabularDataset) dataset;
            List<String> variableNames = contDataset.getVariables();
            long expected = numOfVars;
            long actual = variableNames.size();
            Assert.assertEquals(expected, actual);

            double[][] data = contDataset.getData();
            expected = numOfCases;
            actual = data.length;
            Assert.assertEquals(expected, actual);

            expected = numOfVars;
            actual = data[0].length;
            Assert.assertEquals(expected, actual);
        }
    }

}
