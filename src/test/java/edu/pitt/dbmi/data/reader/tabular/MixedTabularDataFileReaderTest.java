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

import edu.pitt.dbmi.data.Dataset;
import edu.pitt.dbmi.data.Delimiter;
import edu.pitt.dbmi.data.MixedTabularDataset;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Apr 4, 2017 5:25:19 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedTabularDataFileReaderTest {

    public MixedTabularDataFileReaderTest() {
    }

    /**
     * Test of readInDataFromFile method, of class MixedTabularDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInDataFromFile() throws IOException {
        Path dataFile = Paths.get("test", "data", "sim_data", "mixed", "small_mixed_data.csv");
        Delimiter delimiter = Delimiter.COMMA;
        char quoteCharacter = '"';
        String missingValueMarker = "*";
        String commentMarker = "//";
        int numberOfDiscreteCategories = 2;

        TabularDataReader reader = new MixedTabularDataFileReader(numberOfDiscreteCategories, dataFile.toFile(), delimiter);
        reader.setQuoteCharacter(quoteCharacter);
        reader.setMissingValueMarker(missingValueMarker);
        reader.setCommentMarker(commentMarker);

        Dataset dataset = reader.readInData();
        validateDataset(dataset, 10, 20);
    }

    private void validateDataset(Dataset dataset, long numOfVars, long numOfCases) {
        if (dataset instanceof MixedTabularDataset) {
            MixedTabularDataset mixedTabularDataset = (MixedTabularDataset) dataset;

            long expected = numOfCases;
            long actual = mixedTabularDataset.getNumOfRows();
            Assert.assertEquals(expected, actual);

            expected = numOfVars;
            actual = 0;
            double[][] continuousData = mixedTabularDataset.getContinuousData();
            for (double[] data : continuousData) {
                if (data != null) {
                    actual++;
                }
            }
            int[][] discreteData = mixedTabularDataset.getDiscreteData();
            for (int[] data : discreteData) {
                if (data != null) {
                    actual++;
                }
            }
            Assert.assertEquals(expected, actual);
        } else {
            Assert.fail("Dataset is not of type MixedTabularDataset.");
        }
    }

}
