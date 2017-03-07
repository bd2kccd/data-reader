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
package edu.pitt.dbmi.data.reader.covariance;

import edu.pitt.dbmi.data.CovarianceDataset;
import edu.pitt.dbmi.data.Dataset;
import edu.pitt.dbmi.data.Delimiter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Mar 6, 2017 9:19:46 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class LowerCovarianceDataReaderTest {

    public LowerCovarianceDataReaderTest() {
    }

    /**
     * Test of readInData method, of class LowerCovarianceDataReader.
     *
     * @throws IOException
     */
    @Test
    public void testReadInData() throws IOException {
        Path dataFile = Paths.get("test", "data", "cmu", "spartina.txt");
        Delimiter delimiter = Delimiter.SPACE;
        String commentMarker = "//";

        CovarianceDataReader dataReader = new LowerCovarianceDataReader(dataFile.toFile(), delimiter);
        dataReader.setCommentMarker(commentMarker);

        Dataset dataset = dataReader.readInData();
        Assert.assertTrue(dataset instanceof CovarianceDataset);

        CovarianceDataset covarianceDataset = (CovarianceDataset) dataset;

        long expected = 45;
        long actual = covarianceDataset.getNumberOfCases();
        Assert.assertEquals(expected, actual);

        List<String> variableNames = covarianceDataset.getVariables();
        expected = 15;
        actual = variableNames.size();
        Assert.assertEquals(expected, actual);

        double[][] data = covarianceDataset.getData();
        expected = 15;
        actual = data.length;
        Assert.assertEquals(expected, actual);
    }

}
