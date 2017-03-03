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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

/**
 *
 * Feb 22, 2017 2:55:01 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class LowerCovarianceDataReaderTest {

    public LowerCovarianceDataReaderTest() {
    }

    @Test
    public void testReadInData() throws IOException {
        Path dataFile = Paths.get("test", "data", "covariance", "lead_iq.txt");
//        char delimiter = '\t';
//
//        CovarianceDataReader dataReader = new LowerCovarianceDataReader(dataFile.toFile(), delimiter);
//
//        Dataset dataset = dataReader.readInData();
//        Assert.assertTrue(dataset instanceof CovarianceDataset);
//
//        CovarianceDataset covarianceDataset = (CovarianceDataset) dataset;
//
//        long expected = 221;
//        long actual = covarianceDataset.getNumberOfCases();
//        Assert.assertEquals(expected, actual);
//
//        List<String> variableNames = covarianceDataset.getVariables();
//        expected = 7;
//        actual = variableNames.size();
//        Assert.assertEquals(expected, actual);
//
//        double[][] data = covarianceDataset.getData();
//        expected = 7;
//        actual = data.length;
//        Assert.assertEquals(expected, actual);
    }

}
