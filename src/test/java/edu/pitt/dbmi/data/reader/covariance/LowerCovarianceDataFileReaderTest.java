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
package edu.pitt.dbmi.data.reader.covariance;

import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Nov 19, 2018 11:16:29 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class LowerCovarianceDataFileReaderTest {

    private final Delimiter delimiter = Delimiter.SPACE;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";

    private final Path dataFile = Paths.get(getClass().getResource("/data/covariance/spartina.txt").getFile());

    public LowerCovarianceDataFileReaderTest() {
    }

    /**
     * Test of getNumberOfCases method, of class LowerCovarianceDataFileReader.
     *
     * @throws IOException
     */
    @Test
    public void testGetNumberOfCases() throws IOException {
        CovarianceDataReader dataFileReader = new LowerCovarianceDataFileReader(dataFile.toFile(), delimiter);
        dataFileReader.setCommentMarker(commentMarker);
        dataFileReader.setMissingValueMarker(missingValueMarker);
        dataFileReader.setQuoteCharacter(quoteCharacter);

        CovarianceData covarianceData = dataFileReader.readInData();

        int numberOfCases = covarianceData.getNumberOfCases();
        long expected = 45;
        long actual = numberOfCases;
        Assert.assertEquals(expected, actual);

        List<String> variables = covarianceData.getVariables();
        expected = 15;
        actual = variables.size();
        Assert.assertEquals(expected, actual);

        double[][] data = covarianceData.getData();
        expected = 15;
        actual = data.length;
        Assert.assertEquals(expected, actual);

        expected = 15;
        actual = data[0].length;
        Assert.assertEquals(expected, actual);
    }

}
