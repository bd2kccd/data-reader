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

import edu.pitt.dbmi.data.Dataset;
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
        Path dataFile = Paths.get("/home", "kvb2", "shared", "test", "data", "lead_iq.txt");
        char delimiter = '\t';
        char quoteCharacter = '"';
        String commentMarker = "//";

        CovarianceDataReader dataReader = new LowerCovarianceDataReader(dataFile.toFile(), delimiter);
        dataReader.setQuoteCharacter(quoteCharacter);
//        dataReader.setCommentMarker(commentMarker);
        System.out.println("================================================================================");
        Dataset dataSet = dataReader.readInData();
        System.out.println("================================================================================");
    }

}
