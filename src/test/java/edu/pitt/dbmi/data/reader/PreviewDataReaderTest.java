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
package edu.pitt.dbmi.data.reader;

import edu.pitt.dbmi.data.reader.tabular.PreviewTabularDataReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Feb 10, 2017 4:49:36 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class PreviewDataReaderTest {

    public PreviewDataReaderTest() {
    }

    /**
     * Test of getPreviews method, of class PreviewDataReader.
     *
     * @throws Exception
     */
    @Test
    public void testGetPreviews() throws Exception {
        Path dataFile = Paths.get("test", "data", "continuous", "uci_air_quality.csv");
        char delimiter = ',';
        char quoteCharacter = '"';
        String commentMarker = "//";

        int fromRow = 1;
        int toRow = 3;
        int fromColumn = 1;
        int toColumn = 4;

        PreviewDataReader dataReader = new PreviewTabularDataReader(dataFile.toFile(), delimiter);
        dataReader.setCommentMarker(commentMarker);
        dataReader.setQuoteCharacter(quoteCharacter);

        List<String> previews = dataReader.getPreviews(fromRow, toRow, fromColumn, toColumn);
        long expected = 3;
        long actual = previews.size();
        Assert.assertEquals(expected, actual);
    }

}
