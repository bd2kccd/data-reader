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
package edu.pitt.dbmi.data.util;

import java.io.File;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Mar 8, 2017 5:21:55 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TextFileUtilsTest {

    public TextFileUtilsTest() {
    }

    /**
     * Test of inferDelimiter method, of class TextFileUtils.
     */
    @Test
    public void testInferDelimiter() throws Exception {
        File file = Paths.get("test", "data", "cmu", "spartina.txt").toFile();
        int n = Integer.MAX_VALUE;
        int skip = 0;
        String comment = "//";
        char quoteCharacter = '"';
        char[] delims = {'\t', ' ', ',', ':', ';', '|'};

        char expected = ' ';
        char actual = TextFileUtils.inferDelimiter(file, n, skip, comment, quoteCharacter, delims);
        Assert.assertEquals(expected, actual);

        Assert.assertTrue("SPACE".equals(getDelimValue(actual)));
    }

    private String getDelimValue(char c) {
        switch (c) {
            case '\t':
                return "TAB";
            case ' ':
                return "SPACE";
            case ',':
                return "COMMA";
            case ':':
                return "COLON";
            case ';':
                return "SEMICOLON";
            case '|':
                return "PIPE";
            default:
                return "Unknown";
        }
    }

}
