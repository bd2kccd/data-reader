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
package edu.pitt.dbmi.data.validation;

import edu.pitt.dbmi.data.validation.file.ContinuousTabularDataFileValidation;
import edu.pitt.dbmi.data.validation.file.TabularDataFileValidation;
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
 * Feb 10, 2017 12:48:15 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class DataValidationTest {

    public DataValidationTest() {
    }

    /**
     * Test of validate method, of class DataValidation.
     */
    @Test
    public void testValidate() {
        Path dataFile = Paths.get("test", "data", "continuous", "error_sim_data_5var_10case.csv");
        char delimiter = ',';
        char quoteCharacter = '"';
        String commentMarker = "//";

        String[] variableNames = {
            //            "X1",
            "X2",
            //            "X3",
            "X4",
            //            "X5",
            "X6",
            //            "X7",
            "X8",
            //            "X9",
            "X10"
        };
        Set<String> variables = new HashSet<>(Arrays.asList(variableNames));

        TabularDataFileValidation validation = new ContinuousTabularDataFileValidation(dataFile.toFile(), delimiter);
        validation.setHasHeader(true);
        validation.setQuoteCharacter(quoteCharacter);
        validation.setCommentMarker(commentMarker);

        validation.validate(variables);

        Assert.assertTrue(validation.hasErrors());
        Assert.assertTrue(validation.hasInfos());
        Assert.assertFalse(validation.hasWarnings());

        List<String> results = validation.getErrors();
        long expected = 1;
        long actual = results.size();
        Assert.assertEquals(expected, actual);

        results = validation.getInfos();
        expected = 2;
        actual = results.size();
        Assert.assertEquals(expected, actual);
    }

}
