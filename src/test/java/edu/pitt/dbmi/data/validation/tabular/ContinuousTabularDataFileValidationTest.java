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
package edu.pitt.dbmi.data.validation.tabular;

import edu.pitt.dbmi.data.Delimiter;
import static edu.pitt.dbmi.data.validation.ValidationCode.INFO;
import static edu.pitt.dbmi.data.validation.ValidationCode.WARNING;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Feb 16, 2017 2:47:07 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ContinuousTabularDataFileValidationTest {

    public ContinuousTabularDataFileValidationTest() {
    }

    /**
     * Test of validate method, of class ContinuousTabularDataFileValidation.
     */
    @Test
    public void testValidate() {
        Path dataFile = Paths.get("test", "data", "sim_data", "continuous", "small_data_errors.txt");
        Delimiter delimiter = Delimiter.TAB;
        char quoteCharacter = '"';
        String missingValueMarker = "*";
        String commentMarker = "//";

        TabularDataValidation validation = new ContinuousTabularDataFileValidation(dataFile.toFile(), delimiter);
        validation.setQuoteCharacter(quoteCharacter);
        validation.setMissingValueMarker(missingValueMarker);
        validation.setCommentMarker(commentMarker);

        validation.validate();

        List<ValidationResult> results = validation.getValidationResults();

        List<ValidationResult> infos = new LinkedList<>();
        List<ValidationResult> warnings = new LinkedList<>();
        List<ValidationResult> errors = new LinkedList<>();
        for (ValidationResult result : results) {
            switch (result.getCode()) {
                case INFO:
                    infos.add(result);
                    break;
                case WARNING:
                    warnings.add(result);
                    break;
                default:
                    errors.add(result);
            }
        }

        long expected = 1;
        long actual = infos.size();
        Assert.assertEquals(expected, actual);

        expected = 1;
        actual = warnings.size();
        Assert.assertEquals(expected, actual);

        expected = 1;
        actual = errors.size();
        Assert.assertEquals(expected, actual);
    }

}
