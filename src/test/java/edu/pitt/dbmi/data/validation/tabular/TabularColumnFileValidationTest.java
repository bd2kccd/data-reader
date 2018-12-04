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
package edu.pitt.dbmi.data.validation.tabular;

import edu.pitt.dbmi.data.reader.Delimiter;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Dec 3, 2018 5:28:48 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class TabularColumnFileValidationTest {

    private final Delimiter delimiter = Delimiter.COMMA;
    private final char quoteCharacter = '"';
    private final String commentMarker = "//";
    private final boolean hasHeader = true;

    private final Path dataFile = Paths.get(getClass().getResource("/data/tabular/continuous/validation/sim_test_data.csv").getFile());

    public TabularColumnFileValidationTest() {
    }

    /**
     * Test of validateFile method, of class TabularColumnFileValidation.
     */
    @Test
    public void testValidateFile() {
        TabularColumnFileValidation fileValidation = new TabularColumnFileValidation(dataFile.toFile(), delimiter);
        fileValidation.setCommentMarker(commentMarker);
        fileValidation.setQuoteCharacter(quoteCharacter);

        fileValidation.validate();

        List<ValidationResult> results = fileValidation.getValidationResults();
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

        expected = 0;
        actual = warnings.size();
        Assert.assertEquals(expected, actual);

        expected = 1;
        actual = errors.size();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testValidateFileWithExcludeVariables() {
        TabularColumnFileValidation fileValidation = new TabularColumnFileValidation(dataFile.toFile(), delimiter);
        fileValidation.setCommentMarker(commentMarker);
        fileValidation.setQuoteCharacter(quoteCharacter);

        fileValidation.validate(new int[]{7});

        List<ValidationResult> results = fileValidation.getValidationResults();
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

        expected = 0;
        actual = warnings.size();
        Assert.assertEquals(expected, actual);

        expected = 0;
        actual = errors.size();
        Assert.assertEquals(expected, actual);
    }

}
