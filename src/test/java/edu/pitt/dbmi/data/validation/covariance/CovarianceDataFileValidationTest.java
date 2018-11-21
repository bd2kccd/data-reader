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
package edu.pitt.dbmi.data.validation.covariance;

import edu.pitt.dbmi.data.reader.Delimiter;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;

/**
 *
 * Nov 20, 2018 2:04:40 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class CovarianceDataFileValidationTest {

    private final Delimiter delimiter = Delimiter.SPACE;
    private final char quoteCharacter = '"';
    private final String missingValueMarker = "*";
    private final String commentMarker = "//";

    private final Path dataFile = Paths.get(getClass().getResource("/data/covariance/spartina.txt").getFile());

    public CovarianceDataFileValidationTest() {
    }

    /**
     * Test of validate method, of class CovarianceDataFileValidation.
     */
    @Test
    public void testValidate() {
        CovarianceDataFileValidation fileValidation = new CovarianceDataFileValidation(dataFile.toFile(), delimiter);
        fileValidation.setCommentMarker(commentMarker);
        fileValidation.setMissingValueMarker(missingValueMarker);
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
    }

}
