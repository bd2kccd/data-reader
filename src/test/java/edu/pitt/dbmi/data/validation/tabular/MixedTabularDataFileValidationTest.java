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
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;

/**
 *
 * May 24, 2017 12:25:24 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedTabularDataFileValidationTest {

    public MixedTabularDataFileValidationTest() {
    }

    /**
     * Test of validateDataFromFile method, of class
     * MixedTabularDataFileValidation.
     *
     * @throws Exception
     */
    @Test
    public void testValidate() throws Exception {
        Path dataFile = Paths.get("test", "data", "sim_data", "mixed", "small_mixed_data.csv");
        Delimiter delimiter = Delimiter.TAB;
        char quoteCharacter = '"';
        String missingValueMarker = "*";
        String commentMarker = "//";
        int numberOfDiscreteCategories = 2;

        TabularDataValidation validation = new MixedTabularDataFileValidation(numberOfDiscreteCategories, dataFile.toFile(), delimiter);
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

        System.out.println("================================================================================");
        printList(infos, "INFO");
        printList(warnings, "WARNING");
        printList(errors, "ERROR");
        System.out.println("================================================================================");
    }

    public void printList(List<ValidationResult> results, String title) {
        System.out.println(title);
        System.out.println("--------------------------------------------------------------------------------");
        results.forEach(result -> {
            System.out.println(result);
        });
        System.out.println();

    }

}
