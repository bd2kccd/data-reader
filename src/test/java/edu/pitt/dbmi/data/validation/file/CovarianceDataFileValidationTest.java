/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.pitt.dbmi.data.validation.file;

import edu.pitt.dbmi.data.validation.ValidationResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Feb 23, 2017 3:38:54 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class CovarianceDataFileValidationTest {

    public CovarianceDataFileValidationTest() {
    }

    /**
     * Test of validate method, of class CovarianceDataFileValidation.
     */
    @Test
    public void testValidate() {
        Path dataFile = Paths.get("test", "data", "covariance", "lead_iq.txt");
        char delimiter = '\t';

        DataFileValidation validation = new CovarianceDataFileValidation(dataFile.toFile(), delimiter);

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

        expected = 0;
        actual = warnings.size();
        Assert.assertEquals(expected, actual);

        expected = 0;
        actual = errors.size();
        Assert.assertEquals(expected, actual);
    }

}
