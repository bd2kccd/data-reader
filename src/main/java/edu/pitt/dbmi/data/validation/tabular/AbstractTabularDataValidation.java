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
import edu.pitt.dbmi.data.validation.MessageType;
import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 *
 * May 23, 2017 4:13:20 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataValidation extends AbstractTabularDataFileValidation implements TabularDataValidation {

    public AbstractTabularDataValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    protected abstract void validateDataFromFile(int[] excludedColumns) throws IOException;

    @Override
    public void validate(Set<String> excludedVariables) {
        try {
            validateDataFromFile(getColumnNumbers(excludedVariables));
        } catch (IOException exception) {
            String errMsg = String.format("Unable to read file %s.", dataFile.getName());
            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
            result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
            validationResults.add(result);
        }
    }

    @Override
    public void validate(int[] excludedColumns) {
        try {
            validateDataFromFile(filterValidColumnNumbers(excludedColumns));
        } catch (IOException exception) {
            String errMsg = String.format("Unable to read file %s.", dataFile.getName());
            ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
            result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
            validationResults.add(result);
        }
    }

    @Override
    public void validate() {
        validate(Collections.EMPTY_SET);
    }

}
