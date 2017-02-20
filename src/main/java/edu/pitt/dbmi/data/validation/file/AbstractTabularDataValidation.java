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
package edu.pitt.dbmi.data.validation.file;

import edu.pitt.dbmi.data.reader.tabular.AbstractTabularDataReader;
import edu.pitt.dbmi.data.validation.MessageType;
import edu.pitt.dbmi.data.validation.ValidationAttribute;
import edu.pitt.dbmi.data.validation.ValidationCode;
import edu.pitt.dbmi.data.validation.ValidationResult;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 *
 * Feb 17, 2017 5:21:28 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractTabularDataValidation extends AbstractTabularDataReader implements TabularDataValidation {

    protected final List<ValidationResult> validationResults;

    public AbstractTabularDataValidation(File dataFile, char delimiter) {
        super(dataFile, delimiter);
        this.validationResults = new LinkedList<>();
    }

    protected abstract void validateDataFromFile(int[] excludedColumns) throws IOException;

    @Override
    public void validate(Set<String> excludedVariables) {
        try {
            validateDataFromFile(getVariableColumnNumbers(excludedVariables));
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
            validateDataFromFile(getValidColumnNumbers(excludedColumns));
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

    @Override
    public List<ValidationResult> getValidationResults() {
        return validationResults;
    }

}
