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
package edu.pitt.dbmi.data.validation;

import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * Dec 3, 2018 3:55:32 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractColumnValidation extends AbstractValidation implements ColumnValidation {

    public AbstractColumnValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

    protected abstract void validateFile(int[] excludedColumns) throws IOException;

    @Override
    public void validate(int[] excludedColumns) {
        int size = (excludedColumns == null) ? 0 : excludedColumns.length;
        int[] excludedCols = new int[size];
        if (size > 0) {
            System.arraycopy(excludedColumns, 0, excludedCols, 0, size);
            Arrays.sort(excludedCols);
        }

        try {
            int numOfCols = countNumberOfColumns();
            int[] validCols = extractValidColumnNumbers(numOfCols, excludedCols);

            validateFile(validCols);
        } catch (IOException exception) {
            if (errors.size() <= maximumNumberOfMessages) {
                String errMsg = String.format("Unable to read file %s.", dataFile.getName());
                ValidationResult result = new ValidationResult(ValidationCode.ERROR, MessageType.FILE_IO_ERROR, errMsg);
                result.setAttribute(ValidationAttribute.FILE_NAME, dataFile.getName());
                errors.add(result);
            }
        }
    }

    @Override
    public void validate() {
        validate(new int[0]);
    }

}
