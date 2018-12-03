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

import edu.pitt.dbmi.data.reader.AbstractColumnReader;
import edu.pitt.dbmi.data.reader.Delimiter;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * Dec 3, 2018 2:58:30 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public abstract class AbstractValidation extends AbstractColumnReader implements Validation {

    protected final List<ValidationResult> infos = new LinkedList<>();
    protected final List<ValidationResult> warnings = new LinkedList<>();
    protected final List<ValidationResult> errors = new LinkedList<>();

    protected int maximumNumberOfMessages;

    public AbstractValidation(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
        this.maximumNumberOfMessages = Integer.MAX_VALUE;
    }

    @Override
    public List<ValidationResult> getValidationResults() {
        return Stream.concat(
                Stream.concat(infos.stream(), warnings.stream()),
                errors.stream())
                .collect(Collectors.toList());
    }

    @Override
    public void setMaximumNumberOfMessages(int maximumNumberOfMessages) {
        this.maximumNumberOfMessages = maximumNumberOfMessages;
    }

}
