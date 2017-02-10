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

import edu.pitt.dbmi.data.validation.DataValidation;
import java.util.Set;

/**
 *
 * Feb 6, 2017 6:26:15 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public interface TabularDataFileValidation extends DataValidation {

    public void validate(Set<String> excludedVariables);

    public void validate(int[] excludedColumns);

    public void setQuoteCharacter(char quoteCharacter);

    public void setCommentMarker(String commentMarker);

    public void setHasHeader(boolean hasHeader);

}
