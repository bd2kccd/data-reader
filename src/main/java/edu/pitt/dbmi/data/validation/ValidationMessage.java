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
package edu.pitt.dbmi.data.validation;

/**
 *
 * Feb 16, 2017 1:56:50 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public interface ValidationMessage {

    public static final String FILE_IO_ERROR = "Unable to read file.";

    public static final String MISSING_VALUE = "Missing value.";

    public static final String INVALID_NUMBER = "Invalid number.";

    public static final String EXCESS_DATA = "Excess data.";

    public static final String INSUFFICIENT_DATA = "Insufficient data.";

}
