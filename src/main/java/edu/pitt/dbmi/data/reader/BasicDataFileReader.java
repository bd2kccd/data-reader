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
package edu.pitt.dbmi.data.reader;

import edu.pitt.dbmi.data.Delimiter;
import java.io.File;

/**
 * This data read is used to get basic information such number of rows and
 * columns.
 *
 * Jun 11, 2017 10:48:40 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class BasicDataFileReader extends AbstractDataFileReader {

    public BasicDataFileReader(File dataFile, Delimiter delimiter) {
        super(dataFile, delimiter);
    }

}
