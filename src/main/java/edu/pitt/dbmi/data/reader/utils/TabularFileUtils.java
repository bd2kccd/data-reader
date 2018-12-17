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
package edu.pitt.dbmi.data.reader.utils;

/**
 *
 * Dec 9, 2018 11:28:38 AM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public final class TabularFileUtils {

    private TabularFileUtils() {
    }

    public static final String stripCharacter(String word, byte character) {
        if (word == null || word.isEmpty()) {
            return "";
        }

        StringBuilder dataBuilder = new StringBuilder();
        for (byte currChar : word.getBytes()) {
            if (currChar != character) {
                dataBuilder.append((char) currChar);
            }
        }

        return dataBuilder.toString();
    }

}
