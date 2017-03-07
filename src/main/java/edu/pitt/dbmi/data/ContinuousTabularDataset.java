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
package edu.pitt.dbmi.data;

import java.util.Collections;
import java.util.List;

/**
 *
 * Feb 13, 2017 6:00:13 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class ContinuousTabularDataset implements Dataset {

    private final List<String> variables;

    private final double[][] data;

    public ContinuousTabularDataset(List<String> variables, double[][] data) {
        this.variables = (variables == null) ? Collections.EMPTY_LIST : variables;
        this.data = data;
    }

    public List<String> getVariables() {
        return variables;
    }

    public double[][] getData() {
        return data;
    }

}
