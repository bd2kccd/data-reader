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

import java.util.List;

/**
 *
 * Feb 23, 2017 2:50:19 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class CovarianceDataset implements Dataset {

    private final int numberOfCases;

    private final List<String> variables;

    private final double[][] data;

    public CovarianceDataset(int numberOfCases, List<String> variables, double[][] data) {
        this.numberOfCases = numberOfCases;
        this.variables = variables;
        this.data = data;
    }

    public int getNumberOfCases() {
        return numberOfCases;
    }

    public List<String> getVariables() {
        return variables;
    }

    public double[][] getData() {
        return data;
    }

}
