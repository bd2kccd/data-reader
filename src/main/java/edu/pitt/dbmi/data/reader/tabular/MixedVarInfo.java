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
package edu.pitt.dbmi.data.reader.tabular;

/**
 *
 * Apr 5, 2017 5:04:17 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class MixedVarInfo extends DiscreteVarInfo {

    protected boolean continuous;

    public MixedVarInfo(String name) {
        super(name);
    }

    @Override
    public String toString() {
        return "MixedVarInfo{" + "name=" + name + ", values=" + values + ", categories=" + categories + ", continuous=" + continuous + '}';
    }

    public boolean isContinuous() {
        return continuous;
    }

    public void setContinuous(boolean continuous) {
        this.continuous = continuous;
    }

    public int getNumberOfValues() {
        return values.size();
    }

    public void clearValues() {
        values.clear();
    }

}
