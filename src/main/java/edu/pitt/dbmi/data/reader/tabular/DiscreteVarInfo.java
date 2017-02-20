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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * Feb 15, 2017 5:17:06 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class DiscreteVarInfo {

    private final String name;
    private final Map<String, Integer> values;

    public DiscreteVarInfo(String name) {
        this.name = name;
        this.values = new TreeMap<>();
    }

    public void recategorize() {
        Set<String> keyset = values.keySet();
        int count = 0;
        for (String key : keyset) {
            values.put(key, count++);
        }
    }

    @Override
    public String toString() {
        return "DiscreteVarInfo{" + "name=" + name + ", values=" + values + ", categories=" + values.keySet() + '}';
    }

    public String getName() {
        return name;
    }

    public Integer getEncodeValue(String value) {
        return values.get(value);
    }

    public void setValue(String value) {
        this.values.put(value, null);
    }

    public Set<String> getCategories() {
        return values.keySet();
    }

}
